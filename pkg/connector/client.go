package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	gm "github.com/yourusername/matrix-garmin-messenger/internal/hermes"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

// GarminClient is the per-login network client.
// It wraps the slush-dev library's HermesAPI and HermesSignalR.
type GarminClient struct {
	connector *GarminConnector
	userLogin *bridgev2.UserLogin
	phone     string            // logged-in user's phone number
	auth      *gm.HermesAuth    // shared auth; passed to both api and sr
	api       *gm.HermesAPI     // REST client
	sr        *gm.HermesSignalR // SignalR real-time client
	log       zerolog.Logger

	// pendingConvPortals maps a real Garmin conversation UUID (string) to the
	// portal key of a "phone:+E164" synthetic portal created by ResolveIdentifier
	// when no existing conversation was found. When the first message is sent from
	// Matrix (which creates the Garmin conversation), the resulting ConversationID
	// is stored here so that incoming SignalR events for that conversation are
	// routed to the same portal instead of creating a duplicate.
	pendingConvPortals sync.Map // map[string]networkid.PortalKey

	// initialSyncDone is set after Connect() has performed the initial catch-up
	// sync. It is used so the OnOpen callback only triggers a reconnect catch-up
	// when the SignalR connection is re-established (not on the first connect,
	// where Connect() already handled the initial sync).
	initialSyncDone bool

	// catchingUp prevents two catch-up syncs from running concurrently if
	// OnOpen fires while a previous sync is still in progress.
	catchingUp atomic.Bool

	// syncTimeMu guards LastSyncTime updates and the debounced save state.
	syncTimeMu      sync.Mutex
	pendingSaveTime *time.Time
	saveTimer       *time.Timer

	// recentMu guards the per-conversation recent-messages ring buffer used
	// to resolve incoming reactions to their target without an API call.
	// Populated by handleIncomingMessage on every non-reaction it processes
	// and by syncMissedMessages in bulk after each conversation detail fetch.
	recentMu       sync.Mutex
	recentByConv   map[uuid.UUID][]gm.ConversationMessageModel
}

// recentMessagesPerConv caps how many messages per conversation are kept in
// the in-memory cache for reaction parent resolution.
const recentMessagesPerConv = 200

// maxCatchupWindow caps how far back catch-up will scan if LastSyncTime is
// missing or very stale. Without this, a long downtime could trigger an
// arbitrarily large backfill.
const maxCatchupWindow = 48 * time.Hour

// debounceSaveInterval is how long to wait after the last LastSyncTime update
// before persisting it to the bridgev2 database. This avoids one DB write per
// incoming Garmin message.
const debounceSaveInterval = 5 * time.Minute

var _ bridgev2.NetworkAPI = (*GarminClient)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*GarminClient)(nil)
var _ bridgev2.ReactionHandlingNetworkAPI = (*GarminClient)(nil)

func newGarminClient(gc *GarminConnector, login *bridgev2.UserLogin, auth *gm.HermesAuth, phone string) *GarminClient {
	hermesLog := login.Log.With().Str("component", "hermes").Logger()
	hermesLogger := slog.New(newZerologSlogHandler(hermesLog))
	api := gm.NewHermesAPI(auth, gm.WithAPILogger(hermesLogger))
	sr := gm.NewHermesSignalR(auth,
		gm.WithSignalRLogger(hermesLogger),
	)
	return &GarminClient{
		connector: gc,
		userLogin: login,
		phone:     phone,
		auth:      auth,
		api:       api,
		sr:        sr,
		log:       login.Log.With().Str("component", "garmin-client").Logger(),
	}
}

// ─── bridgev2.NetworkAPI ──────────────────────────────────────────────────────

// Connect validates the session and starts the SignalR listener.
func (c *GarminClient) Connect(ctx context.Context) {
	// Validate session with a lightweight call.
	if _, err := c.api.GetConversations(ctx, gm.WithLimit(1)); err != nil {
		c.sendErrorState(err, "Failed to connect to Garmin Messenger: "+err.Error())
		return
	}

	c.userLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateConnected,
	})

	// Run an initial catch-up sync for any messages that arrived while the
	// bridge was offline. The bridgev2 framework deduplicates by
	// (PortalKey, MessageID), so already-bridged events are silently ignored.
	// Status receipts are skipped on initial sync because the Matrix events
	// they reference may predate the bot's current room access (causing
	// M_FORBIDDEN noise) and because their state is no longer actionable.
	if err := c.syncMissedMessages(ctx, true); err != nil {
		c.log.Warn().Err(err).Msg("Initial catch-up sync failed")
	}
	c.initialSyncDone = true

	// Register all SignalR callbacks before starting.
	c.sr.OnMessage(func(msg gm.MessageModel) {
		c.handleIncomingMessage(msg)
	})

	c.sr.OnStatusUpdate(func(upd gm.MessageStatusUpdate) {
		c.handleStatusUpdate(upd)
	})

	c.sr.OnOpen(func() {
		c.log.Info().Msg("SignalR connected to Garmin Messenger")
		c.userLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateConnected,
		})
		// On reconnect (not the first connect), backfill anything we missed
		// while the WebSocket was down. Run in a goroutine so the SignalR
		// callback returns promptly.
		if c.initialSyncDone {
			go func() {
				if err := c.syncMissedMessages(context.Background(), false); err != nil {
					c.log.Warn().Err(err).Msg("Reconnect catch-up sync failed")
				}
			}()
		}
	})

	c.sr.OnClose(func() {
		c.log.Warn().Msg("SignalR disconnected")
		c.userLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      "garmin-signalr-disconnected",
			Message:    "Disconnected from Garmin Messenger real-time service",
		})
	})

	c.sr.OnError(func(err error) {
		c.log.Err(err).Msg("SignalR reported error")
		if isPermanentAuthError(err) {
			c.userLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateBadCredentials,
				Error:      "garmin-session-expired",
				Message:    "Garmin session expired or revoked. Please re-login.",
			})
		}
	})

	// Start() blocks until ctx is cancelled; the library handles reconnects.
	go func() {
		if err := c.sr.Start(ctx); err != nil && ctx.Err() == nil {
			c.log.Err(err).Msg("SignalR Start returned error")
			c.sendErrorState(err, "Lost connection to Garmin Messenger: "+err.Error())
		}
	}()

	// The framework only sets the space room avatar on initial creation.
	// Update it on every connect so it reflects the current bot avatar from config.
	go c.ensureSpaceAvatar(ctx)
}

// ensureSpaceAvatar updates the space room's m.room.avatar to match the
// NetworkIcon returned by GetName() (which uses the bot's configured avatar).
// Called on every Connect() so that avatar changes in config.yaml take effect
// on the next restart without needing to recreate the space room.
func (c *GarminClient) ensureSpaceAvatar(ctx context.Context) {
	icon := c.connector.br.Network.GetName().NetworkIcon
	if icon == "" {
		return
	}
	spaceRoom, err := c.userLogin.GetSpaceRoom(ctx)
	if err != nil || spaceRoom == "" {
		return
	}
	if _, err := c.userLogin.Bridge.Bot.SendState(ctx, spaceRoom, event.StateRoomAvatar, "", &event.Content{
		Parsed: &event.RoomAvatarEventContent{URL: icon},
	}, time.Now()); err != nil {
		c.log.Warn().Err(err).Msg("Failed to update space room avatar")
	}
}

// Disconnect stops the SignalR connection cleanly.
func (c *GarminClient) Disconnect() {
	// Flush any pending LastSyncTime save before tearing down so we don't lose
	// the cursor for the next bridge start.
	c.flushSyncTimeSave(context.Background())
	c.sr.Stop()
	c.api.Close()
}

// isPermanentAuthError returns true if err is an *gm.APIError with status 401
// or 403, indicating the user must re-authenticate.
func isPermanentAuthError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *gm.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden
	}
	return false
}

// sendErrorState classifies err and sends an appropriate BridgeState event.
// Permanent auth failures get StateBadCredentials with a clear message; all
// other errors get StateTransientDisconnect.
func (c *GarminClient) sendErrorState(err error, transientMsg string) {
	if isPermanentAuthError(err) {
		c.userLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      "garmin-session-expired",
			Message:    "Garmin session expired or revoked. Please re-login.",
		})
		return
	}
	c.userLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      "garmin-connect-error",
		Message:    transientMsg,
	})
}

// syncMissedMessages performs a catch-up backfill of messages and (optionally)
// status updates that arrived since c.userLogin.Metadata.LastSyncTime. It is
// safe to call multiple times — the bridgev2 framework deduplicates events by
// (PortalKey, MessageID).
//
// When isInitial is true (catch-up after a bridge restart), status receipts
// are skipped: the Matrix events they reference may predate the bot's current
// room access, causing M_FORBIDDEN errors when bridgev2 tries to mark them as
// read, and the receipts are no longer actionable in any case.
//
// The catch-up window is capped at maxCatchupWindow to prevent an arbitrarily
// large backfill if LastSyncTime is missing or very stale.
func (c *GarminClient) syncMissedMessages(ctx context.Context, isInitial bool) error {
	if !c.catchingUp.CompareAndSwap(false, true) {
		c.log.Debug().Msg("Catch-up already in progress, skipping")
		return nil
	}
	defer c.catchingUp.Store(false)

	meta, _ := c.userLogin.Metadata.(*UserLoginMetadata)
	if meta == nil {
		return fmt.Errorf("user login has no metadata")
	}

	now := time.Now()
	earliest := now.Add(-maxCatchupWindow)
	var since time.Time
	if meta.LastSyncTime != nil && meta.LastSyncTime.After(earliest) {
		since = *meta.LastSyncTime
	} else {
		since = earliest
		if meta.LastSyncTime != nil {
			c.log.Warn().
				Time("last_sync_time", *meta.LastSyncTime).
				Time("capped_to", since).
				Msg("LastSyncTime is older than maxCatchupWindow; capping catch-up window")
		}
	}

	c.log.Info().Time("since", since).Msg("Starting catch-up sync")

	// 1. Find conversations updated since `since`.
	convs, err := c.api.GetConversations(ctx, gm.WithAfterDate(since), gm.WithLimit(500))
	if err != nil {
		return fmt.Errorf("list updated conversations: %w", err)
	}

	msgCount := 0
	for _, conv := range convs.Conversations {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		detail, err := c.api.GetConversationDetail(ctx, conv.ConversationID, gm.WithDetailLimit(200))
		if err != nil {
			c.log.Warn().Err(err).
				Stringer("conversation_id", conv.ConversationID).
				Msg("Failed to fetch conversation detail during catch-up")
			continue
		}
		// Pre-populate the recent-messages cache with the FULL detail (not
		// just messages newer than `since`), so reactions whose parent is
		// older than the catch-up window can still be resolved.
		c.bulkCacheRecentMessages(conv.ConversationID, detail.Messages)
		for _, m := range detail.Messages {
			if m.SentAt != nil && m.SentAt.Before(since) {
				continue
			}
			c.handleIncomingMessage(conversationMsgToModel(conv.ConversationID, m))
			msgCount++
		}
		// Small delay to avoid hammering the API.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	// 2. Catch up on read/delivered receipts. Skipped on initial (post-restart)
	// sync — see function doc.
	if !isInitial {
		statuses, err := c.api.GetUpdatedStatuses(ctx, since, gm.WithStatusLimit(500))
		if err != nil {
			c.log.Warn().Err(err).Msg("Failed to fetch updated statuses during catch-up")
		} else {
			for _, sm := range statuses.StatusReceiptsForMessages {
				for _, r := range sm.StatusReceipts {
					upd := gm.MessageStatusUpdate{
						MessageID: gm.SimpleCompoundMessageId{
							MessageID:      sm.MessageID,
							ConversationID: sm.ConversationID,
						},
						MessageStatus: &r.MessageStatus,
						UpdatedAt:     r.UpdatedAt,
					}
					if r.UserID != "" {
						if uid, err := uuid.Parse(r.UserID); err == nil {
							upd.UserID = &uid
						}
					}
					c.handleStatusUpdate(upd)
				}
			}
		}
	}

	// Mark this catch-up as complete by advancing LastSyncTime to the time we
	// started the sync (using `now` rather than time.Now() so we don't skip
	// over events that arrived during the sync itself).
	meta.LastSyncTime = &now
	if err := c.userLogin.Save(ctx); err != nil {
		c.log.Warn().Err(err).Msg("Failed to persist LastSyncTime after catch-up")
	}

	c.log.Info().
		Int("conversations", len(convs.Conversations)).
		Int("messages", msgCount).
		Msg("Catch-up sync complete")
	return nil
}

// conversationMsgToModel converts a ConversationMessageModel (returned by the
// REST conversation detail endpoint) into the MessageModel format used by the
// SignalR handler. ConversationMessageModel lacks ConversationID (it's
// implicit from the parent conversation) and a few other fields.
func conversationMsgToModel(convID uuid.UUID, m gm.ConversationMessageModel) gm.MessageModel {
	return gm.MessageModel{
		MessageID:        m.MessageID,
		ConversationID:   convID,
		ParentMessageID:  m.ParentMessageID,
		MessageBody:      m.MessageBody,
		From:             m.From,
		SentAt:           m.SentAt,
		ReceivedAt:       m.ReceivedAt,
		Status:           m.Status,
		UserLocation:     m.UserLocation,
		ReferencePoint:   m.ReferencePoint,
		MessageType:      m.MessageType,
		MapShareUrl:      m.MapShareUrl,
		MapSharePassword: m.MapSharePassword,
		LiveTrackUrl:     m.LiveTrackUrl,
		FromDeviceType:   m.FromDeviceType,
		MediaID:          m.MediaID,
		MediaType:        m.MediaType,
		UUID:             m.UUID,
		Transcription:    m.Transcription,
		OtaUuid:          m.OtaUuid,
		FromUnitID:       m.FromUnitID,
		IntendedUnitID:   m.IntendedUnitID,
	}
}

// cacheRecentMessage adds a single message to the per-conversation
// recent-messages ring buffer. Reaction messages are skipped — the cache is
// only used to find reaction targets, and a reaction is never a target.
// Duplicates by MessageID are silently ignored.
func (c *GarminClient) cacheRecentMessage(convID uuid.UUID, m gm.ConversationMessageModel) {
	if isReactionBody(derefStr(m.MessageBody)) {
		return
	}
	c.recentMu.Lock()
	defer c.recentMu.Unlock()
	if c.recentByConv == nil {
		c.recentByConv = make(map[uuid.UUID][]gm.ConversationMessageModel)
	}
	list := c.recentByConv[convID]
	for i := range list {
		if list[i].MessageID == m.MessageID {
			return
		}
	}
	list = append(list, m)
	if len(list) > recentMessagesPerConv {
		list = list[len(list)-recentMessagesPerConv:]
	}
	c.recentByConv[convID] = list
}

// bulkCacheRecentMessages merges a slice of messages (e.g. the result of
// GetConversationDetail) into the per-conversation cache. Used by catch-up
// sync to populate the cache before processing the conversation, so that
// any reactions in the same batch can be resolved against their preceding
// messages without an extra API call.
func (c *GarminClient) bulkCacheRecentMessages(convID uuid.UUID, messages []gm.ConversationMessageModel) {
	for _, m := range messages {
		c.cacheRecentMessage(convID, m)
	}
}

// snapshotRecentMessages returns a copy of the per-conversation cache. The
// copy is required because matchReactionTarget reads it without holding
// the cache lock.
func (c *GarminClient) snapshotRecentMessages(convID uuid.UUID) []gm.ConversationMessageModel {
	c.recentMu.Lock()
	defer c.recentMu.Unlock()
	list := c.recentByConv[convID]
	if len(list) == 0 {
		return nil
	}
	out := make([]gm.ConversationMessageModel, len(list))
	copy(out, list)
	return out
}

// resolveReactionParent is the unified entry point for reaction-target
// resolution. It tries the in-memory cache first; if the cache is empty for
// the conversation (which only happens before catch-up has populated it),
// it falls back to a single REST GetConversationDetail call.
func (c *GarminClient) resolveReactionParent(ctx context.Context, msg gm.MessageModel) (networkid.MessageID, error) {
	if cached := c.snapshotRecentMessages(msg.ConversationID); len(cached) > 0 {
		return resolveReactionParentFromMessages(msg, cached)
	}
	return c.resolveReactionParentID(ctx, msg)
}

// noteMessageReceived is called from handleIncomingMessage to advance the
// LastSyncTime cursor as live messages arrive. The actual database write is
// debounced via debounceSaveInterval to avoid one DB write per Garmin event.
func (c *GarminClient) noteMessageReceived(ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}
	c.syncTimeMu.Lock()
	defer c.syncTimeMu.Unlock()
	if c.pendingSaveTime != nil && !ts.After(*c.pendingSaveTime) {
		// We already have a pending save with a newer timestamp.
	} else {
		t := ts
		c.pendingSaveTime = &t
	}
	if c.saveTimer == nil {
		c.saveTimer = time.AfterFunc(debounceSaveInterval, func() {
			c.flushSyncTimeSave(context.Background())
		})
	}
}

// flushSyncTimeSave persists any pending LastSyncTime update immediately.
// Called both from the debounce timer and from Disconnect().
func (c *GarminClient) flushSyncTimeSave(ctx context.Context) {
	c.syncTimeMu.Lock()
	pending := c.pendingSaveTime
	c.pendingSaveTime = nil
	if c.saveTimer != nil {
		c.saveTimer.Stop()
		c.saveTimer = nil
	}
	c.syncTimeMu.Unlock()

	if pending == nil {
		return
	}
	meta, ok := c.userLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return
	}
	if meta.LastSyncTime != nil && !pending.After(*meta.LastSyncTime) {
		return
	}
	meta.LastSyncTime = pending
	if err := c.userLogin.Save(ctx); err != nil {
		c.log.Warn().Err(err).Msg("Failed to persist LastSyncTime")
	}
}

// IsLoggedIn returns true if the auth session has credentials.
// Must not do IO.
func (c *GarminClient) IsLoggedIn() bool {
	return c.phone != ""
}

// LogoutRemote invalidates the remote session.
// Garmin has no explicit logout endpoint; clearing the session file is enough.
func (c *GarminClient) LogoutRemote(_ context.Context) {
	sessDir := c.connector.sessionDir(c.userLogin.ID)
	credFile := sessDir + "/hermes_credentials.json"
	if err := removeFile(credFile); err != nil {
		c.log.Warn().Err(err).Msg("Failed to remove session file on logout")
	}
}

// GetCapabilities returns Matrix room feature limits.
func (c *GarminClient) GetCapabilities(_ context.Context, _ *bridgev2.Portal) *event.RoomFeatures {
	imageMIMEs := map[string]event.CapabilitySupportLevel{
		"image/jpeg": event.CapLevelFullySupported,
		"image/png":  event.CapLevelFullySupported,
		"image/webp": event.CapLevelFullySupported,
		"image/avif": event.CapLevelFullySupported,
	}
	audioMIMEs := map[string]event.CapabilitySupportLevel{
		"audio/ogg":  event.CapLevelFullySupported,
		"audio/mpeg": event.CapLevelFullySupported,
		"audio/mp4":  event.CapLevelFullySupported,
		"audio/wav":  event.CapLevelFullySupported,
		"audio/webm": event.CapLevelFullySupported,
	}
	return &event.RoomFeatures{
		MaxTextLength: 160,
		Reaction:      event.CapLevelFullySupported,
		File: event.FileFeatureMap{
			event.MsgImage:    {MimeTypes: imageMIMEs, Caption: event.CapLevelPartialSupport},
			event.MsgAudio:    {MimeTypes: audioMIMEs},
			event.MsgFile:     {MimeTypes: audioMIMEs},
			event.CapMsgVoice: {MimeTypes: audioMIMEs},
		},
	}
}

// GetChatInfo returns the Matrix room configuration for a Garmin conversation.
func (c *GarminClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	// Synthetic portals created for new conversations before the first message is sent.
	if strings.HasPrefix(string(portal.ID), "phone:") {
		phone := strings.TrimPrefix(string(portal.ID), "phone:")
		return c.chatInfoForRecipient(phone), nil
	}
	if strings.HasPrefix(string(portal.ID), "email:") {
		email := strings.TrimPrefix(string(portal.ID), "email:")
		return c.chatInfoForRecipient(email), nil
	}

	convID, err := uuid.Parse(string(portal.ID))
	if err != nil {
		return nil, fmt.Errorf("invalid conversation ID %q: %w", portal.ID, err)
	}
	members, err := c.api.GetConversationMembers(ctx, convID)
	if err != nil {
		return nil, fmt.Errorf("get members for %s: %w", portal.ID, err)
	}

	var chatMembers []bridgev2.ChatMember

	// Add ourselves.
	chatMembers = append(chatMembers, bridgev2.ChatMember{
		EventSender: bridgev2.EventSender{
			IsFromMe: true,
			// Our own Hermes UUID derived from our phone number.
			Sender: ghostIDFromHermesID(gm.PhoneToHermesUserID(c.phone)),
		},
		Membership: event.MembershipJoin,
		PowerLevel: ptrInt(50),
	})

	// Add remote members. UserInfoModel.Address is the phone number.
	for _, m := range members {
		addr := derefStr(m.Address)
		if addr == c.phone {
			continue // skip ourselves
		}
		hermesID := gm.PhoneToHermesUserID(addr)
		chatMembers = append(chatMembers, bridgev2.ChatMember{
			EventSender: bridgev2.EventSender{
				Sender: ghostIDFromHermesID(hermesID),
			},
			Membership: event.MembershipJoin,
			PowerLevel: ptrInt(50),
		})
	}

	// Collect recipient phone numbers (everyone except ourselves).
	// These are required for sending messages from Matrix to Garmin,
	// because the Garmin API sends by phone number, not conversation ID.
	var recipientPhones []string
	for _, m := range members {
		addr := derefStr(m.Address)
		if addr != "" && addr != c.phone {
			recipientPhones = append(recipientPhones, addr)
		}
	}

	info := &bridgev2.ChatInfo{
		Members: &bridgev2.ChatMemberList{
			IsFull:  true,
			Members: chatMembers,
		},
		ExtraUpdates: func(_ context.Context, portal *bridgev2.Portal) bool {
			meta, ok := portal.Metadata.(*PortalMetadata)
			if !ok {
				meta = &PortalMetadata{}
				portal.Metadata = meta
			}
			if !slicesEqual(meta.RecipientPhones, recipientPhones) {
				meta.RecipientPhones = recipientPhones
				return true // metadata changed
			}
			return false
		},
	}

	// Group chats (>2 members) get a comma-separated name.
	if len(members) > 2 {
		name := buildGroupName(members, c.phone)
		info.Name = &name
	}

	return info, nil
}

// chatInfoForRecipient builds a ChatInfo for a new conversation with a single
// recipient identified by phone number or email address.
// Used for synthetic "phone:+E164" and "email:addr" portals created by
// ResolveIdentifier when no existing conversation exists yet.
func (c *GarminClient) chatInfoForRecipient(recipient string) *bridgev2.ChatInfo {
	recipientGhostID := ghostIDForRecipient(recipient)
	chatMembers := []bridgev2.ChatMember{
		{
			EventSender: bridgev2.EventSender{
				IsFromMe: true,
				Sender:   ghostIDFromHermesID(gm.PhoneToHermesUserID(c.phone)),
			},
			Membership: event.MembershipJoin,
			PowerLevel: ptrInt(50),
		},
		{
			EventSender: bridgev2.EventSender{
				Sender: recipientGhostID,
			},
			Membership: event.MembershipJoin,
			PowerLevel: ptrInt(50),
		},
	}
	recipients := []string{recipient}
	// Set the room name to the recipient address so the Matrix room is named
	// after the phone number or email rather than the raw ghost UUID.
	name := recipient
	return &bridgev2.ChatInfo{
		Name: &name,
		Members: &bridgev2.ChatMemberList{
			IsFull:  true,
			Members: chatMembers,
		},
		ExtraUpdates: func(_ context.Context, portal *bridgev2.Portal) bool {
			meta, ok := portal.Metadata.(*PortalMetadata)
			if !ok {
				meta = &PortalMetadata{}
				portal.Metadata = meta
			}
			if !slicesEqual(meta.RecipientPhones, recipients) {
				meta.RecipientPhones = recipients
				return true
			}
			return false
		},
	}
}

// ghostIDForRecipient returns a stable ghost UserID for a phone number or email.
// For phone numbers (start with +), derives the Hermes UUID via PhoneToHermesUserID.
// For email addresses, uses the address directly as the ID.
func ghostIDForRecipient(recipient string) networkid.UserID {
	if strings.HasPrefix(recipient, "+") {
		return ghostIDFromHermesID(gm.PhoneToHermesUserID(recipient))
	}
	// Email or other address: use as-is (lowercased for consistency).
	return networkid.UserID(strings.ToLower(recipient))
}

// GetUserInfo returns ghost profile data (displayname, avatar, identifiers).
// The ghost ID is the Hermes UUID, but we want to show a human-friendly name.
func (c *GarminClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	id := string(ghost.ID)

	// Email-based ghost IDs (from email: portals) — use the address as display name.
	if strings.Contains(id, "@") {
		return &bridgev2.UserInfo{
			Identifiers: []string{"mailto:" + id},
			Name:        ptrStr(id),
		}, nil
	}

	// Regular Hermes UUID: try to resolve to a phone number via conversation members.
	if phone, name, imageURL := c.lookupPhoneFromUUID(ctx, id); phone != "" {
		displayName := name
		if displayName == "" {
			displayName = phone
		}
		info := &bridgev2.UserInfo{
			Identifiers: []string{"tel:" + phone},
			Name:        ptrStr(displayName),
		}
		if imageURL != "" {
			info.Avatar = avatarFromURL(imageURL)
		}
		return info, nil
	}

	// Fallback: use the Hermes UUID itself as the display name.
	return &bridgev2.UserInfo{
		Name: ptrStr(id),
	}, nil
}

// avatarFromURL builds a bridgev2.Avatar that lazily downloads from a remote HTTP URL.
// The URL is used as the AvatarID so the framework skips re-upload when unchanged.
func avatarFromURL(url string) *bridgev2.Avatar {
	return &bridgev2.Avatar{
		ID: networkid.AvatarID(url),
		Get: func(ctx context.Context) ([]byte, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return nil, err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			return io.ReadAll(resp.Body)
		},
	}
}

// IsThisUser checks whether a ghost ID belongs to the logged-in user.
func (c *GarminClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return ghostIDFromHermesID(gm.PhoneToHermesUserID(c.phone)) == userID
}

// ─── Matrix → Garmin ─────────────────────────────────────────────────────────

// HandleMatrixMessage sends a Matrix message (text or media) to Garmin.
// The Garmin API sends by phone number, so we read recipients from PortalMetadata.
// Media is transcoded via ffmpeg: images → AVIF, audio → OGG.
func (c *GarminClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	meta, ok := msg.Portal.Metadata.(*PortalMetadata)
	if !ok || len(meta.RecipientPhones) == 0 {
		return nil, fmt.Errorf("portal has no recipient phone numbers — cannot send")
	}

	var result *gm.SendMessageV2Response
	var err error

	switch msg.Content.MsgType {
	case event.MsgText, event.MsgNotice, event.MsgEmote:
		result, err = c.api.SendMessage(ctx, meta.RecipientPhones, msg.Content.Body)

	case event.MsgImage, event.MsgAudio, event.MsgFile:
		result, err = c.sendMedia(ctx, msg, meta.RecipientPhones)

	default:
		return nil, fmt.Errorf("unsupported message type: %s", msg.Content.MsgType)
	}

	if err != nil {
		return nil, fmt.Errorf("send to garmin: %w", err)
	}

	// When sending the first message to a synthetic "phone:+E164" or "email:addr"
	// portal, the Garmin API creates a new conversation and returns its UUID. Store
	// the mapping so incoming SignalR messages for that conversation are routed to
	// this portal instead of creating a new one with the real UUID.
	if strings.HasPrefix(string(msg.Portal.ID), "phone:") || strings.HasPrefix(string(msg.Portal.ID), "email:") {
		pendingKey := networkid.PortalKey{
			ID:       msg.Portal.ID,
			Receiver: c.userLogin.ID,
		}
		c.pendingConvPortals.Store(result.ConversationID.String(), pendingKey)
		c.log.Debug().
			Str("conv_id", result.ConversationID.String()).
			Str("portal_id", string(msg.Portal.ID)).
			Msg("Stored real conversation ID for pending portal")
	}

	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:       networkid.MessageID(result.MessageID.String()),
			SenderID: ghostIDFromHermesID(gm.PhoneToHermesUserID(c.phone)),
		},
	}, nil
}

// sendMedia downloads the Matrix media, transcodes it to AVIF/OGG, and sends
// it to Garmin using api.SendMediaMessage.
func (c *GarminClient) sendMedia(ctx context.Context, msg *bridgev2.MatrixMessage, recipients []string) (*gm.SendMessageV2Response, error) {
	// Download from Matrix media repo via the bridge bot's Matrix client.
	data, err := msg.Portal.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
	if err != nil {
		return nil, fmt.Errorf("download Matrix media: %w", err)
	}

	srcMime := msg.Content.GetInfo().MimeType

	// Determine target Garmin media type and transcode.
	var transcoded []byte
	var gmMediaType gm.MediaType

	switch {
	case isImageMIME(srcMime):
		gmMediaType = gm.MediaTypeImageAvif
		transcoded, err = ToGarminAVIF(ctx, data, srcMime)
		if err != nil {
			return nil, fmt.Errorf("transcode to AVIF: %w", err)
		}
	case isAudioMIME(srcMime):
		gmMediaType = gm.MediaTypeAudioOgg
		transcoded, err = ToGarminOGG(ctx, data, srcMime)
		if err != nil {
			return nil, fmt.Errorf("transcode to OGG: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported media MIME type for Garmin: %s", srcMime)
	}

	// Build extra options (e.g. duration for audio).
	var extraOpts []gm.SendMessageOption
	if gmMediaType == gm.MediaTypeAudioOgg {
		if durationMS := ProbeAudioDurationMS(ctx, transcoded, "ogg"); durationMS > 0 {
			extraOpts = append(extraOpts, gm.WithMediaMetadata(gm.MediaMetadata{DurationMs: &durationMS}))
			c.log.Debug().Int("durationMs", durationMS).Msg("Probed OGG duration for Garmin send")
		}
	}

	// GetCaption() returns non-empty only when Body and FileName differ,
	// i.e. the user actually typed a caption. When no caption is given,
	// Body == FileName (or FileName is empty), so GetCaption() returns "".
	result, err := c.api.SendMediaMessage(ctx, recipients, msg.Content.GetCaption(), transcoded, gmMediaType, extraOpts...)
	if err != nil {
		return nil, fmt.Errorf("SendMediaMessage: %w", err)
	}
	return result, nil
}

// ─── Garmin → Matrix ─────────────────────────────────────────────────────────

// handleIncomingMessage is the sr.OnMessage callback.
// gm.MessageModel uses uuid.UUID for IDs and *time.Time for timestamps.
func (c *GarminClient) handleIncomingMessage(msg gm.MessageModel) {
	if msg.ConversationID == (uuid.UUID{}) {
		c.log.Warn().Msg("Received message with zero ConversationID — ignoring")
		return
	}

	convIDStr := msg.ConversationID.String()
	msgIDStr := msg.MessageID.String()
	senderRaw := derefStr(msg.From)

	// The Garmin API returns the sender as either a phone number (+47...)
	// or a Hermes UUID, depending on the message source. Normalize to a
	// Hermes UUID so ghost IDs are consistent with GetChatInfo (which
	// always derives UUIDs from phone numbers via PhoneToHermesUserID).
	senderUUID := normalizeSenderID(senderRaw)

	// Route to the pending "phone:+E164" portal if we sent the first message
	// that created this conversation (avoids creating a duplicate portal with
	// the real UUID alongside the synthetic one).
	portalID := portalIDFromConversation(convIDStr)
	if val, ok := c.pendingConvPortals.Load(convIDStr); ok {
		if pk, ok := val.(networkid.PortalKey); ok {
			portalID = pk.ID
			c.log.Debug().
				Str("conv_id", convIDStr).
				Str("portal_id", string(portalID)).
				Msg("Routing incoming message to pending portal")
		}
	}

	// Garmin encodes reactions as \u200b{emoji}\u200b to \u200a{quoted}\u200a.
	// parentMessageId is never populated by the Garmin server, so we resolve
	// the target via text-matching against the per-conversation in-memory
	// recent-messages cache (populated by every prior message and by
	// catch-up sync).
	if isReactionBody(derefStr(msg.MessageBody)) {
		go c.handleIncomingReaction(msg, portalID, senderUUID)
		return
	}

	// Cache the message body so future reactions in this conversation can be
	// resolved by text-match without an API call.
	c.cacheRecentMessage(msg.ConversationID, gm.ConversationMessageModel{
		MessageID:   msg.MessageID,
		MessageBody: msg.MessageBody,
		From:        msg.From,
		SentAt:      msg.SentAt,
		ReceivedAt:  msg.ReceivedAt,
	})

	c.userLogin.Bridge.QueueRemoteEvent(c.userLogin, &simplevent.Message[gm.MessageModel]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventMessage,
			LogContext: func(ctx zerolog.Context) zerolog.Context {
				return ctx.
					Str("garmin_msg_id", msgIDStr).
					Str("conversation_id", convIDStr).
					Str("sender_raw", senderRaw).
					Str("sender_uuid", senderUUID)
			},
			PortalKey: networkid.PortalKey{
				ID:       portalID,
				Receiver: c.userLogin.ID,
			},
			CreatePortal: true,
			Sender: bridgev2.EventSender{
				Sender: ghostIDFromHermesID(senderUUID),
				// IsFromMe is true if the sender UUID matches our own.
				IsFromMe: senderUUID == gm.PhoneToHermesUserID(c.phone),
			},
			Timestamp: derefTime(msg.SentAt),
		},
		Data:               msg,
		ID:                 networkid.MessageID(msgIDStr),
		ConvertMessageFunc: c.convertMessage,
	})

	// Advance the catch-up cursor so a future restart/reconnect knows it can
	// skip everything up to this point. Saved via debounced background flush.
	c.noteMessageReceived(derefTime(msg.SentAt))

	// Mark as delivered via SignalR (real-time, preferred over REST).
	c.sr.MarkAsDelivered(msg.ConversationID, msg.MessageID)
}

// handleIncomingReaction resolves the reaction's target via text-matching
// against the in-memory recent-messages cache (populated by every prior
// message in the conversation, including the catch-up backfill). If the
// cache is empty for this conversation — which only happens for the very
// first message after a fresh start before catch-up has run — it falls back
// to a single GetConversationDetail call.
//
// Called in a goroutine from handleIncomingMessage.
func (c *GarminClient) handleIncomingReaction(msg gm.MessageModel, portalID networkid.PortalID, senderUUID string) {
	emoji := extractReactionEmoji(derefStr(msg.MessageBody))
	parentID, err := c.resolveReactionParent(context.Background(), msg)
	if err != nil {
		// During catch-up, dropping an unresolvable old reaction is expected
		// (the server may have aged out its parent). Log at debug there to
		// avoid noise.
		ev := c.log.Warn()
		if c.catchingUp.Load() {
			ev = c.log.Debug()
		}
		ev.Err(err).
			Str("msg_id", msg.MessageID.String()).
			Msg("Could not resolve reaction parent — dropping")
		return
	}
	c.userLogin.Bridge.QueueRemoteEvent(c.userLogin, &simplevent.Reaction{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventReaction,
			LogContext: func(ctx zerolog.Context) zerolog.Context {
				return ctx.
					Str("garmin_msg_id", msg.MessageID.String()).
					Str("parent_msg_id", string(parentID)).
					Str("emoji", emoji)
			},
			PortalKey: networkid.PortalKey{
				ID:       portalID,
				Receiver: c.userLogin.ID,
			},
			Sender: bridgev2.EventSender{
				Sender: networkid.UserID(senderUUID),
			},
			Timestamp: derefTime(msg.SentAt),
		},
		TargetMessage: parentID,
		Emoji:         emoji,
	})
}

// PreHandleMatrixReaction is called first to identify the reaction for deduplication.
func (c *GarminClient) PreHandleMatrixReaction(_ context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	return bridgev2.MatrixReactionPreResponse{
		SenderID:     ghostIDFromHermesID(gm.PhoneToHermesUserID(c.phone)),
		Emoji:        msg.Content.RelatesTo.Key,
		MaxReactions: 1,
	}, nil
}

// HandleMatrixReaction sends a Matrix reaction to Garmin using the same ZWS-encoded
// body format the native app uses: \u200b{emoji}\u200b to \u200a{original}\u200a
// The server matches the quoted text to a message in the conversation to resolve
// parentMessageId — same heuristic (most recent match) as the native app.
func (c *GarminClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	meta, ok := msg.Portal.Metadata.(*PortalMetadata)
	if !ok || len(meta.RecipientPhones) == 0 {
		return nil, fmt.Errorf("portal has no recipient phone numbers — cannot send reaction")
	}
	conversationID, err := uuid.Parse(string(msg.Portal.ID))
	if err != nil {
		return nil, fmt.Errorf("parse conversation ID: %w", err)
	}
	targetMsgID, err := uuid.Parse(string(msg.TargetMessage.ID))
	if err != nil {
		return nil, fmt.Errorf("parse target message ID: %w", err)
	}
	originalBody, err := c.resolveReactionOriginalBody(ctx, conversationID, targetMsgID)
	if err != nil {
		c.log.Warn().Err(err).Msg("Could not resolve reaction target body — sending emoji only")
		originalBody = msg.Content.RelatesTo.Key
	}
	body := buildReactionBody(msg.Content.RelatesTo.Key, originalBody)
	if _, err := c.api.SendMessage(ctx, meta.RecipientPhones, body); err != nil {
		return nil, fmt.Errorf("send reaction to Garmin: %w", err)
	}
	return &database.Reaction{}, nil
}

// HandleMatrixReactionRemove is called when a reaction is redacted on Matrix.
// Garmin has no reaction removal API, so we silently ignore this.
func (c *GarminClient) HandleMatrixReactionRemove(_ context.Context, _ *bridgev2.MatrixReactionRemove) error {
	return nil
}

// HandleMatrixReadReceipt forwards a Matrix read receipt to Garmin via SignalR MarkAsRead.
func (c *GarminClient) HandleMatrixReadReceipt(_ context.Context, msg *bridgev2.MatrixReadReceipt) error {
	if msg.ExactMessage == nil {
		return nil
	}
	conversationID, err := uuid.Parse(string(msg.Portal.ID))
	if err != nil {
		return fmt.Errorf("parse conversation ID: %w", err)
	}
	messageID, err := uuid.Parse(string(msg.ExactMessage.ID))
	if err != nil {
		return fmt.Errorf("parse message ID: %w", err)
	}
	c.sr.MarkAsRead(conversationID, messageID)
	return nil
}

// handleStatusUpdate is the sr.OnStatusUpdate callback.
func (c *GarminClient) handleStatusUpdate(upd gm.MessageStatusUpdate) {
	if upd.MessageStatus == nil {
		return
	}
	msgStatus := *upd.MessageStatus

	var evtType bridgev2.RemoteEventType
	switch msgStatus {
	case gm.MessageStatusRead:
		evtType = bridgev2.RemoteEventReadReceipt
	case gm.MessageStatusDelivered:
		evtType = bridgev2.RemoteEventDeliveryReceipt
	default:
		return
	}

	convIDStr := upd.MessageID.ConversationID.String()
	msgIDStr := upd.MessageID.MessageID.String()

	evt := &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type: evtType,
			PortalKey: networkid.PortalKey{
				ID:       portalIDFromConversation(convIDStr),
				Receiver: c.userLogin.ID,
			},
		},
		LastTarget: networkid.MessageID(msgIDStr),
	}
	// Use the Garmin UserID from the status update as the ghost sender so the
	// receipt shows with the contact's avatar rather than the bridge bot.
	if upd.UserID != nil {
		evt.Sender = bridgev2.EventSender{
			Sender: ghostIDFromHermesID(upd.UserID.String()),
		}
	}
	c.userLogin.Bridge.QueueRemoteEvent(c.userLogin, evt)
}

// ─── IdentifierResolvingNetworkAPI ───────────────────────────────────────────

// ResolveIdentifier searches existing conversations for a member matching
// the given phone number or email address. When createChat is true and no
// existing conversation is found, returns a synthetic portal so the user can
// send the first message (which will create the Garmin conversation).
func (c *GarminClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	isEmail := strings.Contains(identifier, "@")

	if isEmail {
		return c.resolveEmail(ctx, identifier, createChat)
	}
	return c.resolvePhone(ctx, identifier, createChat)
}

func (c *GarminClient) resolvePhone(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	phone := "+" + normalizePhone(identifier)
	targetUUID := gm.PhoneToHermesUserID(phone)

	convs, err := c.api.GetConversations(ctx, gm.WithLimit(500))
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}

	// Only match a 1-on-1 DM (exactly 2 members: the user and the contact).
	// If the contact only appears in group conversations, fall through and
	// create a new synthetic DM portal below.
	for _, conv := range convs.Conversations {
		if len(conv.MemberIDs) != 2 {
			continue
		}
		for _, memberUUID := range conv.MemberIDs {
			if strings.ToLower(memberUUID) != targetUUID {
				continue
			}
			return c.resolveExistingConv(ctx, conv.ConversationID.String(), ghostIDFromHermesID(memberUUID))
		}
	}

	if !createChat {
		return nil, fmt.Errorf("no Garmin Messenger conversation found with %s", identifier)
	}

	c.log.Info().Str("phone", phone).Msg("No existing conversation; creating synthetic portal for new chat")
	ghostID := ghostIDForRecipient(phone)
	return c.resolveNewChat(ctx, "phone:"+phone, ghostID, phone)
}

func (c *GarminClient) resolveEmail(ctx context.Context, email string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	email = strings.ToLower(strings.TrimSpace(email))

	if !createChat {
		return nil, fmt.Errorf("no Garmin Messenger conversation found with %s", email)
	}

	c.log.Info().Str("email", email).Msg("No existing conversation; creating synthetic portal for email chat")
	ghostID := ghostIDForRecipient(email)
	return c.resolveNewChat(ctx, "email:"+email, ghostID, email)
}

// resolveExistingConv builds a ResolveIdentifierResponse for a known conversation UUID.
func (c *GarminClient) resolveExistingConv(ctx context.Context, convIDStr string, ghostUserID networkid.UserID) (*bridgev2.ResolveIdentifierResponse, error) {
	portalKey := networkid.PortalKey{
		ID:       portalIDFromConversation(convIDStr),
		Receiver: c.userLogin.ID,
	}
	ghost, err := c.userLogin.Bridge.GetGhostByID(ctx, ghostUserID)
	if err != nil {
		return nil, fmt.Errorf("get ghost: %w", err)
	}
	portal, err := c.userLogin.Bridge.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return nil, fmt.Errorf("get portal: %w", err)
	}
	ghostInfo, _ := c.GetUserInfo(ctx, ghost)
	portalInfo, _ := c.GetChatInfo(ctx, portal)
	return &bridgev2.ResolveIdentifierResponse{
		Ghost:    ghost,
		UserID:   ghostUserID,
		UserInfo: ghostInfo,
		Chat: &bridgev2.CreateChatResponse{
			Portal:     portal,
			PortalKey:  portalKey,
			PortalInfo: portalInfo,
		},
	}, nil
}

// resolveNewChat builds a ResolveIdentifierResponse for a brand-new synthetic portal.
func (c *GarminClient) resolveNewChat(ctx context.Context, portalIDStr string, ghostUserID networkid.UserID, recipient string) (*bridgev2.ResolveIdentifierResponse, error) {
	pendingKey := networkid.PortalKey{
		ID:       networkid.PortalID(portalIDStr),
		Receiver: c.userLogin.ID,
	}
	ghost, err := c.userLogin.Bridge.GetGhostByID(ctx, ghostUserID)
	if err != nil {
		return nil, fmt.Errorf("get ghost: %w", err)
	}
	portal, err := c.userLogin.Bridge.GetPortalByKey(ctx, pendingKey)
	if err != nil {
		return nil, fmt.Errorf("get portal: %w", err)
	}
	// Use the recipient address as the ghost display name directly.
	// GetUserInfo would fall back to the raw UUID for new ghosts with no
	// existing conversation, which makes the DM room and ghost unrecognizable.
	ghostInfo := &bridgev2.UserInfo{Name: ptrStr(recipient)}
	if strings.HasPrefix(recipient, "+") {
		ghostInfo.Identifiers = []string{"tel:" + recipient}
	} else if strings.Contains(recipient, "@") {
		ghostInfo.Identifiers = []string{"mailto:" + recipient}
	}
	portalInfo := c.chatInfoForRecipient(recipient)
	return &bridgev2.ResolveIdentifierResponse{
		Ghost:    ghost,
		UserID:   ghostUserID,
		UserInfo: ghostInfo,
		Chat: &bridgev2.CreateChatResponse{
			Portal:     portal,
			PortalKey:  pendingKey,
			PortalInfo: portalInfo,
		},
	}, nil
}

// ─── Internal helpers ────────────────────────────────────────────────────────

// lookupPhoneFromUUID scans conversations to find a member matching hermesUUID.
// Returns (phone, displayName, imageURL) or ("", "", "") if not found.
func (c *GarminClient) lookupPhoneFromUUID(ctx context.Context, hermesUUID string) (string, string, string) {
	convs, err := c.api.GetConversations(ctx, gm.WithLimit(50))
	if err != nil {
		return "", "", ""
	}
	for _, conv := range convs.Conversations {
		members, err := c.api.GetConversationMembers(ctx, conv.ConversationID)
		if err != nil {
			continue
		}
		for _, m := range members {
			addr := derefStr(m.Address)
			if addr == "" {
				continue
			}
			if gm.PhoneToHermesUserID(addr) == strings.ToLower(hermesUUID) {
				return addr, derefStr(m.FriendlyName), derefStr(m.ImageUrl)
			}
		}
	}
	return "", "", ""
}

// buildGroupName builds a display name for a group conversation.
func buildGroupName(members []gm.UserInfoModel, myPhone string) string {
	name := ""
	for _, m := range members {
		addr := derefStr(m.Address)
		if addr == myPhone {
			continue
		}
		if name != "" {
			name += ", "
		}
		if n := derefStr(m.FriendlyName); n != "" {
			name += n
		} else {
			name += addr
		}
	}
	return name
}

// derefTime safely dereferences a *time.Time, falling back to time.Now().
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Now()
	}
	return *t
}

// derefStr safely dereferences a *string pointer.
func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func ptrInt(v int) *int       { return &v }
func ptrStr(v string) *string { return &v }

// normalizePhone strips non-digit characters for comparison.
func normalizePhone(s string) string {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r >= '0' && r <= '9' {
			out = append(out, byte(r))
		}
	}
	return string(out)
}

// normalizeSenderID ensures a sender identifier from the Garmin API is always
// a Hermes UUID. The API may return either a phone number (+47...) or a UUID.
// Phone numbers are converted to UUIDs via PhoneToHermesUserID; UUIDs are
// lowercased for consistent matching.
func normalizeSenderID(raw string) string {
	if strings.HasPrefix(raw, "+") {
		return gm.PhoneToHermesUserID(raw)
	}
	return strings.ToLower(raw)
}

// slicesEqual compares two string slices for equality.
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// removeFile removes a file, ignoring "not found" errors.
func removeFile(path string) error {
	err := removeFileImpl(path)
	if err != nil && !isNotExist(err) {
		return err
	}
	return nil
}
