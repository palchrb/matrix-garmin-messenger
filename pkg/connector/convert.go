package connector

import (
	"context"
	"fmt"
	"html"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	gm "github.com/yourusername/matrix-garmin-messenger/internal/hermes"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

// convertMessage converts a gm.MessageModel into Matrix events.
// Called by the simplevent.Message handler inside the bridge event loop.
// All fields on gm.MessageModel are pointers — use derefStr()/derefFloat64().
func (c *GarminClient) convertMessage(
	ctx context.Context,
	_ *bridgev2.Portal,
	intent bridgev2.MatrixAPI,
	msg gm.MessageModel,
) (*bridgev2.ConvertedMessage, error) {
	var parts []*bridgev2.ConvertedMessagePart

	body := derefStr(msg.MessageBody)
	// Strip invisible delimiter characters Garmin uses in reaction message bodies
	// (\u200b zero-width space, \u200a hair space, \u2009 thin space).
	body = strings.Map(func(r rune) rune {
		switch r {
		case '\u200b', '\u200a', '\u2009':
			return -1
		}
		return r
	}, body)

	bodyText := body
	bodyHTML := ""
	hasLocation := false
	if msg.UserLocation != nil {
		lat := derefFloat64(msg.UserLocation.LatitudeDegrees)
		lon := derefFloat64(msg.UserLocation.LongitudeDegrees)
		if lat != 0 || lon != 0 {
			hasLocation = true
			osmURL := fmt.Sprintf(
				"https://www.openstreetmap.org/?mlat=%.6f&mlon=%.6f#map=14/%.6f/%.6f&layers=P",
				lat, lon, lat, lon,
			)

			// Build location suffix parts (altitude, speed)
			locSuffix := ""
			if alt := derefFloat64(msg.UserLocation.ElevationMeters); alt != 0 {
				locSuffix += fmt.Sprintf(" (%.0fm)", alt)
			}
			if spd := derefFloat64(msg.UserLocation.GroundVelocityMetersPerSecond); spd != 0 {
				locSuffix += fmt.Sprintf(", %.1f km/h", spd*3.6)
			}

			locPlain := fmt.Sprintf("Loc:📍%s\n%s", locSuffix, osmURL)
			locHTML := fmt.Sprintf("Loc:<a href=\"%s\">📍</a>%s", osmURL, locSuffix)

			if bodyText == "" {
				bodyText = locPlain
				bodyHTML = locHTML
			} else {
				bodyHTML = html.EscapeString(bodyText) + "<br><br>" + locHTML
				bodyText += "\n\n" + locPlain
			}
		}
	}

	// --- Media attachment (AVIF image or OGG audio from Garmin) ---
	// Download from Garmin, transcode if needed, reupload to Matrix.
	// Note: process media even when there's also a location/text body.
	if msg.MediaID != nil {
		mediaPart, transcription, err := c.bridgeIncomingMedia(ctx, intent, msg)
		if err != nil {
			// Don't drop the message. If we have text/location content, include the
			// media failure as a suffix to keep one Matrix part and avoid duplicate
			// DB writes with empty part IDs.
			mediaTypeStr := ""
			if msg.MediaType != nil {
				mediaTypeStr = string(*msg.MediaType)
			}
			c.log.Warn().Err(err).Str("msg_id", msg.MessageID.String()).Msg("Failed to bridge media")
			if bodyText != "" {
				mediaErrSuffix := fmt.Sprintf("\n\n[Media attachment (%s) — could not be downloaded]", mediaTypeStr)
				content := &event.MessageEventContent{
					MsgType: event.MsgText,
					Body:    bodyText + mediaErrSuffix,
				}
				if hasLocation {
					content.Format = event.FormatHTML
					content.FormattedBody = bodyHTML + "<br><br>" + fmt.Sprintf("[Media attachment (%s) — could not be downloaded]", mediaTypeStr)
				}
				parts = append(parts, &bridgev2.ConvertedMessagePart{
					Type:    event.EventMessage,
					Content: content,
				})
			} else {
				parts = append(parts, &bridgev2.ConvertedMessagePart{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType: event.MsgNotice,
						Body:    fmt.Sprintf("[Media attachment (%s) — could not be downloaded]", mediaTypeStr),
					},
				})
			}
		} else {
			// Transcription comes from the REST detail response (SignalR omits it).
			// Prefer REST-sourced transcription; fall back to SignalR if somehow present.
			effectiveTrans := transcription
			if effectiveTrans == nil {
				effectiveTrans = msg.Transcription
			}
			if effectiveTrans != nil {
				if t := strings.TrimSpace(*effectiveTrans); t != "" {
					if bodyText != "" {
						bodyText += "\n" + t
					} else {
						bodyText = t
					}
				}
			}
			if bodyText != "" {
				// Save the filename before overwriting body with the caption/transcription.
				mediaPart.Content.FileName = mediaPart.Content.Body
				mediaPart.Content.Body = bodyText
				if hasLocation {
					mediaPart.Content.Format = event.FormatHTML
					mediaPart.Content.FormattedBody = bodyHTML
				}
			}
			parts = append(parts, mediaPart)
		}
	}

	// --- Text fallback when there's no media attachment ---
	if len(parts) == 0 && bodyText != "" {
		content := &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    bodyText,
		}
		if hasLocation {
			content.Format = event.FormatHTML
			content.FormattedBody = bodyHTML
		}
		parts = append(parts, &bridgev2.ConvertedMessagePart{
			Type:    event.EventMessage,
			Content: content,
		})
	}

	// Fallback: don't silently drop messages.
	if len(parts) == 0 {
		parts = append(parts, &bridgev2.ConvertedMessagePart{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgNotice,
				Body:    "[Empty Garmin Messenger message]",
			},
		})
	}

	return &bridgev2.ConvertedMessage{Parts: parts}, nil
}

// bridgeIncomingMedia downloads a Garmin media attachment, transcodes it to a
// Matrix-friendly format, and reuploads it to the Matrix media repository.
// It also returns any transcription found in the REST detail response.
//
// Garmin sends AVIF images and OGG audio.
// Keep AVIF as-is (no conversion) and keep OGG as-is.
func (c *GarminClient) bridgeIncomingMedia(
	ctx context.Context,
	intent bridgev2.MatrixAPI,
	msg gm.MessageModel,
) (*bridgev2.ConvertedMessagePart, *string, error) {
	if msg.MediaID == nil {
		return nil, nil, fmt.Errorf("message has no media ID")
	}
	if msg.MediaType == nil {
		return nil, nil, fmt.Errorf("message has no media type")
	}

	msgUUID, transcription, err := c.resolveMediaMessageDetails(ctx, msg)
	if err != nil {
		return nil, nil, err
	}

	mediaID := *msg.MediaID
	mediaType := *msg.MediaType

	// Transcription only applies to audio; clear it for other media types.
	if mediaType != gm.MediaTypeAudioOgg {
		transcription = nil
	}

	// Download from Garmin using the REST API.
	data, err := c.api.DownloadMedia(
		ctx,
		msgUUID,
		mediaID,
		msg.MessageID,
		msg.ConversationID,
		mediaType,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("DownloadMedia: %w", err)
	}

	// Determine Matrix event type and MIME.
	var uploadData []byte
	var mxMsgType event.MessageType
	var mimeType string
	var filename string

	switch mediaType {
	case gm.MediaTypeImageAvif:
		uploadData = data
		mxMsgType = event.MsgImage
		mimeType = "image/avif"
		filename = "image.avif"

	case gm.MediaTypeAudioOgg:
		uploadData = data
		mxMsgType = event.MsgAudio
		mimeType = "audio/ogg"
		filename = "voice.ogg"

	default:
		return nil, nil, fmt.Errorf("unknown Garmin media type: %s", mediaType)
	}

	// Upload to Matrix media repository via the ghost user's intent.
	mxcURI, encryptedFile, err := intent.UploadMedia(ctx, "", uploadData, filename, mimeType)
	if err != nil {
		return nil, nil, fmt.Errorf("UploadMedia: %w", err)
	}

	content := &event.MessageEventContent{
		MsgType: mxMsgType,
		Body:    filename,
		URL:     mxcURI,
		File:    encryptedFile,
		Info: &event.FileInfo{
			MimeType: mimeType,
			Size:     len(uploadData),
		},
	}

	if mxMsgType == event.MsgAudio {
		durationMS := ProbeAudioDurationMS(ctx, uploadData, "ogg")
		if durationMS > 0 {
			content.Info.Duration = durationMS
		}
		content.MSC3245Voice = &event.MSC3245Voice{}
		content.MSC1767Audio = &event.MSC1767Audio{
			Duration: durationMS,
			Waveform: []int{},
		}
	}

	return &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: content,
	}, transcription, nil
}

// resolveMediaMessageDetails finds the UUID and transcription required for
// media message handling.
//
// SignalR events may omit MessageModel.UUID and MessageModel.Transcription.
// In that case, fetch recent conversation details and find the matching entry.
// As a final fallback, use MessageID as UUID (best-effort).
func (c *GarminClient) resolveMediaMessageDetails(ctx context.Context, msg gm.MessageModel) (uuid.UUID, *string, error) {
	if msg.UUID != nil {
		return *msg.UUID, msg.Transcription, nil
	}

	detail, err := c.api.GetConversationDetail(ctx, msg.ConversationID, gm.WithDetailLimit(100))
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("message has no UUID and lookup failed: %w", err)
	}

	for _, m := range detail.Messages {
		if m.MessageID != msg.MessageID {
			continue
		}
		if m.UUID != nil {
			return *m.UUID, m.Transcription, nil
		}
		break
	}

	c.log.Warn().
		Str("msg_id", msg.MessageID.String()).
		Str("conversation_id", msg.ConversationID.String()).
		Msg("Media message UUID not present in detail response, falling back to MessageID")
	return msg.MessageID, nil, nil
}

// isReactionBody reports whether a Garmin message body is a reaction.
// Garmin encodes reactions as: \u200b{emoji}\u200b {locale "to"} \u200a{quoted}\u200a
// (iOS may additionally wrap the quoted text in guillemets «...» and truncate
// long quotes with an ellipsis).
func isReactionBody(s string) bool {
	return strings.HasPrefix(s, "\u200b")
}

// reactionBodyRegex matches the Garmin reaction format and captures the emoji
// (group 1) and the quoted target text (group 2). The middle `.+?` allows for
// any locale-specific connector word ("to", "à", "auf", etc.). Optional
// guillemets «...» around the hair-spaced section handle iOS variants.
var reactionBodyRegex = regexp.MustCompile(`^\x{200b}(.+?)\x{200b}.+?«?\x{200a}(.*?)\x{200a}»?$`)

// extractReactionTarget parses a Garmin reaction body and returns the emoji
// and the quoted target text. ok is false if the body is not a valid
// reaction. Trailing ellipsis (… or ...) is stripped from the target text
// because iOS truncates long quotes.
func extractReactionTarget(body string) (emoji, target string, ok bool) {
	m := reactionBodyRegex.FindStringSubmatch(body)
	if m == nil {
		return "", "", false
	}
	target = m[2]
	target = strings.TrimSuffix(target, "\u2026") // …
	target = strings.TrimSuffix(target, "...")
	return m[1], target, true
}

// extractReactionEmoji returns just the emoji from a Garmin reaction body.
// Used by callers that don't need the target text.
func extractReactionEmoji(s string) string {
	if emoji, _, ok := extractReactionTarget(s); ok {
		return emoji
	}
	// Fallback for malformed bodies that still start with ZWS: take whatever
	// is between the first two ZWS characters.
	s = strings.TrimPrefix(s, "\u200b")
	if idx := strings.Index(s, "\u200b"); idx >= 0 {
		return s[:idx]
	}
	return s
}

// buildReactionBody constructs a Garmin-compatible reaction message body.
// Format mirrors what the native app sends: \u200b{emoji}\u200b to \u200a{original}\u200a
func buildReactionBody(emoji, originalBody string) string {
	return "\u200b" + emoji + "\u200b to \u200a" + originalBody + "\u200a"
}

// zwsStripper removes the zero-width / hair / thin space characters that
// Garmin native apps insert into reaction bodies. Used when comparing a
// quoted target text to candidate message bodies.
var zwsStripper = strings.NewReplacer(
	"\u200a", "", // hair space
	"\u200b", "", // zero-width space
	"\u2009", "", // thin space
)

// reactionMediaRefRegex matches Garmin's placeholder format for "quoting" a
// media message that has no text caption. The native apps emit something
// like "\u2009📷(816CA11B-FD69-49AD-A849-2AE7121E215F)" — an optional thin
// space, an icon (camera/microphone/etc.), and the media UUID in
// parentheses. The captured group is the UUID.
var reactionMediaRefRegex = regexp.MustCompile(`\(([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\)$`)

// extractMediaRefUUID returns the UUID from a Garmin media-reference target
// text, or zero+false if the text isn't a media reference. The format is
// reliably "<optional thin space><icon emoji>(<uuid>)" — the icon varies
// per media type, so we anchor the parse on the trailing "(<uuid>)" only.
func extractMediaRefUUID(target string) (uuid.UUID, bool) {
	m := reactionMediaRefRegex.FindStringSubmatch(target)
	if m == nil {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(m[1])
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// matchReactionMediaTarget scans `messages` for the media message that a
// reaction targets, given the UUID extracted from the reaction's
// media-reference target text. Garmin embeds either the message's UUID or
// MediaID field in the reference (it varies, so we accept both). When
// multiple candidates match, the one closest in time wins — same
// tie-breaker as the text-matching path.
func matchReactionMediaTarget(messages []gm.ConversationMessageModel, reactionMsg gm.MessageModel, mediaUUID uuid.UUID) *gm.ConversationMessageModel {
	reactionTime := derefTime(reactionMsg.SentAt)
	if reactionTime.IsZero() {
		reactionTime = derefTime(reactionMsg.ReceivedAt)
	}

	var best *gm.ConversationMessageModel
	bestDiff := time.Duration(math.MaxInt64)

	for i := range messages {
		c := &messages[i]
		if c.MessageID == reactionMsg.MessageID {
			continue
		}
		match := false
		if c.MediaID != nil && *c.MediaID == mediaUUID {
			match = true
		} else if c.UUID != nil && *c.UUID == mediaUUID {
			match = true
		}
		if !match {
			continue
		}
		candTime := derefTime(c.SentAt)
		if candTime.IsZero() {
			candTime = derefTime(c.ReceivedAt)
		}
		diff := reactionTime.Sub(candTime)
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			best = c
		}
	}
	return best
}

// matchReactionTarget scans `messages` for the message that a reaction most
// likely targets, using the same algorithm as the gm-webclient frontend:
//
//   - Skip the reaction itself and any other reaction messages
//   - Strip ZWS-family characters from candidate bodies before comparing
//   - Accept exact match OR startsWith match (for iOS-truncated quotes)
//   - Among multiple matches, pick the one with the smallest time distance
//     from the reaction's sentAt
//
// Returns nil if no candidate matches. This is the fallback used when the
// Garmin server does not populate parentMessageId on the reaction message
// (which, in practice, is almost always).
func matchReactionTarget(messages []gm.ConversationMessageModel, reactionMsg gm.MessageModel, targetText string) *gm.ConversationMessageModel {
	if targetText == "" {
		return nil
	}
	reactionTime := derefTime(reactionMsg.SentAt)
	if reactionTime.IsZero() {
		reactionTime = derefTime(reactionMsg.ReceivedAt)
	}

	var best *gm.ConversationMessageModel
	bestDiff := time.Duration(math.MaxInt64)

	for i := range messages {
		c := &messages[i]
		if c.MessageID == reactionMsg.MessageID {
			continue
		}
		body := derefStr(c.MessageBody)
		if body == "" || isReactionBody(body) {
			continue
		}
		stripped := strings.TrimSpace(zwsStripper.Replace(body))
		if stripped != targetText && !strings.HasPrefix(stripped, targetText) {
			continue
		}
		candTime := derefTime(c.SentAt)
		if candTime.IsZero() {
			candTime = derefTime(c.ReceivedAt)
		}
		diff := reactionTime.Sub(candTime)
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			best = c
		}
	}
	return best
}

// resolveReactionParentFromMessages determines which message a reaction
// targets, given a pre-fetched slice of recent messages from the same
// conversation. No API calls are made.
//
// Garmin never populates parentMessageId on reaction messages — neither in
// SignalR pushes nor in REST conversation detail responses — so the only
// reliable resolution path is content matching the quoted body against the
// recent history. Two flavours:
//
//   - Text reactions (the common case): match the candidate body text after
//     stripping ZWS-family characters, mirroring gm-webclient.
//   - Media-reference reactions (caption-less images/audio): the target text
//     is a placeholder like "📷(<media-uuid>)" — match the UUID against
//     candidate messages' MediaID or UUID field instead.
func resolveReactionParentFromMessages(msg gm.MessageModel, messages []gm.ConversationMessageModel) (networkid.MessageID, error) {
	_, targetText, ok := extractReactionTarget(derefStr(msg.MessageBody))
	if !ok {
		return "", fmt.Errorf("reaction %s has malformed body", msg.MessageID)
	}
	if targetText == "" {
		return "", fmt.Errorf("reaction %s has empty target text", msg.MessageID)
	}

	// Media reactions: target text is a "<icon>(<uuid>)" placeholder.
	if mediaUUID, isMediaRef := extractMediaRefUUID(targetText); isMediaRef {
		if target := matchReactionMediaTarget(messages, msg, mediaUUID); target != nil {
			return networkid.MessageID(target.MessageID.String()), nil
		}
		return "", fmt.Errorf("no media message matched reaction %s media UUID %s", msg.MessageID, mediaUUID)
	}

	target := matchReactionTarget(messages, msg, targetText)
	if target == nil {
		return "", fmt.Errorf("no message matched reaction %s target text %q", msg.MessageID, targetText)
	}
	return networkid.MessageID(target.MessageID.String()), nil
}

// resolveReactionParentID is the live (SignalR-push) variant of reaction
// resolution: it issues exactly one GetConversationDetail call to fetch
// recent messages, then runs the text-matching algorithm against them.
//
// During catch-up sync the caller already has the detail in hand and should
// call resolveReactionParentFromMessages directly to avoid the API call.
func (c *GarminClient) resolveReactionParentID(ctx context.Context, msg gm.MessageModel) (networkid.MessageID, error) {
	detail, err := c.api.GetConversationDetail(ctx, msg.ConversationID, gm.WithDetailLimit(100))
	if err != nil {
		return "", fmt.Errorf("reaction parent lookup failed: %w", err)
	}
	parentID, err := resolveReactionParentFromMessages(msg, detail.Messages)
	if err != nil {
		return "", err
	}
	c.log.Debug().
		Str("reaction_id", msg.MessageID.String()).
		Str("matched_parent_id", string(parentID)).
		Msg("Resolved reaction parent via text matching")
	return parentID, nil
}

// resolveReactionOriginalBody fetches the message body of the target message for
// use when constructing an outgoing reaction from Matrix to Garmin.
func (c *GarminClient) resolveReactionOriginalBody(ctx context.Context, conversationID uuid.UUID, targetMsgID uuid.UUID) (string, error) {
	detail, err := c.api.GetConversationDetail(ctx, conversationID, gm.WithDetailLimit(100))
	if err != nil {
		return "", fmt.Errorf("reaction target lookup failed: %w", err)
	}
	for _, m := range detail.Messages {
		if m.MessageID == targetMsgID {
			return derefStr(m.MessageBody), nil
		}
	}
	return "", fmt.Errorf("target message %s not found in conversation", targetMsgID)
}

// derefFloat64 safely dereferences a *float64.
func derefFloat64(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}
