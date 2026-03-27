package connector

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	gm "github.com/palchrb/garmin-messenger-go"
	"maunium.net/go/mautrix/bridgev2"
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
	bodyText := body
	if msg.UserLocation != nil {
		lat := derefFloat64(msg.UserLocation.LatitudeDegrees)
		lon := derefFloat64(msg.UserLocation.LongitudeDegrees)
		if bodyText == "" {
			// Keep this as plain text to avoid large map previews in Matrix clients.
			bodyText = fmt.Sprintf("📍 %.6f, %.6f", lat, lon)
		} else {
			bodyText += fmt.Sprintf("\n\n📍 Location: %.6f, %.6f", lat, lon)
		}
		if alt := derefFloat64(msg.UserLocation.ElevationMeters); alt != 0 {
			bodyText += fmt.Sprintf(" (%.0fm)", alt)
		}
		// GroundVelocityMetersPerSecond * 3.6 = km/h
		if spd := derefFloat64(msg.UserLocation.GroundVelocityMetersPerSecond); spd != 0 {
			bodyText += fmt.Sprintf(", %.1f km/h", spd*3.6)
		}
	}

	// --- Media attachment (AVIF image or OGG audio from Garmin) ---
	// Download from Garmin, transcode if needed, reupload to Matrix.
	// Note: process media even when there's also a location/text body.
	if msg.MediaID != nil {
		mediaPart, err := c.bridgeIncomingMedia(ctx, intent, msg)
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
				parts = append(parts, &bridgev2.ConvertedMessagePart{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType: event.MsgText,
						Body:    bodyText + fmt.Sprintf("\n\n[Media attachment (%s) — could not be downloaded]", mediaTypeStr),
					},
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
			if bodyText != "" {
				// Save the filename before overwriting body with the caption.
				mediaPart.Content.FileName = mediaPart.Content.Body
				mediaPart.Content.Body = bodyText
			}
			parts = append(parts, mediaPart)
		}
	}

	// --- Text fallback when there's no media attachment ---
	if len(parts) == 0 && bodyText != "" {
		parts = append(parts, &bridgev2.ConvertedMessagePart{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    bodyText,
			},
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
//
// Garmin sends AVIF images and OGG audio.
// Keep AVIF as-is (no conversion) and keep OGG as-is.
func (c *GarminClient) bridgeIncomingMedia(
	ctx context.Context,
	intent bridgev2.MatrixAPI,
	msg gm.MessageModel,
) (*bridgev2.ConvertedMessagePart, error) {
	if msg.MediaID == nil {
		return nil, fmt.Errorf("message has no media ID")
	}
	if msg.MediaType == nil {
		return nil, fmt.Errorf("message has no media type")
	}

	msgUUID, err := c.resolveMediaMessageUUID(ctx, msg)
	if err != nil {
		return nil, err
	}

	mediaID := *msg.MediaID
	mediaType := *msg.MediaType

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
		return nil, fmt.Errorf("DownloadMedia: %w", err)
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
		return nil, fmt.Errorf("unknown Garmin media type: %s", mediaType)
	}

	// Upload to Matrix media repository via the ghost user's intent.
	mxcURI, encryptedFile, err := intent.UploadMedia(ctx, "", uploadData, filename, mimeType)
	if err != nil {
		return nil, fmt.Errorf("UploadMedia: %w", err)
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
	}, nil
}

// resolveMediaMessageUUID finds the UUID required by Hermes media download.
//
// SignalR events may omit MessageModel.UUID for some incoming messages. In that
// case, fetch recent conversation details and find the matching message entry.
// As a final fallback, use MessageID as UUID (best-effort).
func (c *GarminClient) resolveMediaMessageUUID(ctx context.Context, msg gm.MessageModel) (uuid.UUID, error) {
	if msg.UUID != nil {
		return *msg.UUID, nil
	}

	detail, err := c.api.GetConversationDetail(ctx, msg.ConversationID, gm.WithDetailLimit(100))
	if err != nil {
		return uuid.Nil, fmt.Errorf("message has no UUID and lookup failed: %w", err)
	}

	for _, m := range detail.Messages {
		if m.MessageID != msg.MessageID {
			continue
		}
		if m.UUID != nil {
			return *m.UUID, nil
		}
		break
	}

	c.log.Warn().
		Str("msg_id", msg.MessageID.String()).
		Str("conversation_id", msg.ConversationID.String()).
		Msg("Media message UUID not present in detail response, falling back to MessageID")
	return msg.MessageID, nil
}

// derefFloat64 safely dereferences a *float64.
func derefFloat64(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}
