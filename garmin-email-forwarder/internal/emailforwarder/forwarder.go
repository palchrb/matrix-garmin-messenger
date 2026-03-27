package emailforwarder

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	gm "github.com/palchrb/garmin-messenger-go"
)

var emailRegexp = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)

// Forwarder listens for incoming Garmin messages and forwards them via email.
type Forwarder struct {
	cfg    *Config
	auth   *gm.HermesAuth
	api    *gm.HermesAPI
	sr     *gm.HermesSignalR
	mailer *Mailer
	log    *slog.Logger
}

// New creates a new Forwarder. Call Run() to start it.
func New(cfg *Config, log *slog.Logger) (*Forwarder, error) {
	sessDir := cfg.Garmin.SessionDir
	if sessDir == "" {
		sessDir = "./sessions"
	}

	auth := gm.NewHermesAuth(
		gm.WithSessionDir(sessDir),
		gm.WithLogger(log),
	)

	api := gm.NewHermesAPI(auth, gm.WithAPILogger(log))
	sr := gm.NewHermesSignalR(auth, gm.WithSignalRLogger(log))
	mailer := NewMailer(cfg.SMTP)

	return &Forwarder{
		cfg:    cfg,
		auth:   auth,
		api:    api,
		sr:     sr,
		mailer: mailer,
		log:    log,
	}, nil
}

// Resume loads the saved Garmin session from disk.
func (f *Forwarder) Resume(ctx context.Context) error {
	return f.auth.Resume(ctx)
}

// Run starts the SignalR listener and blocks until ctx is cancelled.
func (f *Forwarder) Run(ctx context.Context) error {
	// Validate session with a lightweight API call.
	if _, err := f.api.GetConversations(ctx, gm.WithLimit(1)); err != nil {
		return fmt.Errorf("connecting to Garmin Messenger: %w\n\nRun 'garmin-email-forwarder login' to authenticate", err)
	}

	f.log.Info("Connected to Garmin Messenger", "phone", f.cfg.Garmin.Phone)

	f.sr.OnMessage(func(msg gm.MessageModel) {
		f.handleMessage(msg)
	})

	f.sr.OnOpen(func() {
		f.log.Info("SignalR connected")
	})

	f.sr.OnClose(func() {
		f.log.Warn("SignalR disconnected — will reconnect automatically")
	})

	f.sr.OnError(func(err error) {
		f.log.Error("SignalR error", "error", err)
	})

	f.log.Info("Listening for Garmin messages", "default_recipients", f.cfg.Forwarding.DefaultRecipients)

	if err := f.sr.Start(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("SignalR: %w", err)
	}
	return nil
}

// Stop shuts down the SignalR connection.
func (f *Forwarder) Stop() {
	f.sr.Stop()
	f.api.Close()
}

// handleMessage processes an incoming Garmin message and forwards it by email.
func (f *Forwarder) handleMessage(msg gm.MessageModel) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	f.log.Info("Received Garmin message",
		"msg_id", msg.MessageID,
		"conversation_id", msg.ConversationID,
		"from", derefStr(msg.From),
	)

	// Skip reaction messages — not meaningful as email.
	if msg.MessageType.IsReaction() {
		f.log.Debug("Skipping reaction message")
		return
	}

	body := f.buildEmailBody(msg)
	caption := derefStr(msg.MessageBody)

	// Determine recipients.
	recipients := f.resolveRecipients(caption)
	if len(recipients) == 0 {
		f.log.Warn("No recipients configured — dropping message")
		return
	}

	emailMsg := Message{
		To:      recipients,
		Subject: f.buildSubject(msg),
		Body:    body,
	}

	// Attach media if present.
	if msg.MediaID != nil && f.cfg.Forwarding.ForwardMedia {
		att, err := f.downloadMedia(ctx, msg)
		if err != nil {
			f.log.Error("Failed to download media", "error", err)
			emailMsg.Body += "\n\n[Media attachment could not be downloaded: " + err.Error() + "]"
		} else {
			emailMsg.Attachments = append(emailMsg.Attachments, *att)
		}
	}

	if err := f.mailer.Send(emailMsg); err != nil {
		f.log.Error("Failed to send email", "error", err, "recipients", recipients)
		return
	}

	f.log.Info("Email forwarded", "recipients", recipients, "subject", emailMsg.Subject)

	// Mark message as delivered.
	f.sr.MarkAsDelivered(msg.ConversationID, msg.MessageID)
}

// buildSubject constructs the email subject line.
func (f *Forwarder) buildSubject(msg gm.MessageModel) string {
	from := derefStr(msg.From)
	if from == "" {
		from = "unknown"
	}

	ts := time.Now()
	if msg.SentAt != nil {
		ts = *msg.SentAt
	}

	deviceTag := ""
	if msg.FromDeviceType != nil && *msg.FromDeviceType == gm.DeviceTypeInReach {
		deviceTag = " [InReach]"
	}

	return fmt.Sprintf("Garmin Message from %s%s — %s",
		from, deviceTag, ts.Format("2006-01-02 15:04 MST"))
}

// buildEmailBody constructs the plain-text email body from a message.
func (f *Forwarder) buildEmailBody(msg gm.MessageModel) string {
	var sb strings.Builder

	from := derefStr(msg.From)
	ts := time.Now()
	if msg.SentAt != nil {
		ts = *msg.SentAt
	}

	sb.WriteString("═══════════════════════════════════════\n")
	sb.WriteString("  GARMIN MESSENGER\n")
	sb.WriteString("═══════════════════════════════════════\n\n")

	sb.WriteString(fmt.Sprintf("From:    %s\n", from))
	sb.WriteString(fmt.Sprintf("Time:    %s\n", ts.Format("2006-01-02 15:04:05 MST")))

	if msg.FromDeviceType != nil {
		sb.WriteString(fmt.Sprintf("Device:  %s\n", *msg.FromDeviceType))
	}

	sb.WriteString("\n")

	// Text body
	if body := derefStr(msg.MessageBody); body != "" && f.cfg.Forwarding.ForwardText {
		sb.WriteString("Message:\n")
		sb.WriteString("  " + strings.ReplaceAll(body, "\n", "\n  ") + "\n\n")
	}

	// Location
	if msg.UserLocation != nil && f.cfg.Forwarding.ForwardLocation {
		loc := msg.UserLocation
		lat := derefFloat64(loc.LatitudeDegrees)
		lon := derefFloat64(loc.LongitudeDegrees)

		sb.WriteString("Location:\n")
		sb.WriteString(fmt.Sprintf("  Coordinates: %.6f, %.6f\n", lat, lon))
		sb.WriteString(fmt.Sprintf("  Maps: https://maps.google.com/?q=%.6f,%.6f\n", lat, lon))

		if alt := derefFloat64(loc.ElevationMeters); alt != 0 {
			sb.WriteString(fmt.Sprintf("  Elevation:   %.0f m\n", alt))
		}
		if spd := derefFloat64(loc.GroundVelocityMetersPerSecond); spd != 0 {
			sb.WriteString(fmt.Sprintf("  Speed:       %.1f km/h\n", spd*3.6))
		}
		sb.WriteString("\n")
	}

	// MapShare / LiveTrack links
	if url := derefStr(msg.MapShareUrl); url != "" {
		sb.WriteString(fmt.Sprintf("MapShare: %s\n", url))
		if pw := derefStr(msg.MapSharePassword); pw != "" {
			sb.WriteString(fmt.Sprintf("Password: %s\n", pw))
		}
		sb.WriteString("\n")
	}
	if url := derefStr(msg.LiveTrackUrl); url != "" {
		sb.WriteString(fmt.Sprintf("LiveTrack: %s\n\n", url))
	}

	// Audio transcription
	if t := derefStr(msg.Transcription); t != "" {
		sb.WriteString("Transcription:\n")
		sb.WriteString("  " + t + "\n\n")
	}

	sb.WriteString("───────────────────────────────────────\n")
	sb.WriteString(fmt.Sprintf("Message ID:      %s\n", msg.MessageID))
	sb.WriteString(fmt.Sprintf("Conversation ID: %s\n", msg.ConversationID))
	sb.WriteString("Forwarded by garmin-email-forwarder\n")

	return sb.String()
}

// resolveRecipients returns the list of email recipients for a message.
// If caption_routing is enabled, email addresses are parsed from the caption.
func (f *Forwarder) resolveRecipients(caption string) []string {
	captionEmails := f.cfg.Forwarding.CaptionRouting && caption != ""
	var parsed []string
	if captionEmails {
		parsed = emailRegexp.FindAllString(caption, -1)
	}

	if len(parsed) > 0 && f.cfg.Forwarding.CaptionRoutingReplacesDefault {
		return unique(parsed)
	}

	all := append([]string{}, f.cfg.Forwarding.DefaultRecipients...)
	all = append(all, parsed...)
	return unique(all)
}

// downloadMedia fetches the media attachment from Garmin and returns an Attachment.
// If ffmpeg is available, AVIF images are converted to JPEG and OGG audio to MP3
// for better email client compatibility. Falls back to the original format otherwise.
func (f *Forwarder) downloadMedia(ctx context.Context, msg gm.MessageModel) (*Attachment, error) {
	if msg.MediaID == nil || msg.MediaType == nil {
		return nil, fmt.Errorf("message has no media")
	}

	// Resolve message UUID required by the download API.
	msgUUID, err := f.resolveMediaUUID(ctx, msg)
	if err != nil {
		return nil, err
	}

	data, err := f.api.DownloadMedia(ctx, msgUUID, *msg.MediaID, msg.MessageID, msg.ConversationID, *msg.MediaType)
	if err != nil {
		return nil, fmt.Errorf("downloading media: %w", err)
	}

	switch *msg.MediaType {
	case gm.MediaTypeImageAvif:
		if jpeg, err := transcodeAVIFtoJPEG(ctx, data); err == nil {
			return &Attachment{
				Filename:    "image.jpg",
				ContentType: "image/jpeg",
				Data:        jpeg,
			}, nil
		}
		// ffmpeg not available — attach as-is
		return &Attachment{
			Filename:    "image.avif",
			ContentType: "image/avif",
			Data:        data,
		}, nil

	case gm.MediaTypeAudioOgg:
		if mp3, err := transcodeOGGtoMP3(ctx, data); err == nil {
			return &Attachment{
				Filename:    "audio.mp3",
				ContentType: "audio/mpeg",
				Data:        mp3,
			}, nil
		}
		// ffmpeg not available — attach as-is
		return &Attachment{
			Filename:    "audio.ogg",
			ContentType: "audio/ogg",
			Data:        data,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported media type: %s", *msg.MediaType)
	}
}

// resolveMediaUUID finds the UUID required by the Garmin media download API.
func (f *Forwarder) resolveMediaUUID(ctx context.Context, msg gm.MessageModel) (uuid.UUID, error) {
	if msg.UUID != nil {
		return *msg.UUID, nil
	}
	detail, err := f.api.GetConversationDetail(ctx, msg.ConversationID, gm.WithDetailLimit(100))
	if err != nil {
		return uuid.Nil, fmt.Errorf("lookup failed: %w", err)
	}
	for _, m := range detail.Messages {
		if m.MessageID == msg.MessageID && m.UUID != nil {
			return *m.UUID, nil
		}
	}
	// Fall back to MessageID as UUID (best-effort).
	return msg.MessageID, nil
}

// transcodeAVIFtoJPEG converts AVIF image data to JPEG using ffmpeg.
// Returns an error if ffmpeg is not available.
func transcodeAVIFtoJPEG(ctx context.Context, data []byte) ([]byte, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, fmt.Errorf("ffmpeg not found")
	}
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", "avif", "-i", "pipe:0",
		"-f", "mjpeg", "-q:v", "3",
		"pipe:1",
	)
	cmd.Stdin = bytes.NewReader(data)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg: %w: %s", err, errBuf.String())
	}
	return out.Bytes(), nil
}

// transcodeOGGtoMP3 converts OGG audio data to MP3 using ffmpeg.
// Returns an error if ffmpeg is not available.
func transcodeOGGtoMP3(ctx context.Context, data []byte) ([]byte, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, fmt.Errorf("ffmpeg not found")
	}
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", "ogg", "-i", "pipe:0",
		"-f", "mp3", "-b:a", "128k",
		"pipe:1",
	)
	cmd.Stdin = bytes.NewReader(data)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg: %w: %s", err, errBuf.String())
	}
	return out.Bytes(), nil
}

// unique returns a deduplicated copy of the slice.
func unique(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefFloat64(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}
