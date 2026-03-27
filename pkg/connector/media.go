package connector

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	gm "github.com/palchrb/garmin-messenger-go"
)

// ToGarminAVIF converts an image to AVIF for sending via Garmin Messenger.
// The iOS app's debug menu reports: resolution 1920, quality 20/100, speed 6, YUV444.
// The app's quality scale appears to be 0=worst/100=best, so 20 is low quality.
// We default to CRF 30 (out of 63, lower=better) for visibly better results than
// a strict quality-20 match, while still keeping file sizes reasonable.
// cpu-used 6 balances encoding speed vs compression (app uses speed=6 on 0–10 scale).
func ToGarminAVIF(ctx context.Context, src []byte, srcMime string) ([]byte, error) {
	srcFormat, err := mimeToFFmpegFormat(srcMime)
	if err != nil {
		return nil, err
	}
	if _, lookupErr := exec.LookPath("ffmpeg"); lookupErr != nil {
		return nil, fmt.Errorf("ffmpeg not found: %w", lookupErr)
	}
	// AVIF muxer requires seekable output — write to a temp file.
	tmpOut, err := os.CreateTemp("", "garmin-avif-*.avif")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpOut.Name()
	tmpOut.Close()
	defer os.Remove(tmpPath)

	// Scale so the longest side is at most 1920px, keeping aspect ratio.
	// CRF 50 matches quality=20/100 on libavif/libaom-av1.
	// yuv444p matches the app's YUV444 pixel format setting.
	// cpu-used 6 for fast encoding (app uses speed=6).
	var errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", srcFormat, "-i", "pipe:0",
		"-vf", "scale='min(1920,iw)':'min(1920,ih)':force_original_aspect_ratio=decrease:flags=lanczos",
		"-c:v", "libaom-av1",
		"-crf", "30", "-b:v", "0",
		"-cpu-used", "6",
		"-pix_fmt", "yuv444p",
		"-y", tmpPath,
	)
	cmd.Stdin = bytes.NewReader(src)
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("image→garmin-avif: %w\n%s", err, errBuf.String())
	}
	return os.ReadFile(tmpPath)
}

// ToGarminOGG converts audio to OGG Opus matching the Garmin Messenger iOS
// voice message format: Opus codec, 48000 Hz input sample rate, mono, max 30 seconds.
// Verified by inspecting actual files stored by the Garmin Messenger Android app:
//   - iOS-recorded messages: Ogg Opus, mono, 48000 Hz
//   - 16 kbps is standard Opus voice quality
func ToGarminOGG(ctx context.Context, src []byte, srcMime string) ([]byte, error) {
	srcFormat, err := mimeToFFmpegFormat(srcMime)
	if err != nil {
		return nil, err
	}
	if _, lookupErr := exec.LookPath("ffmpeg"); lookupErr != nil {
		return nil, fmt.Errorf("ffmpeg not found: %w", lookupErr)
	}
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", srcFormat, "-i", "pipe:0",
		"-t", "30", // max 30 seconds
		"-ar", "48000", // 48000 Hz — Opus native sample rate (matches iOS Garmin Messenger)
		"-ac", "1", // mono
		"-c:a", "libopus",
		"-b:a", "16k", // 16 kbps — standard Opus voice quality
		"-f", "ogg", "pipe:1",
	)
	cmd.Stdin = bytes.NewReader(src)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("audio→garmin-ogg: %w\n%s", err, errBuf.String())
	}
	return out.Bytes(), nil
}

// mimeToFFmpegFormat maps common MIME types to ffmpeg demuxer names.
func mimeToFFmpegFormat(mime string) (string, error) {
	switch mime {
	case "image/jpeg", "image/jpg":
		return "mjpeg", nil
	case "image/png":
		return "png_pipe", nil
	case "image/webp":
		return "webp_pipe", nil
	case "image/avif":
		return "avif", nil
	case "audio/ogg", "audio/ogg; codecs=vorbis", "audio/ogg; codecs=opus":
		return "ogg", nil
	case "audio/mpeg", "audio/mp3":
		return "mp3", nil
	case "audio/mp4", "audio/m4a", "audio/aac":
		return "aac", nil
	case "audio/wav", "audio/wave":
		return "wav", nil
	case "audio/webm":
		return "webm", nil
	default:
		return "", fmt.Errorf("unsupported media type: %s", mime)
	}
}

// GarminMediaType returns the Garmin MediaType enum for a given source MIME type,
// or the zero value if the type is not supported.
func GarminMediaType(mime string) gm.MediaType {
	switch {
	case isImageMIME(mime):
		return gm.MediaTypeImageAvif
	case isAudioMIME(mime):
		return gm.MediaTypeAudioOgg
	default:
		return ""
	}
}

// isImageMIME reports whether the MIME type is a supported image format.
func isImageMIME(mime string) bool {
	switch mime {
	case "image/jpeg", "image/jpg", "image/png", "image/webp", "image/avif":
		return true
	}
	return false
}

// isAudioMIME reports whether the MIME type is a supported audio format.
func isAudioMIME(mime string) bool {
	switch mime {
	case "audio/ogg", "audio/ogg; codecs=vorbis", "audio/ogg; codecs=opus",
		"audio/mpeg", "audio/mp3",
		"audio/mp4", "audio/m4a", "audio/aac",
		"audio/wav", "audio/wave",
		"audio/webm":
		return true
	}
	return false
}

// ProbeAudioDurationMS returns the duration of an audio file in milliseconds
// by running ffprobe on the data. Returns 0 on any error (non-fatal).
func ProbeAudioDurationMS(ctx context.Context, data []byte, srcFormat string) int {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-f", srcFormat,
		"-show_entries", "format=duration",
		"-of", "csv=p=0",
		"pipe:0",
	)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(out))
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int(f * 1000)
}
