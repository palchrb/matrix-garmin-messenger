package connector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gm "github.com/palchrb/garmin-messenger-go"
)

func TestGarminMediaType(t *testing.T) {
	tests := []struct {
		name string
		mime string
		want gm.MediaType
	}{
		{name: "jpeg", mime: "image/jpeg", want: gm.MediaTypeImageAvif},
		{name: "png", mime: "image/png", want: gm.MediaTypeImageAvif},
		{name: "avif", mime: "image/avif", want: gm.MediaTypeImageAvif},
		{name: "ogg", mime: "audio/ogg", want: gm.MediaTypeAudioOgg},
		{name: "mp3", mime: "audio/mp3", want: gm.MediaTypeAudioOgg},
		{name: "unsupported", mime: "video/mp4", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, GarminMediaType(tc.mime))
		})
	}
}
