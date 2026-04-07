package connector

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	gm "github.com/yourusername/matrix-garmin-messenger/internal/hermes"
)

func TestExtractReactionTarget(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantEmoji  string
		wantTarget string
		wantOK     bool
	}{
		{
			name:       "simple english to",
			body:       "\u200b👍\u200b to \u200aHello world\u200a",
			wantEmoji:  "👍",
			wantTarget: "Hello world",
			wantOK:     true,
		},
		{
			name:       "norwegian connector word",
			body:       "\u200b❤️\u200b til \u200aGod morgen\u200a",
			wantEmoji:  "❤️",
			wantTarget: "God morgen",
			wantOK:     true,
		},
		{
			name:       "ios guillemets",
			body:       "\u200b😂\u200b to «\u200aFunny message\u200a»",
			wantEmoji:  "😂",
			wantTarget: "Funny message",
			wantOK:     true,
		},
		{
			name:       "trailing ellipsis unicode",
			body:       "\u200b👀\u200b to \u200aThis is a really long mes\u2026\u200a",
			wantEmoji:  "👀",
			wantTarget: "This is a really long mes",
			wantOK:     true,
		},
		{
			name:       "trailing ellipsis ascii",
			body:       "\u200b🎉\u200b to \u200aPartytime...\u200a",
			wantEmoji:  "🎉",
			wantTarget: "Partytime",
			wantOK:     true,
		},
		{
			name:   "not a reaction",
			body:   "Hello there",
			wantOK: false,
		},
		{
			name:   "starts with zws but malformed",
			body:   "\u200bjust some text",
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			emoji, target, ok := extractReactionTarget(tc.body)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.wantEmoji, emoji)
				assert.Equal(t, tc.wantTarget, target)
			}
		})
	}
}

func TestMatchReactionTarget(t *testing.T) {
	mkMsg := func(body string, secondsAgo int) gm.ConversationMessageModel {
		t := time.Now().Add(-time.Duration(secondsAgo) * time.Second)
		b := body
		return gm.ConversationMessageModel{
			MessageID:   uuid.New(),
			MessageBody: &b,
			SentAt:      &t,
		}
	}

	t.Run("exact match", func(t *testing.T) {
		messages := []gm.ConversationMessageModel{
			mkMsg("Other message", 60),
			mkMsg("Hello world", 30),
			mkMsg("Yet another", 10),
		}
		reactTime := time.Now().Add(-5 * time.Second)
		reaction := gm.MessageModel{
			MessageID: uuid.New(),
			SentAt:    &reactTime,
		}
		got := matchReactionTarget(messages, reaction, "Hello world")
		assert.NotNil(t, got)
		assert.Equal(t, messages[1].MessageID, got.MessageID)
	})

	t.Run("ios truncated startsWith", func(t *testing.T) {
		messages := []gm.ConversationMessageModel{
			mkMsg("This is a really long message that gets truncated by iOS", 30),
		}
		reactTime := time.Now()
		reaction := gm.MessageModel{MessageID: uuid.New(), SentAt: &reactTime}
		got := matchReactionTarget(messages, reaction, "This is a really long mes")
		assert.NotNil(t, got)
		assert.Equal(t, messages[0].MessageID, got.MessageID)
	})

	t.Run("picks closest in time when multiple match", func(t *testing.T) {
		messages := []gm.ConversationMessageModel{
			mkMsg("Same body", 600), // 10 min ago
			mkMsg("Same body", 30),  // 30 sec ago — should win
			mkMsg("Same body", 1200),
		}
		reactTime := time.Now().Add(-5 * time.Second)
		reaction := gm.MessageModel{MessageID: uuid.New(), SentAt: &reactTime}
		got := matchReactionTarget(messages, reaction, "Same body")
		assert.NotNil(t, got)
		assert.Equal(t, messages[1].MessageID, got.MessageID)
	})

	t.Run("skips other reactions", func(t *testing.T) {
		reactionBody := "\u200b👍\u200b to \u200aHello\u200a"
		messages := []gm.ConversationMessageModel{
			mkMsg(reactionBody, 30),
			mkMsg("Hello", 60),
		}
		reactTime := time.Now()
		reaction := gm.MessageModel{MessageID: uuid.New(), SentAt: &reactTime}
		got := matchReactionTarget(messages, reaction, "Hello")
		assert.NotNil(t, got)
		assert.Equal(t, messages[1].MessageID, got.MessageID)
	})

	t.Run("strips ZWS chars from candidate body", func(t *testing.T) {
		// Defensive: a candidate body with stray ZWS should still match
		messages := []gm.ConversationMessageModel{
			mkMsg("Hel\u200blo", 30),
		}
		reactTime := time.Now()
		reaction := gm.MessageModel{MessageID: uuid.New(), SentAt: &reactTime}
		got := matchReactionTarget(messages, reaction, "Hello")
		assert.NotNil(t, got)
	})

	t.Run("no match returns nil", func(t *testing.T) {
		messages := []gm.ConversationMessageModel{
			mkMsg("Something else", 30),
		}
		reactTime := time.Now()
		reaction := gm.MessageModel{MessageID: uuid.New(), SentAt: &reactTime}
		got := matchReactionTarget(messages, reaction, "Nope")
		assert.Nil(t, got)
	})

	t.Run("empty target text returns nil", func(t *testing.T) {
		messages := []gm.ConversationMessageModel{
			mkMsg("anything", 30),
		}
		reactTime := time.Now()
		reaction := gm.MessageModel{MessageID: uuid.New(), SentAt: &reactTime}
		got := matchReactionTarget(messages, reaction, "")
		assert.Nil(t, got)
	})
}

func TestRecentMessagesCache(t *testing.T) {
	c := &GarminClient{}
	convID := uuid.New()

	mk := func(body string) gm.ConversationMessageModel {
		b := body
		return gm.ConversationMessageModel{
			MessageID:   uuid.New(),
			MessageBody: &b,
		}
	}

	t.Run("cacheRecentMessage stores and snapshot returns copy", func(t *testing.T) {
		c.recentByConv = nil // reset
		m := mk("hello")
		c.cacheRecentMessage(convID, m)
		snap := c.snapshotRecentMessages(convID)
		assert.Len(t, snap, 1)
		assert.Equal(t, m.MessageID, snap[0].MessageID)
		// Mutating the snapshot must not affect the cache.
		snap[0] = gm.ConversationMessageModel{}
		snap2 := c.snapshotRecentMessages(convID)
		assert.Equal(t, m.MessageID, snap2[0].MessageID)
	})

	t.Run("skips reaction bodies", func(t *testing.T) {
		c.recentByConv = nil
		c.cacheRecentMessage(convID, mk("\u200b👍\u200b to \u200aHello\u200a"))
		assert.Len(t, c.snapshotRecentMessages(convID), 0)
	})

	t.Run("dedupes by message ID", func(t *testing.T) {
		c.recentByConv = nil
		m := mk("hi")
		c.cacheRecentMessage(convID, m)
		c.cacheRecentMessage(convID, m)
		assert.Len(t, c.snapshotRecentMessages(convID), 1)
	})

	t.Run("ring buffer evicts oldest", func(t *testing.T) {
		c.recentByConv = nil
		for i := 0; i < recentMessagesPerConv+5; i++ {
			c.cacheRecentMessage(convID, mk("msg"))
		}
		snap := c.snapshotRecentMessages(convID)
		assert.Len(t, snap, recentMessagesPerConv)
	})

	t.Run("bulkCacheRecentMessages merges", func(t *testing.T) {
		c.recentByConv = nil
		batch := []gm.ConversationMessageModel{mk("a"), mk("b"), mk("c")}
		c.bulkCacheRecentMessages(convID, batch)
		assert.Len(t, c.snapshotRecentMessages(convID), 3)
		// Re-bulking the same batch is idempotent.
		c.bulkCacheRecentMessages(convID, batch)
		assert.Len(t, c.snapshotRecentMessages(convID), 3)
	})

	t.Run("snapshot of unknown conv returns nil", func(t *testing.T) {
		c.recentByConv = nil
		assert.Nil(t, c.snapshotRecentMessages(uuid.New()))
	})
}
