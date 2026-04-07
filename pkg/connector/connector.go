// Package connector implements the mautrix bridgev2 network connector for
// Garmin Messenger, using the vendored Hermes API client (internal/hermes).
package connector

import (
	"context"
	_ "embed"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	gm "github.com/yourusername/matrix-garmin-messenger/internal/hermes"
	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// GarminConnector is the top-level NetworkConnector for the bridge.
// One instance is shared across the whole bridge process.
var _ bridgev2.NetworkConnector = (*GarminConnector)(nil)

type GarminConnector struct {
	br      *bridgev2.Bridge
	Config  Config
}

// Config holds network-specific config fields (in the network: section).
type Config struct {
	// SessionsDir is the base directory where per-user HermesAuth sessions
	// are stored. Each login gets a subdirectory named after its login ID.
	// Defaults to <bridge data dir>/sessions.
	SessionsDir string `yaml:"sessions_dir"`
}

//go:embed example-config.yaml
var ExampleConfig string

func upgradeConfig(helper configupgrade.Helper) {
	helper.Copy(configupgrade.Str, "sessions_dir")
}

// sessionDir returns the directory for a specific login's credentials.
func (gc *GarminConnector) sessionDir(loginID networkid.UserLoginID) string {
	base := gc.Config.SessionsDir
	if base == "" {
		base = "sessions"
	}
	return filepath.Join(base, string(loginID))
}

func (gc *GarminConnector) Init(br *bridgev2.Bridge) {
	gc.br = br
}

func (gc *GarminConnector) Start(_ context.Context) error {
	return nil
}

func (gc *GarminConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (gc *GarminConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

func (gc *GarminConnector) GetName() bridgev2.BridgeName {
	// Use the bot's configured avatar as the network/space icon so the Matrix
	// space room reflects the same avatar as set in config.yaml under appservice.bot.avatar.
	// The framework updates the space room avatar on every restart if it changed.
	var networkIcon id.ContentURIString
	if gc.br != nil {
		if mc, ok := gc.br.Matrix.(*matrix.Connector); ok {
			avatar := mc.Config.AppService.Bot.Avatar
			if avatar != "" && avatar != "remove" {
				networkIcon = id.ContentURIString(avatar)
			}
		}
	}
	return bridgev2.BridgeName{
		DisplayName:      "Garmin Messenger",
		NetworkURL:       "https://explore.garmin.com/en-US/inreach/",
		NetworkIcon:      networkIcon,
		NetworkID:        "garmin-messenger",
		BeeperBridgeType: "github.com/yourusername/matrix-garmin-messenger",
		DefaultPort:      29340,
	}
}

func (gc *GarminConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	return ExampleConfig, &gc.Config, configupgrade.SimpleUpgrader(upgradeConfig)
}

func (gc *GarminConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		UserLogin: func() any { return &UserLoginMetadata{} },
		Portal:    func() any { return &PortalMetadata{} },
		Ghost:     nil,
		Message:   nil,
		Reaction:  nil,
	}
}

// LoadUserLogin prepares an existing login for connection.
// It creates a HermesAuth using the stored session directory, then
// calls Resume() to reload saved credentials from disk.
func (gc *GarminConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta := login.Metadata.(*UserLoginMetadata)
	sessDir := gc.sessionDir(login.ID)

	auth := gm.NewHermesAuth(gm.WithSessionDir(sessDir))

	// Resume loads credentials from hermes_credentials.json in sessDir.
	// If the file doesn't exist yet (first run after migration), Connect()
	// will handle the error via BridgeState.
	if err := auth.Resume(ctx); err != nil {
		// Log but don't fail — Connect() will report the state properly.
		login.Log.Warn().Err(err).Msg("Could not resume Garmin session, will reconnect on next Connect()")
	}

	login.Client = newGarminClient(gc, login, auth, meta.PhoneNumber)
	return nil
}

func (gc *GarminConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		Name:        "SMS one-time password",
		Description: "Log in with your phone number registered in the Garmin Messenger app",
		ID:          "sms-otp",
	}}
}

func (gc *GarminConnector) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != "sms-otp" {
		return nil, fmt.Errorf("unknown login flow ID: %q", flowID)
	}
	return &GarminLogin{connector: gc, user: user}, nil
}

// --- DB metadata types ---

// UserLoginMetadata stores the phone number and a reference to the
// session directory. The actual credentials are stored on disk by HermesAuth.
//
// LastSyncTime is the most recent time at which the bridge knows it had
// successfully received Garmin events. It is used to drive catch-up sync
// after a SignalR reconnect or a bridge restart, by passing it as the
// AfterDate filter to GetConversations and GetUpdatedStatuses.
type UserLoginMetadata struct {
	PhoneNumber  string     `json:"phoneNumber"`
	LastSyncTime *time.Time `json:"lastSyncTime,omitempty"`
}

// PortalMetadata stores Garmin-specific data for a bridged conversation.
// RecipientPhones is needed for sending: the Garmin API sends by phone number,
// not by conversation ID.
type PortalMetadata struct {
	RecipientPhones []string `json:"recipientPhones"`
}

// --- Identifier helpers ---

// portalIDFromConversation creates a stable PortalID from a Garmin conversation ID.
func portalIDFromConversation(convID string) networkid.PortalID {
	return networkid.PortalID(convID)
}

// ghostIDFromHermesID uses the Hermes UUID-v5 as the ghost user ID.
// Use gm.PhoneToHermesUserID(phone) to derive it from a phone number.
// The UUID is normalized to lowercase to ensure consistent ghost IDs
// regardless of the case returned by the Garmin API vs PhoneToHermesUserID.
func ghostIDFromHermesID(hermesUUID string) networkid.UserID {
	return networkid.UserID(strings.ToLower(hermesUUID))
}

// loginIDFromPhone uses the phone number as the login ID.
func loginIDFromPhone(phone string) networkid.UserLoginID {
	return networkid.UserLoginID(phone)
}
