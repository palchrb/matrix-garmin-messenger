package main

import (
	"github.com/palchrb/matrix-garmin-messenger/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
)

// Version variables are set at build time by the linker flags in build.sh.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {
	m := mxmain.BridgeMain{
		Name:        "matrix-garmin-messenger",
		Description: "A Matrix bridge for Garmin Messenger / InReach satellite devices",
		URL:         "https://github.com/palchrb/matrix-garmin-messenger",
		Version:     "0.1.0",
		Connector:   &connector.GarminConnector{},
	}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
