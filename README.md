# matrix-garmin-messenger

A [Matrix](https://matrix.org) bridge for **Garmin Messenger** and **InReach satellite devices**, built on [mautrix-go bridgev2](https://mau.fi/blog/megabridge-twilio/).

Uses the [slush-dev/garmin-messenger](https://github.com/slush-dev/garmin-messenger) Go library for all Garmin API communication.

Send and receive messages between Matrix and any Garmin InReach device (Mini 2, Messenger, GPSMAP 67i, etc.) — no Garmin hardware required on your side.

## Features

- **Real-time messages** via SignalR WebSocket (`HermesSignalR`)
- **Send text messages** to Garmin conversations
- **GPS location bridging** — InReach location pings become `m.location` events
- **Read receipt propagation** via SignalR
- **Multi-account support**
- **Token auto-refresh** handled by the library
- **Auto-reconnect** with backoff (handled by `HermesSignalR`)
- **Full bridgev2 support** — encryption, double-puppeting, `start-chat` command

## Requirements

- A phone number registered with the [Garmin Messenger app](https://explore.garmin.com/en-US/inreach/) (free)
- A Matrix homeserver with appservice support (Synapse, Conduit, Dendrite)
- Go 1.24+

## Building

```bash
git clone https://github.com/yourusername/matrix-garmin-messenger
cd matrix-garmin-messenger

# Fetch the garmin-messenger library
go get github.com/slush-dev/garmin-messenger@main
go mod tidy

chmod +x build.sh
./build.sh
```

## Setup

### 1. Generate example config
```bash
./matrix-garmin-messenger -e
# Saved to config.yaml
```

### 2. Edit config.yaml

```yaml
homeserver:
  address: https://your.homeserver.tld
  domain: your.homeserver.tld

appservice:
  address: http://localhost:29340
  hostname: 0.0.0.0
  port: 29340

bridge:
  permissions:
    "@you:your.homeserver.tld": admin

network:
  # Where per-user Garmin sessions are stored (hermes_credentials.json per login).
  sessions_dir: /data/sessions
```

### 3. Generate appservice registration
```bash
./matrix-garmin-messenger -g
# Saved to registration.yaml
```

### 4. Register with homeserver (Synapse example)
Add to `homeserver.yaml`:
```yaml
app_service_config_files:
  - /path/to/registration.yaml
```
Restart Synapse.

### 5. Run the bridge
```bash
./matrix-garmin-messenger
```

### 6. Log in via bridge bot

Start a chat with `@garminbot:your.homeserver.tld` and send:
```
login
```
The bot asks for your phone number, sends you an SMS code, and you're connected.

## Docker

```bash
docker run -v /path/to/data:/data ghcr.io/yourusername/matrix-garmin-messenger:latest
```

First run generates `config.yaml`. Edit it, run again for `registration.yaml`, register with your homeserver, then run normally.

```yaml
# docker-compose.yaml
services:
  matrix-garmin-messenger:
    image: ghcr.io/yourusername/matrix-garmin-messenger:latest
    volumes:
      - ./data:/data
    restart: unless-stopped
```

The `/data/sessions/` directory contains HermesAuth credential files. **Include this in backups.**

## Architecture

```
Matrix homeserver
      │  (appservice API)
      ▼
matrix-garmin-messenger (this bridge)
      │
      ├── HermesAPI (REST) ──────────► https://hermes.inreachapp.com
      │   GetConversations / Members       (sync, send messages)
      │   SendMessage
      │
      └── HermesSignalR (WebSocket) ─► Azure SignalR service
                                         OnMessage → incoming messages
                                         OnStatusUpdate → read receipts
                                         MarkAsDelivered / MarkAsRead
```

## Session storage

The `slush-dev/garmin-messenger` library stores credentials in:
```
<sessions_dir>/<phone_number>/hermes_credentials.json
```

These are automatically refreshed when the access token expires. No manual credential management needed.

## Protocol

This bridge uses the [Garmin Messenger (Hermes) API](https://github.com/slush-dev/garmin-messenger/blob/main/docs/api-reference.md) via the `slush-dev/garmin-messenger` library, which handles:
- SMS OTP authentication
- Token refresh
- Azure SignalR negotiate/redirect
- REST API calls

## License

MIT
