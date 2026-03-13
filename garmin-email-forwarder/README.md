# garmin-email-forwarder

Forward incoming [Garmin Messenger](https://explore.garmin.com/en-US/inreach/) messages to email automatically.

Runs as a standalone binary or Docker container on Linux, Windows, and macOS (amd64 + arm64).

## Features

- Forwards text messages, media (images, audio), and location data to one or more email addresses
- **Caption routing** — include email addresses in a media caption to route that message to specific recipients
- Converts AVIF images → JPEG and OGG audio → MP3 for email compatibility (requires ffmpeg, optional)
- Supports SMTP with STARTTLS (port 587), SSL (port 465), or plain (port 25)

## Quick start

### Binary

```sh
# 1. Create a config file
garmin-email-forwarder init

# 2. Edit config.yaml with your phone number and SMTP settings
# 3. Log in (sends an SMS to your phone)
garmin-email-forwarder login

# 4. Start forwarding
garmin-email-forwarder run
```

### Docker

```sh
mkdir data
# Place your config.yaml in ./data/

# Log in interactively
docker compose run --rm garmin-email-forwarder login -config /data/config.yaml

# Start the forwarder
docker compose up -d
```

## Commands

| Command | Description |
|---|---|
| `init` | Write an example `config.yaml` |
| `login` | Authenticate via SMS OTP |
| `logout` | Remove saved session |
| `run` | Start the forwarder (default) |
| `status` | Check Garmin session + SMTP connectivity |
| `test-smtp` | Send a test email |
| `version` | Print version info |

All commands accept `-config <path>` to specify the config file (default: `config.yaml`).

## Configuration

```yaml
garmin:
  phone: "+4712345678"
  session_dir: "./sessions"

smtp:
  host: "smtp.gmail.com"
  port: 587
  username: "you@gmail.com"
  password: "your-app-password"
  from: "Garmin Forwarder <you@gmail.com>"
  tls: "starttls"   # none | starttls | ssl

forwarding:
  default_recipients:
    - "you@example.com"
  caption_routing: true
  caption_routing_replaces_default: false
  forward_text: true
  forward_media: true
  forward_location: true

log:
  level: "info"
  pretty: true
```

### Caption routing

When `caption_routing: true`, email addresses found in a media caption are used as additional recipients.

Example: send a photo from your Garmin device with caption `"check this alice@example.com"` — the message will be forwarded to both `alice@example.com` and any `default_recipients`.

Set `caption_routing_replaces_default: true` to send **only** to the caption addresses.

## Building from source

```sh
go build ./cmd/garmin-email-forwarder

# All platforms
./build.sh
```

## Docker image

Published to `ghcr.io/palchrb/garmin-email-forwarder` for `linux/amd64` and `linux/arm64`.

## Session storage

Authentication credentials are stored in `sessions/hermes_credentials.json`. This file contains a JWT access token and refresh token for the Garmin Messenger API. Keep it secure and do not commit it to version control.
