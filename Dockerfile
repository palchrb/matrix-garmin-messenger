# Pin the Go major explicitly. Must be >= 1.27.3 / 1.26.10 for the crypto/x509
# block of the compromised eMudhra CA keys (CVE-2026-111943); the floating
# 1.27 tag picks up later patch releases on every rebuild.
FROM golang:1.27-alpine3.23 AS builder

RUN apk add --no-cache git ca-certificates build-base su-exec olm-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Re-apply replace in case go.mod was overwritten by COPY . .
RUN go mod edit -replace github.com/slush-dev/garmin-messenger=/garmin-messenger/lib/go

# Robust fix:
#  - Remove possible CRLF line endings
#  - Ensure executable bit
#  - Execute build script
RUN sed -i 's/\r$//' ./build.sh \
 && chmod +x ./build.sh \
 && ./build.sh

FROM alpine:3.23

ENV UID=1337 \
    GID=1337

# apk upgrade pulls the newest ca-certificates (and other security fixes) even
# when the alpine:3.23 base layer in the build cache is older.
RUN apk upgrade --no-cache \
 && apk add --no-cache su-exec ca-certificates olm bash yq-go ffmpeg

COPY --from=builder /build/matrix-garmin-messenger /usr/bin/matrix-garmin-messenger
COPY --from=builder /build/docker-run.sh /docker-run.sh

RUN sed -i 's/\r$//' /docker-run.sh \
 && chmod +x /docker-run.sh

VOLUME /data

CMD ["/docker-run.sh"]
