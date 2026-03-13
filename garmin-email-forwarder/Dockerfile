FROM golang:1-alpine3.23 AS builder

RUN apk add --no-cache git ca-certificates build-base

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN GIT_TAG=$(git describe --exact-match --tags 2>/dev/null || true) \
    GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown") \
    BUILD_TIME=$(date -Iseconds) \
    go build \
      -ldflags="-s -w -X main.Version=${GIT_TAG:-dev} -X main.Commit=${GIT_COMMIT} -X 'main.BuildTime=${BUILD_TIME}'" \
      -o garmin-email-forwarder \
      ./cmd/garmin-email-forwarder

FROM alpine:3.23

RUN apk add --no-cache ca-certificates ffmpeg tzdata

# Run as non-root user
RUN addgroup -S garmin && adduser -S -G garmin garmin

WORKDIR /data

COPY --from=builder /build/garmin-email-forwarder /usr/local/bin/garmin-email-forwarder

USER garmin

VOLUME /data

ENTRYPOINT ["garmin-email-forwarder"]
CMD ["run", "-config", "/data/config.yaml"]
