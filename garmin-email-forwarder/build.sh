#!/bin/sh
# Build garmin-email-forwarder for all supported platforms.
set -e

GIT_TAG=$(git describe --exact-match --tags 2>/dev/null || true)
GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(date -Iseconds)
VERSION="${GIT_TAG:-dev}"

LDFLAGS="-s -w \
  -X main.Version=${VERSION} \
  -X main.Commit=${GIT_COMMIT} \
  -X 'main.BuildTime=${BUILD_TIME}'"

echo "Building garmin-email-forwarder ${VERSION} (${GIT_COMMIT})"
echo ""

build() {
  GOOS=$1 GOARCH=$2
  EXT=""
  [ "$GOOS" = "windows" ] && EXT=".exe"
  OUT="dist/garmin-email-forwarder_${GOOS}_${GOARCH}${EXT}"
  echo "  ${GOOS}/${GOARCH} → ${OUT}"
  CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH \
    go build -ldflags="${LDFLAGS}" -o "${OUT}" ./cmd/garmin-email-forwarder
}

mkdir -p dist

build linux  amd64
build linux  arm64
build windows amd64
build windows arm64
build darwin  amd64
build darwin  arm64

echo ""
echo "Done. Binaries in ./dist/"
ls -lh dist/
