#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p bin
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -tags production -ldflags='-s -w -H windowsgui' -o bin/p2pfs.exe .
printf '%s\n' 'Built bin/p2pfs.exe. Windows/WebView2 runtime acceptance is a separate check.'
