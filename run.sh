#!/bin/sh
# Compile and run Robust-AI-Proxy without a build tool.
# Configuration is via environment variables (see README.md) or -D properties.
set -e
cd "$(dirname "$0")"
OUT=out
mkdir -p "$OUT"
javac -d "$OUT" $(find src -name '*.java')
exec java -cp "$OUT" com.robust.aiproxy.Main "$@"
