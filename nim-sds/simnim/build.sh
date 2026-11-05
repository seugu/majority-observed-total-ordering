#!/bin/sh
# Build finality_sim against this (patched) nim-sds checkout.
# Needs: nim >= 2.2 on PATH, git, python3. Run from anywhere.
set -e
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DEPS="$ROOT/../deps"
cd "$ROOT"

command -v nim >/dev/null || { echo "nim not found (brew install nim / choosenim 2.2.6)"; exit 1; }
nim --version | head -1

# Locked dependencies at the exact commits in nimble.lock (skips nimble's own nim download).
if [ ! -f nimble.paths ]; then
  python3 simnim/fetch_deps.py nimble.lock "$DEPS"
  sed 's/^--path:\(.*\)$/switch("path", \1)/' "$DEPS/paths.cfg" > nimble.paths
fi

nim c -d:release -d:chronicles_enabled=off --hints:off --warnings:off \
  --outdir:build simnim/finality_sim.nim
echo "built: $ROOT/build/finality_sim"
