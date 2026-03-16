#!/usr/bin/env bash
# Build Clone-Xs Desktop App
# Usage:
#   ./scripts/build-desktop.sh              # Build for current platform
#   ./scripts/build-desktop.sh --mac        # macOS only
#   ./scripts/build-desktop.sh --win        # Windows only
#   ./scripts/build-desktop.sh --skip-frontend  # Skip frontend build

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SKIP_FRONTEND=false
TARGET=""

for arg in "$@"; do
  case $arg in
    --mac) TARGET="mac" ;;
    --win) TARGET="win" ;;
    --skip-frontend) SKIP_FRONTEND=true ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

echo "============================================"
echo "  Clone-Xs Desktop Build"
echo "============================================"
echo ""

# --- Step 1: Build frontend ---
if [ "$SKIP_FRONTEND" = false ]; then
  echo "[1/3] Building frontend..."
  cd "$ROOT/ui"
  npm install --silent
  npm run build
  echo "  Frontend built -> ui/dist/"
  echo ""
else
  echo "[1/3] Skipping frontend build (--skip-frontend)"
  echo ""
fi

# --- Step 2: Install desktop dependencies ---
echo "[2/3] Installing desktop dependencies..."
cd "$ROOT/desktop"
npm install --silent
echo "  Dependencies installed."
echo ""

# --- Step 3: Build Electron app ---
echo "[3/3] Building Electron app..."
cd "$ROOT/desktop"

if [ "$TARGET" = "win" ] && [ "$(uname)" = "Darwin" ]; then
  echo "  Cross-compiling for Windows from macOS..."
  echo "  (Install Wine for NSIS: brew install --cask wine-stable)"
  npx electron-builder --win
elif [ -n "$TARGET" ]; then
  npx electron-builder --$TARGET
else
  npx electron-builder
fi

echo ""
echo "============================================"
echo "  Build complete!"
echo "  Output: desktop/dist/"
echo "============================================"

# List output
ls -la "$ROOT/desktop/dist/" 2>/dev/null || true
