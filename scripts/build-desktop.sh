#!/usr/bin/env bash
# Build Clone-Xs Desktop App
# Usage:
#   ./scripts/build-desktop.sh              # Build for current platform
#   ./scripts/build-desktop.sh --mac        # macOS only
#   ./scripts/build-desktop.sh --win        # Windows only
#   ./scripts/build-desktop.sh --linux      # Linux only
#   ./scripts/build-desktop.sh --all        # All platforms
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
    --linux) TARGET="linux" ;;
    --all) TARGET="all" ;;
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
  echo "[1/4] Building frontend..."
  cd "$ROOT/ui"
  npm install --silent
  npm run build
  echo "  Frontend built -> ui/dist/"
  echo ""
else
  echo "[1/4] Skipping frontend build (--skip-frontend)"
  echo ""
fi

# --- Step 2: Install desktop dependencies ---
echo "[2/4] Installing desktop dependencies..."
cd "$ROOT/desktop"
npm install --silent
echo "  Dependencies installed."
echo ""

# --- Step 3: Generate icons ---
echo "[3/4] Generating app icons..."
if [ ! -f "$ROOT/desktop/build/icon.png" ]; then
  node "$ROOT/desktop/scripts/generate-icons.js"
else
  echo "  Icons already exist, skipping."
fi
echo ""

# --- Step 4: Build Electron app ---
echo "[4/4] Building Electron app..."
cd "$ROOT/desktop"

if [ "$TARGET" = "all" ]; then
  echo "  Building for all platforms..."
  npx electron-builder --mac --win --linux
elif [ "$TARGET" = "win" ] && [ "$(uname)" = "Darwin" ]; then
  echo "  Cross-compiling for Windows from macOS..."
  echo "  Note: Requires Wine (brew install --cask wine-stable) for NSIS installer."
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
