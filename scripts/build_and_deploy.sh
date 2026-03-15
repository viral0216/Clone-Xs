#!/usr/bin/env bash
#
# Build wheel and deploy to local env and/or Databricks Volume.
#
# Usage:
#   ./scripts/build_and_deploy.sh                   # Build + install locally
#   ./scripts/build_and_deploy.sh --upload           # Build + install locally + upload to Databricks Volume
#   ./scripts/build_and_deploy.sh --upload-only      # Build + upload only (skip local install)
#
# Environment variables (for Databricks upload):
#   VOLUME_PATH  — Unity Catalog Volume path (default: /Volumes/shared/packages/wheels)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$PROJECT_ROOT/dist"
VOLUME_PATH="${VOLUME_PATH:-/Volumes/shared/packages/wheels}"

# Parse args
UPLOAD=false
UPLOAD_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --upload)      UPLOAD=true ;;
        --upload-only) UPLOAD=true; UPLOAD_ONLY=true ;;
        --help|-h)
            echo "Usage: $0 [--upload] [--upload-only]"
            echo ""
            echo "  --upload       Build + install locally + upload wheel to Databricks Volume"
            echo "  --upload-only  Build + upload to Databricks Volume (skip local install)"
            echo ""
            echo "Environment variables:"
            echo "  VOLUME_PATH    Databricks Volume path (default: /Volumes/shared/packages/wheels)"
            exit 0
            ;;
    esac
done

echo "============================================"
echo "  clone-xs — Build & Deploy"
echo "============================================"

# -------------------------------------------------------------------
# Step 1: Clean previous builds
# -------------------------------------------------------------------
echo ""
echo "[1/5] Cleaning previous builds..."
rm -rf "$DIST_DIR" "$PROJECT_ROOT/build" "$PROJECT_ROOT"/*.egg-info "$PROJECT_ROOT"/src/*.egg-info
echo "      Done."

# -------------------------------------------------------------------
# Step 2: Run tests
# -------------------------------------------------------------------
echo ""
echo "[2/5] Running tests..."
cd "$PROJECT_ROOT"
python3 -m pytest tests/ -q --tb=short
echo "      All tests passed."

# -------------------------------------------------------------------
# Step 3: Build wheel
# -------------------------------------------------------------------
echo ""
echo "[3/5] Building wheel..."
python3 -m build --wheel --outdir "$DIST_DIR"
WHEEL_FILE=$(ls "$DIST_DIR"/*.whl 2>/dev/null | head -1)

if [ -z "$WHEEL_FILE" ]; then
    echo "ERROR: No wheel file found in $DIST_DIR"
    exit 1
fi

WHEEL_NAME=$(basename "$WHEEL_FILE")
echo "      Built: $WHEEL_NAME"

# -------------------------------------------------------------------
# Step 4: Install locally
# -------------------------------------------------------------------
if [ "$UPLOAD_ONLY" = false ]; then
    echo ""
    echo "[4/5] Installing locally..."
    pip install --force-reinstall --no-deps "$WHEEL_FILE" 2>&1 | tail -1
    echo "      Installed (system Python)."

    # Also install in .venv if it exists
    VENV_PIP="$PROJECT_ROOT/.venv/bin/pip"
    if [ -x "$VENV_PIP" ]; then
        echo "      Installing in .venv..."
        "$VENV_PIP" install --force-reinstall --no-deps "$WHEEL_FILE" 2>&1 | tail -1
        echo "      Installed (.venv)."
    fi

    # Also install in system Python framework if it exists (macOS)
    SYS_PIP="/Library/Frameworks/Python.framework/Versions/3.13/bin/pip"
    if [ -x "$SYS_PIP" ]; then
        echo "      Installing in system Python..."
        "$SYS_PIP" install --force-reinstall --no-deps "$WHEEL_FILE" 2>&1 | tail -1
        echo "      Installed (system Python)."
    fi

    # Clear all __pycache__ to avoid stale bytecode
    find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

    echo "      Verify: clone-catalog --help"
else
    echo ""
    echo "[4/5] Skipping local install (--upload-only)"
fi

# -------------------------------------------------------------------
# Step 5: Upload to Databricks Volume
# -------------------------------------------------------------------
if [ "$UPLOAD" = true ]; then
    echo ""
    echo "[5/5] Uploading to Databricks Volume..."
    echo "      Target: $VOLUME_PATH/$WHEEL_NAME"

    # Check databricks CLI is available
    if ! command -v databricks &> /dev/null; then
        echo "ERROR: 'databricks' CLI not found. Install: pip install databricks-cli"
        exit 1
    fi

    databricks fs cp "$WHEEL_FILE" "dbfs:$VOLUME_PATH/$WHEEL_NAME" --overwrite
    echo "      Uploaded successfully."
    echo ""
    echo "      In a Databricks notebook, install with:"
    echo "      %pip install $VOLUME_PATH/$WHEEL_NAME"
else
    echo ""
    echo "[5/5] Skipping Databricks upload (use --upload to enable)"
fi

# -------------------------------------------------------------------
# Summary
# -------------------------------------------------------------------
echo ""
echo "============================================"
echo "  Build complete!"
echo "============================================"
echo "  Wheel:  $DIST_DIR/$WHEEL_NAME"

if [ "$UPLOAD_ONLY" = false ]; then
    echo "  Local:  installed (clone-catalog CLI ready)"
fi

if [ "$UPLOAD" = true ]; then
    echo "  Remote: $VOLUME_PATH/$WHEEL_NAME"
fi

echo ""
echo "  Notebook usage (wheel):"
echo "    %pip install $VOLUME_PATH/$WHEEL_NAME"
echo "    from src.catalog_clone_api import clone_full_catalog"
echo ""
echo "  Notebook usage (repo):"
echo "    import sys; sys.path.insert(0, '/Workspace/Repos/<user>/clone-xs')"
echo "    from src.catalog_clone_api import clone_full_catalog"
echo "============================================"
