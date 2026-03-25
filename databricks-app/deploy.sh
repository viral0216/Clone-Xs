#!/usr/bin/env bash
# ============================================================
# Deploy Clone-Xs as a Databricks App
# ============================================================
#
# Build flow:
#   1. Frontend is built locally (npm run build → ui/dist/)
#   2. A staging directory is assembled with only the needed files
#   3. Files are uploaded to /Workspace/apps/<app-name>
#   4. App is created (if new) and deployed
#   5. On app startup: pip install + uvicorn (defined in app.yaml)
#
# Prerequisites:
#   - Databricks CLI v0.200+ configured (databricks auth login)
#   - Node.js / npm installed (for frontend build)
#
# Usage:
#   ./databricks-app/deploy.sh [app-name]
#
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DBX_APP_DIR="$ROOT/databricks-app"
APP_NAME="${1:-clone-xs}"
WORKSPACE_PATH="/Workspace/apps/${APP_NAME}"

echo "============================================================"
echo "  Clone-Xs Databricks App Deployment"
echo "============================================================"
echo ""
echo "  App name:       ${APP_NAME}"
echo "  Workspace path: ${WORKSPACE_PATH}"
echo ""

# ----------------------------------------------------------
# Step 1: Build frontend
# ----------------------------------------------------------
echo "==> Step 1: Building frontend..."
cd "$ROOT/ui" && npm ci --silent && npm run build
echo "    Built to ui/dist/"

if [[ ! -f "$ROOT/ui/dist/index.html" ]]; then
    echo "ERROR: Frontend build failed — ui/dist/index.html not found"
    exit 1
fi

# ----------------------------------------------------------
# Step 2: Assemble staging directory
# ----------------------------------------------------------
echo ""
echo "==> Step 2: Assembling deployment package..."
STAGING=$(mktemp -d)
trap "rm -rf $STAGING" EXIT

# Python source
cp -r "$ROOT/src" "$STAGING/src"
cp -r "$ROOT/api" "$STAGING/api"

# Frontend build
mkdir -p "$STAGING/ui"
cp -r "$ROOT/ui/dist" "$STAGING/ui/dist"

# Configuration
cp -r "$ROOT/config" "$STAGING/config"

# App manifest
cp "$DBX_APP_DIR/app.yaml" "$STAGING/app.yaml"

# Python packaging
cp "$ROOT/pyproject.toml" "$STAGING/pyproject.toml"
[[ -f "$ROOT/requirements.txt" ]] && cp "$ROOT/requirements.txt" "$STAGING/requirements.txt"
[[ -f "$ROOT/LICENSE" ]] && cp "$ROOT/LICENSE" "$STAGING/LICENSE"
[[ -f "$ROOT/README.md" ]] && cp "$ROOT/README.md" "$STAGING/README.md"

# Count files
FILE_COUNT=$(find "$STAGING" -type f | wc -l | tr -d ' ')
echo "    Staged ${FILE_COUNT} files"
echo ""
echo "    Package contents:"
echo "      src/           $(find "$STAGING/src" -name '*.py' | wc -l | tr -d ' ') Python modules"
echo "      api/           FastAPI backend"
echo "      ui/dist/       Pre-built frontend"
echo "      config/        Clone configuration"
echo "      app.yaml       Databricks App manifest"
echo "      pyproject.toml Python dependencies"

# ----------------------------------------------------------
# Step 3: Upload to workspace
# ----------------------------------------------------------
echo ""
echo "==> Step 3: Uploading to ${WORKSPACE_PATH}..."
databricks workspace import-dir "$STAGING" "$WORKSPACE_PATH" --overwrite
echo "    Upload complete."

# ----------------------------------------------------------
# Step 4: Create app if it doesn't exist
# ----------------------------------------------------------
echo ""
echo "==> Step 4: Checking app status..."

if ! databricks apps get "$APP_NAME" &>/dev/null; then
    echo "    Creating new app: ${APP_NAME}..."
    databricks apps create "$APP_NAME" \
        --description "Clone-Xs: Enterprise Unity Catalog cloning toolkit"
    echo "    App created. Waiting for compute to initialize..."
    sleep 10
fi

# Wait for compute to be ready
for i in $(seq 1 60); do
    COMPUTE_STATE=$(databricks apps get "$APP_NAME" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))" 2>/dev/null \
        || echo "UNKNOWN")

    if [[ "$COMPUTE_STATE" == "ACTIVE" ]]; then
        echo "    App compute is ACTIVE."
        break
    elif [[ "$COMPUTE_STATE" == "STOPPED" ]]; then
        echo "    Starting app compute..."
        databricks apps start "$APP_NAME" --no-wait 2>/dev/null || true
    fi

    if [[ $i -eq 60 ]]; then
        echo ""
        echo "WARNING: Compute still '${COMPUTE_STATE}' after 5 minutes."
        echo "  Start manually:  databricks apps start ${APP_NAME}"
        echo "  Then re-deploy:  databricks apps deploy ${APP_NAME} --source-code-path ${WORKSPACE_PATH}"
        exit 1
    fi

    echo "    Waiting... (${COMPUTE_STATE}, ${i}/60)"
    sleep 5
done

# ----------------------------------------------------------
# Step 5: Deploy
# ----------------------------------------------------------
echo ""
echo "==> Step 5: Deploying..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$WORKSPACE_PATH"

# Get app URL
APP_URL=$(databricks apps get "$APP_NAME" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null \
    || echo "")

echo ""
echo "============================================================"
echo "  Deployment Complete!"
echo "============================================================"
echo ""
echo "  App:  ${APP_NAME}"
[[ -n "$APP_URL" ]] && echo "  URL:  ${APP_URL}"
echo ""
echo "  The app installs Python dependencies and starts"
echo "  uvicorn automatically. Authentication is handled"
echo "  via the app's service principal — no PAT needed."
echo ""
echo "  Useful commands:"
echo "    databricks apps get ${APP_NAME}        # Check status"
echo "    databricks apps get-logs ${APP_NAME}    # View logs"
echo "    databricks apps stop ${APP_NAME}        # Stop app"
echo "    databricks apps start ${APP_NAME}       # Start app"
echo "============================================================"
