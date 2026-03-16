#!/usr/bin/env bash
# Deploy Clone-Xs as a Databricks App.
# Prerequisites: Databricks CLI configured, npm installed.
# Usage: ./scripts/deploy-databricks-app.sh [app-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="${1:-clone-xs}"

# Step 1: Build frontend
echo "Building frontend..."
cd "$ROOT/ui" && npm ci --silent && npm run build
echo "Frontend built to ui/dist/"

# Step 2: Verify build output
if [[ ! -d "$ROOT/ui/dist" ]]; then
    echo "ERROR: Frontend build failed — ui/dist/ not found"
    exit 1
fi

# Step 3: Deploy to Databricks
echo "Deploying Databricks App: $APP_NAME"
cd "$ROOT"
databricks apps deploy "$APP_NAME" --source-code-path "$ROOT"

echo ""
echo "Deployed! Open your Databricks workspace to access the app."
