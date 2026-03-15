#!/usr/bin/env bash
# ============================================================
# Clone-Xs Web App — Start Script
# ============================================================
# Starts both FastAPI backend (port 8000) and Vite React frontend (port 3000)
#
# Usage:
#   ./scripts/start_web.sh
#   make web-start
# ============================================================

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
BOLD='\033[1m'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
API_PID=""
UI_PID=""

cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down...${NC}"
    [[ -n "$API_PID" ]] && kill "$API_PID" 2>/dev/null
    [[ -n "$UI_PID" ]] && kill "$UI_PID" 2>/dev/null
    wait 2>/dev/null
    echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT INT TERM

echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  Clone-Xs Web Application${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""

# ── Kill stale processes on our ports ─────────────────────────
for port in 8000 3000; do
    pid=$(lsof -ti:$port 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
        echo -e "${YELLOW}Killing stale process on port $port (PID $pid)${NC}"
        kill -9 $pid 2>/dev/null || true
        sleep 1
    fi
done

# Clean stale lock files
rm -f "$ROOT/ui/.next/dev/lock" 2>/dev/null

# ── Check dependencies ───────────────────────────────────────
echo -e "${BLUE}Checking dependencies...${NC}"

if ! python3 -c "import fastapi" 2>/dev/null; then
    echo -e "${YELLOW}Installing FastAPI...${NC}"
    pip install fastapi uvicorn websockets python-multipart 2>&1 | tail -1
fi

if [[ ! -d "$ROOT/ui/node_modules" ]]; then
    echo -e "${YELLOW}Installing UI dependencies...${NC}"
    cd "$ROOT/ui" && npm install 2>&1 | tail -1
    cd "$ROOT"
fi

# ── Build & install clone-xs package ──────────────────────────
echo -e "${BLUE}Building clone-xs package...${NC}"
cd "$ROOT"
rm -rf build/ dist/ clone_xs.egg-info
PYTHON="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
PIP="/Library/Frameworks/Python.framework/Versions/3.13/bin/pip"
# Fall back to system python if framework python not found
if [[ ! -x "$PYTHON" ]]; then
    PYTHON="python3"
    PIP="pip3"
fi
$PYTHON -m build --wheel --outdir dist/ 2>&1 | tail -1
$PIP install --force-reinstall --no-deps dist/clone_xs-*.whl 2>&1 | tail -1
echo -e "${GREEN}Package installed${NC}"

echo -e "${GREEN}Dependencies OK${NC}"
echo ""

# ── Start FastAPI backend ────────────────────────────────────
echo -e "${BLUE}Starting API server (port 8000)...${NC}"
cd "$ROOT"
uvicorn api.main:app --reload --port 8000 --log-level info 2>&1 | sed 's/^/  [API] /' &
API_PID=$!

# Wait for API to be ready
for i in {1..10}; do
    if curl -s http://localhost:8000/api/health >/dev/null 2>&1; then
        echo -e "  ${GREEN}API ready at http://localhost:8000${NC}"
        echo -e "  ${GREEN}API docs at http://localhost:8000/docs${NC}"
        break
    fi
    sleep 1
done

echo ""

# ── Start Vite React frontend ────────────────────────────────
echo -e "${BLUE}Starting UI (port 3000)...${NC}"
cd "$ROOT/ui"
npm run dev 2>&1 | sed 's/^/  [UI]  /' &
UI_PID=$!

# Wait for UI to be ready
for i in {1..15}; do
    if curl -s http://localhost:3000 >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo ""
echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  Clone-Xs Web App is running!${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""
echo -e "  ${GREEN}UI:       ${BOLD}http://localhost:3000${NC}"
echo -e "  ${GREEN}API:      http://localhost:8000${NC}"
echo -e "  ${GREEN}API Docs: http://localhost:8000/docs${NC}"
echo ""
echo -e "  ${YELLOW}Press Ctrl+C to stop both servers${NC}"
echo ""

# Keep running until interrupted
wait
