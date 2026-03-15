#!/usr/bin/env bash
# ============================================================
# Clone-Xs Documentation Site — Start Script
# ============================================================
# Starts the Docusaurus documentation site on port 3001
#
# Usage:
#   ./scripts/start_docs.sh
#   make docs-start
# ============================================================

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
BOLD='\033[1m'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS_DIR="$ROOT/docs"
DOCS_PORT=3001

# ── Check docs directory exists ──────────────────────────────
if [[ ! -d "$DOCS_DIR" ]]; then
    echo -e "${RED}Error: docs/ directory not found at $DOCS_DIR${NC}"
    exit 1
fi

echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  Clone-Xs Documentation Site${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""

# ── Kill stale process on docs port ──────────────────────────
pid=$(lsof -ti:$DOCS_PORT 2>/dev/null || true)
if [[ -n "$pid" ]]; then
    echo -e "${YELLOW}Killing stale process on port $DOCS_PORT (PID $pid)${NC}"
    kill -9 $pid 2>/dev/null || true
    sleep 1
fi

# ── Install dependencies if needed ──────────────────────────
if [[ ! -d "$DOCS_DIR/node_modules" ]]; then
    echo -e "${BLUE}Installing documentation dependencies...${NC}"
    cd "$DOCS_DIR" && npm install
    echo -e "${GREEN}Dependencies installed${NC}"
fi

echo ""

# ── Start Docusaurus ─────────────────────────────────────────
echo -e "${BLUE}Starting Docusaurus (port $DOCS_PORT)...${NC}"
echo ""
cd "$DOCS_DIR"
npx docusaurus start --port $DOCS_PORT --no-open &
DOCS_PID=$!

# Wait for docs site to be ready
for i in {1..20}; do
    if curl -s http://localhost:$DOCS_PORT >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo ""
echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  Documentation site is running!${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""
echo -e "  ${GREEN}Docs:  ${BOLD}http://localhost:$DOCS_PORT${NC}"
echo ""
echo -e "  ${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Keep running until interrupted
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down docs server...${NC}"
    kill $DOCS_PID 2>/dev/null
    wait 2>/dev/null
    echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT INT TERM

wait
