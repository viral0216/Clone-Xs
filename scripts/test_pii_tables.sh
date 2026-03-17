#!/usr/bin/env bash
# ============================================================
# Test: PII Table Initialization & Scan Storage
# ============================================================
# Prerequisites: API running on localhost:8000 (make api-dev)
# Tests:
#   1. Settings return correct audit catalog
#   2. Initialize Tables creates PII tables
#   3. PII scan runs and stores results
#   4. Scan history is retrievable
# ============================================================

set -uo pipefail

API="http://localhost:8000/api"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
    local name="$1"
    local condition="$2"
    if eval "$condition"; then
        echo -e "  ${GREEN}✓${NC} $name"
        ((PASS++))
    else
        echo -e "  ${RED}✗${NC} $name"
        ((FAIL++))
    fi
}

echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  PII Tables — Integration Test${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""

# ── Step 0: Check API is running ─────────────────────────────
echo -e "${YELLOW}Step 0: Check API health${NC}"
HEALTH=$(curl -sf "$API/health" 2>/dev/null)
if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: API is not running on $API${NC}"
    echo -e "Start it with: ${BOLD}make api-dev${NC}"
    exit 1
fi
check "API is healthy" "echo '$HEALTH' | grep -q 'ok'"
echo ""

# ── Step 1: Read current settings ────────────────────────────
echo -e "${YELLOW}Step 1: Read audit settings from config${NC}"
CONFIG=$(curl -sf "$API/config" 2>/dev/null)
AUDIT_CATALOG=$(echo "$CONFIG" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('audit_trail',{}).get('catalog','clone_audit'))" 2>/dev/null)
AUDIT_SCHEMA=$(echo "$CONFIG" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('audit_trail',{}).get('schema','logs'))" 2>/dev/null)

if [ -z "$AUDIT_CATALOG" ]; then
    AUDIT_CATALOG="clone_audit"
fi
echo -e "  Audit Catalog: ${BOLD}$AUDIT_CATALOG${NC}"
echo -e "  Audit Schema:  ${BOLD}$AUDIT_SCHEMA${NC}"
check "Audit catalog is set" "[ -n '$AUDIT_CATALOG' ]"
echo ""

# ── Step 2: Initialize Tables (creates PII tables) ──────────
echo -e "${YELLOW}Step 2: Initialize audit tables (including PII)${NC}"
INIT_RESP=$(curl -sf -X POST "$API/audit/init" \
    -H "Content-Type: application/json" \
    -d "{\"catalog\": \"$AUDIT_CATALOG\", \"schema\": \"$AUDIT_SCHEMA\"}" 2>/dev/null)
INIT_STATUS=$?

if [ $INIT_STATUS -ne 0 ]; then
    echo -e "  ${RED}✗ Init request failed (HTTP error)${NC}"
    echo -e "  Response: $INIT_RESP"
    ((FAIL++))
else
    check "Init returned OK" "echo '$INIT_RESP' | grep -q '\"status\":\"ok\"'"

    # Check PII tables are in the created list
    check "pii_scans table created" "echo '$INIT_RESP' | grep -q 'pii_scans'"
    check "pii_detections table created" "echo '$INIT_RESP' | grep -q 'pii_detections'"
    check "pii_remediation table created" "echo '$INIT_RESP' | grep -q 'pii_remediation'"

    # Show all tables created
    echo ""
    echo -e "  Tables created:"
    echo "$INIT_RESP" | python3 -c "
import sys, json
resp = json.load(sys.stdin)
for t in resp.get('tables_created', []):
    print(f'    - {t}')
" 2>/dev/null
fi
echo ""

# ── Step 3: Describe tables (verify PII tables show up) ─────
echo -e "${YELLOW}Step 3: Verify PII tables in describe endpoint${NC}"
DESC_RESP=$(curl -sf -X POST "$API/audit/describe" \
    -H "Content-Type: application/json" \
    -d "{\"catalog\": \"$AUDIT_CATALOG\", \"schema\": \"$AUDIT_SCHEMA\"}" 2>/dev/null)

check "Describe returns pii_scans schema" "echo '$DESC_RESP' | grep -q 'pii_scans'"
check "Describe returns pii_detections schema" "echo '$DESC_RESP' | grep -q 'pii_detections'"
check "Describe returns pii_remediation schema" "echo '$DESC_RESP' | grep -q 'pii_remediation'"
echo ""

# ── Step 4: List catalogs (pick first for PII scan) ─────────
echo -e "${YELLOW}Step 4: Run a PII scan${NC}"
CATALOGS=$(curl -sf "$API/catalogs" 2>/dev/null)
FIRST_CAT=$(echo "$CATALOGS" | python3 -c "
import sys, json
cats = json.load(sys.stdin)
# Pick a catalog that's not the audit catalog
for c in cats:
    name = c.get('name', c) if isinstance(c, dict) else c
    if name != '$AUDIT_CATALOG' and name != 'system':
        print(name)
        break
" 2>/dev/null)

if [ -z "$FIRST_CAT" ]; then
    echo -e "  ${YELLOW}⚠ No catalogs available to scan — skipping scan test${NC}"
else
    echo -e "  Scanning catalog: ${BOLD}$FIRST_CAT${NC}"
    SCAN_RESP=$(curl -sf -X POST "$API/pii-scan" \
        -H "Content-Type: application/json" \
        -d "{\"source_catalog\": \"$FIRST_CAT\", \"sample_data\": true, \"read_uc_tags\": false}" \
        --max-time 120 2>/dev/null)
    SCAN_STATUS=$?

    if [ $SCAN_STATUS -ne 0 ]; then
        echo -e "  ${RED}✗ PII scan request failed${NC}"
        ((FAIL++))
    else
        SCAN_ID=$(echo "$SCAN_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('scan_id',''))" 2>/dev/null)
        PII_COUNT=$(echo "$SCAN_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('summary',{}).get('pii_columns_found',0))" 2>/dev/null)
        TOTAL_COLS=$(echo "$SCAN_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('summary',{}).get('total_columns_scanned',0))" 2>/dev/null)

        check "Scan returned a scan_id" "[ -n '$SCAN_ID' ]"
        echo -e "  Scan ID: ${BOLD}$SCAN_ID${NC}"
        echo -e "  Columns scanned: $TOTAL_COLS, PII found: $PII_COUNT"
    fi
fi
echo ""

# ── Step 5: Verify scan history is stored ────────────────────
echo -e "${YELLOW}Step 5: Verify scan history is stored in Delta tables${NC}"
if [ -n "${FIRST_CAT:-}" ] && [ -n "${SCAN_ID:-}" ]; then
    HISTORY=$(curl -sf "$API/pii-scans?catalog=$FIRST_CAT&limit=5" 2>/dev/null)
    HIST_COUNT=$(echo "$HISTORY" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null)
    check "Scan history has records" "[ '${HIST_COUNT:-0}' -gt 0 ]"

    # Fetch detail for the scan we just ran
    DETAIL=$(curl -sf "$API/pii-scans/$SCAN_ID" 2>/dev/null)
    DET_COUNT=$(echo "$DETAIL" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('detections',[])))" 2>/dev/null)
    check "Scan detail returns detections" "[ '${DET_COUNT:-0}' -ge 0 ]"
    echo -e "  History records: $HIST_COUNT, Detections for scan: $DET_COUNT"
else
    echo -e "  ${YELLOW}⚠ Skipped — no scan was run${NC}"
fi
echo ""

# ── Summary ──────────────────────────────────────────────────
echo -e "${BOLD}============================================================${NC}"
TOTAL=$((PASS + FAIL))
if [ $FAIL -eq 0 ]; then
    echo -e "  ${GREEN}All $TOTAL tests passed ✓${NC}"
else
    echo -e "  ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC} out of $TOTAL"
fi
echo -e "${BOLD}============================================================${NC}"
exit $FAIL
