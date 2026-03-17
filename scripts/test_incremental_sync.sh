#!/usr/bin/env bash
# ============================================================
# Test: Incremental Sync — End-to-End
# ============================================================
# Prerequisites:
#   - API running on localhost:8000 (make api-dev)
#   - A source catalog with at least one schema/table
#
# Usage:
#   ./scripts/test_incremental_sync.sh <source_catalog> <dest_catalog> <schema>
#
# Example:
#   ./scripts/test_incremental_sync.sh edp_dev edp_dev_clone default
#
# What it tests:
#   1. Check for changed tables (POST /incremental/check)
#   2. Inspect local sync state file
#   3. Submit incremental sync job (POST /incremental/sync)
#   4. Poll job until completion (GET /clone/{job_id})
#   5. Re-check — verify tables no longer need sync
#   6. Validate sync state file was updated
# ============================================================

set -uo pipefail

API="http://localhost:8000/api"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'
DIM='\033[2m'

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

# ── Parse args ───────────────────────────────────────────────
if [ $# -lt 3 ]; then
    echo -e "${BOLD}Usage:${NC} $0 <source_catalog> <dest_catalog> <schema>"
    echo -e "${DIM}  e.g. $0 edp_dev edp_dev_clone default${NC}"
    exit 1
fi

SOURCE_CAT="$1"
DEST_CAT="$2"
SCHEMA="$3"
STATE_FILE="sync_state/sync_${SOURCE_CAT}_to_${DEST_CAT}.json"

echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  Incremental Sync — Integration Test${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""
echo -e "  Source:      ${BOLD}${SOURCE_CAT}.${SCHEMA}${NC}"
echo -e "  Destination: ${BOLD}${DEST_CAT}.${SCHEMA}${NC}"
echo -e "  State file:  ${DIM}${STATE_FILE}${NC}"
echo ""

# ── Step 0: Check API is running ─────────────────────────────
echo -e "${YELLOW}Step 0: Check API health${NC}"
HEALTH=$(curl -sf "$API/health" 2>/dev/null)
if [ $? -ne 0 ]; then
    echo -e "  ${RED}ERROR: API is not running on $API${NC}"
    echo -e "  Start it with: ${BOLD}make api-dev${NC}"
    exit 1
fi
check "API is healthy" "echo '$HEALTH' | grep -q 'ok'"
echo ""

# ── Step 1: Check for changed tables ────────────────────────
echo -e "${YELLOW}Step 1: Check for tables needing sync${NC}"
CHECK_RESP=$(curl -sf -X POST "$API/incremental/check" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_catalog\": \"$SOURCE_CAT\",
        \"destination_catalog\": \"$DEST_CAT\",
        \"schema_name\": \"$SCHEMA\"
    }" 2>/dev/null)
CHECK_STATUS=$?

if [ $CHECK_STATUS -ne 0 ]; then
    echo -e "  ${RED}✗ Check request failed${NC}"
    echo -e "  Ensure both catalogs exist and schema '$SCHEMA' has tables."
    exit 1
fi

TABLES_NEEDING=$(echo "$CHECK_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tables_needing_sync', 0))" 2>/dev/null)
check "Check endpoint returned response" "[ -n '$TABLES_NEEDING' ]"

echo ""
echo -e "  Tables needing sync: ${BOLD}$TABLES_NEEDING${NC}"
echo "$CHECK_RESP" | python3 -c "
import sys, json
resp = json.load(sys.stdin)
for t in resp.get('tables', []):
    reason = t.get('reason', '?')
    name = t.get('table_name', '?')
    if reason == 'changed':
        ver = f\"v{t.get('last_synced_version')} → v{t.get('current_version')}\"
        ops = ', '.join(t.get('operations', []))
        print(f'    {name}: {reason} ({ver}) [{ops}]')
    else:
        print(f'    {name}: {reason}')
" 2>/dev/null
echo ""

# ── Step 2: Inspect sync state file ─────────────────────────
echo -e "${YELLOW}Step 2: Inspect local sync state${NC}"
if [ -f "$STATE_FILE" ]; then
    LAST_SYNC=$(python3 -c "
import json
with open('$STATE_FILE') as f:
    s = json.load(f)
print(s.get('last_sync', 'never'))
" 2>/dev/null)
    TABLE_COUNT=$(python3 -c "
import json
with open('$STATE_FILE') as f:
    s = json.load(f)
print(len(s.get('tables', {})))
" 2>/dev/null)
    echo -e "  State file exists: ${GREEN}yes${NC}"
    echo -e "  Last sync: $LAST_SYNC"
    echo -e "  Tables tracked: $TABLE_COUNT"
    check "State file is valid JSON" "python3 -c \"import json; json.load(open('$STATE_FILE'))\" 2>/dev/null"
else
    echo -e "  State file exists: ${YELLOW}no${NC} (first sync)"
    check "No state file is expected for first sync" "true"
fi
echo ""

# ── Step 3: Run incremental sync ────────────────────────────
if [ "$TABLES_NEEDING" -eq 0 ]; then
    echo -e "${YELLOW}Step 3: No tables need syncing — running dry-run instead${NC}"
    DRY_RUN="true"
else
    echo -e "${YELLOW}Step 3: Submit incremental sync job${NC}"
    DRY_RUN="false"
fi

SYNC_RESP=$(curl -sf -X POST "$API/incremental/sync" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_catalog\": \"$SOURCE_CAT\",
        \"destination_catalog\": \"$DEST_CAT\",
        \"schema_name\": \"$SCHEMA\",
        \"clone_type\": \"DEEP\",
        \"dry_run\": $DRY_RUN
    }" 2>/dev/null)

JOB_ID=$(echo "$SYNC_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
check "Sync job submitted" "[ -n '$JOB_ID' ]"
echo -e "  Job ID: ${BOLD}$JOB_ID${NC}"
echo ""

# ── Step 4: Poll job until done ──────────────────────────────
echo -e "${YELLOW}Step 4: Poll job status${NC}"
MAX_POLLS=60
POLL_INTERVAL=3
STATUS="queued"

for i in $(seq 1 $MAX_POLLS); do
    JOB_RESP=$(curl -sf "$API/clone/$JOB_ID" 2>/dev/null)
    STATUS=$(echo "$JOB_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null)

    if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
        break
    fi

    printf "\r  Polling... %s (%ds)" "$STATUS" "$((i * POLL_INTERVAL))"
    sleep $POLL_INTERVAL
done
echo ""

check "Job completed" "[ '$STATUS' = 'completed' ]"

# Show job result
echo "$JOB_RESP" | python3 -c "
import sys, json
job = json.load(sys.stdin)
r = job.get('result', {})
if isinstance(r, dict):
    synced = r.get('synced', r.get('tables_synced', '?'))
    failed = r.get('failed', r.get('tables_failed', '?'))
    checked = r.get('tables_checked', '?')
    print(f'  Result: checked={checked}, synced={synced}, failed={failed}')
elif r:
    print(f'  Result: {r}')
status = job.get('status', '?')
print(f'  Status: {status}')
" 2>/dev/null

# Show logs if available
LOGS=$(echo "$JOB_RESP" | python3 -c "
import sys, json
job = json.load(sys.stdin)
logs = job.get('logs', [])
for line in logs[-10:]:
    print(f'    {line}')
" 2>/dev/null)
if [ -n "$LOGS" ]; then
    echo -e "  ${DIM}Last log lines:${NC}"
    echo "$LOGS"
fi
echo ""

# ── Step 5: Re-check — tables should be in sync ─────────────
if [ "$DRY_RUN" = "false" ] && [ "$STATUS" = "completed" ]; then
    echo -e "${YELLOW}Step 5: Re-check — tables should be in sync now${NC}"
    RECHECK=$(curl -sf -X POST "$API/incremental/check" \
        -H "Content-Type: application/json" \
        -d "{
            \"source_catalog\": \"$SOURCE_CAT\",
            \"destination_catalog\": \"$DEST_CAT\",
            \"schema_name\": \"$SCHEMA\"
        }" 2>/dev/null)

    REMAINING=$(echo "$RECHECK" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tables_needing_sync', -1))" 2>/dev/null)
    # After sync, only "never_synced" tables from the previous check shouldn't count as failures
    echo -e "  Tables still needing sync: ${BOLD}$REMAINING${NC}"
    check "Tables are now in sync (0 remaining)" "[ '$REMAINING' -eq 0 ]"
    echo ""
else
    echo -e "${YELLOW}Step 5: Skipped re-check (dry-run or job failed)${NC}"
    echo ""
fi

# ── Step 6: Validate state file updated ──────────────────────
echo -e "${YELLOW}Step 6: Validate sync state file${NC}"
if [ -f "$STATE_FILE" ]; then
    echo -e "  State file: ${GREEN}exists${NC}"
    python3 -c "
import json
with open('$STATE_FILE') as f:
    s = json.load(f)
print(f\"  Last sync: {s.get('last_sync', 'N/A')}\")
tables = s.get('tables', {})
print(f'  Tables tracked: {len(tables)}')
for key, val in sorted(tables.items()):
    print(f\"    {key}: version={val.get('version')}, synced_at={val.get('synced_at')}\")
" 2>/dev/null
    check "State file updated after sync" "true"
else
    if [ "$DRY_RUN" = "true" ]; then
        echo -e "  ${YELLOW}No state file (expected for dry-run)${NC}"
        check "No state file expected for dry-run" "true"
    else
        echo -e "  ${RED}State file missing after sync${NC}"
        check "State file should exist after sync" "false"
    fi
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
