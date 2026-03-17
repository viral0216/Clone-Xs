#!/usr/bin/env bash
# ============================================================
# End-to-End Test: Incremental Sync — Full Scenario
# ============================================================
# Simulates the exact flow from "How Incremental Sync Works":
#
#   Step 1: Full clone (source → dest)
#   Step 2: Modify the cloned destination table
#   Step 3: Modify the source table (simulate prod change)
#   Step 4: Check for changes (version drift detected)
#   Step 5: Run incremental sync (dest overwritten)
#   Step 6: Verify — dest matches source, local changes lost
#
# Usage:
#   ./scripts/test_incremental_sync_e2e.sh <source_catalog> <dest_catalog> <schema>
#
# Example:
#   ./scripts/test_incremental_sync_e2e.sh edp_dev edp_dev_clone default
#
# Prerequisites:
#   - API running on localhost:8000 (make api-dev)
#   - Source catalog must have at least one table in the schema
#   - The script will CREATE/MODIFY tables in destination — use a dev catalog!
# ============================================================

set -uo pipefail

API="http://localhost:8000/api"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
DIM='\033[2m'
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

poll_job() {
    local job_id="$1"
    local max_polls="${2:-90}"
    local interval="${3:-3}"
    local status="queued"
    local resp_file
    resp_file=$(mktemp)

    for i in $(seq 1 "$max_polls"); do
        curl -sf "$API/clone/$job_id" > "$resp_file" 2>/dev/null
        status=$(python3 -c "import json; print(json.load(open('$resp_file')).get('status','unknown'))" 2>/dev/null)

        if [ "$status" = "completed" ] || [ "$status" = "failed" ]; then
            cat "$resp_file"
            rm -f "$resp_file"
            return 0
        fi
        printf "\r  Waiting... %s (%ds)  " "$status" "$((i * interval))"
        sleep "$interval"
    done
    echo ""
    echo -e "  ${RED}Timeout after $((max_polls * interval))s${NC}"
    cat "$resp_file"
    rm -f "$resp_file"
    return 1
}

run_sql() {
    local sql="$1"
    curl -sf -X POST "$API/execute-sql" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"$sql\"}" 2>/dev/null
}

# ── Parse args ───────────────────────────────────────────────
if [ $# -lt 3 ]; then
    echo -e "${BOLD}Usage:${NC} $0 <source_catalog> <dest_catalog> <schema>"
    echo -e "${DIM}  e.g. $0 edp_dev edp_dev_clone default${NC}"
    echo ""
    echo -e "${YELLOW}WARNING: This script modifies tables in both catalogs. Use dev catalogs only!${NC}"
    exit 1
fi

SOURCE_CAT="$1"
DEST_CAT="$2"
SCHEMA="$3"
STATE_FILE="sync_state/sync_${SOURCE_CAT}_to_${DEST_CAT}.json"

echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  Incremental Sync — End-to-End Test${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""
echo -e "  Source:      ${BOLD}${SOURCE_CAT}.${SCHEMA}${NC}"
echo -e "  Destination: ${BOLD}${DEST_CAT}.${SCHEMA}${NC}"
echo -e "  State file:  ${DIM}${STATE_FILE}${NC}"
echo ""
echo -e "  ${YELLOW}⚠  This test will modify tables in both catalogs!${NC}"
echo ""

# ── Step 0: Preflight ───────────────────────────────────────
echo -e "${BLUE}━━━ Step 0: Preflight ━━━${NC}"
HEALTH=$(curl -sf "$API/health" 2>/dev/null)
if [ $? -ne 0 ]; then
    echo -e "  ${RED}ERROR: API not running. Start with: make api-dev${NC}"
    exit 1
fi
check "API is healthy" "echo '$HEALTH' | grep -q 'ok'"

# Get warehouse ID from config
WID=$(curl -sf "$API/config" | python3 -c "import sys,json; print(json.load(sys.stdin).get('sql_warehouse_id',''))" 2>/dev/null)
check "Warehouse ID found" "[ -n '$WID' ]"
echo -e "  Warehouse: ${DIM}$WID${NC}"

# Validate schema — 'default' and 'information_schema' are always excluded by clone
if [ "$SCHEMA" = "default" ] || [ "$SCHEMA" = "information_schema" ]; then
    echo ""
    echo -e "  ${YELLOW}⚠  Schema '${SCHEMA}' is always excluded by clone_catalog.${NC}"
    echo -e "  Auto-selecting a different schema..."
    SCHEMA=$(curl -sf "$API/catalogs/${SOURCE_CAT}/schemas" | python3 -c "
import sys, json
schemas = json.load(sys.stdin)
skip = {'information_schema', 'default'}
for s in schemas:
    if s not in skip:
        print(s)
        break
" 2>/dev/null)
    if [ -z "$SCHEMA" ]; then
        echo -e "  ${RED}No usable schemas found in ${SOURCE_CAT}${NC}"
        exit 1
    fi
    echo -e "  Using schema: ${BOLD}${SCHEMA}${NC}"
    STATE_FILE="sync_state/sync_${SOURCE_CAT}_to_${DEST_CAT}.json"
fi

# Pick a test table from the source schema
echo ""
echo -e "  Listing tables in ${SOURCE_CAT}.${SCHEMA}..."
TABLES_JSON=$(run_sql "SELECT table_name FROM ${SOURCE_CAT}.information_schema.tables WHERE table_schema = '${SCHEMA}' AND table_type IN ('MANAGED','EXTERNAL') LIMIT 10")
TEST_TABLE=$(echo "$TABLES_JSON" | python3 -c "
import sys, json
rows = json.load(sys.stdin)
for r in rows:
    print(r.get('table_name', ''))
    break
" 2>/dev/null)

if [ -z "$TEST_TABLE" ]; then
    echo -e "  ${RED}No tables found in ${SOURCE_CAT}.${SCHEMA}${NC}"
    exit 1
fi
echo -e "  Test table: ${BOLD}$TEST_TABLE${NC}"
echo ""

# ── Step 1: Full Clone ──────────────────────────────────────
echo -e "${BLUE}━━━ Step 1: Full Clone (${SOURCE_CAT} → ${DEST_CAT}) ━━━${NC}"
echo -e "  Cloning ${SOURCE_CAT}.${SCHEMA} → ${DEST_CAT}.${SCHEMA}..."

# Clear old sync state so incremental check works fresh
if [ -f "$STATE_FILE" ]; then
    echo -e "  Clearing old sync state..."
    rm -f "$STATE_FILE"
fi

CLONE_RESP=$(curl -sf -X POST "$API/clone" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_catalog\": \"$SOURCE_CAT\",
        \"destination_catalog\": \"$DEST_CAT\",
        \"clone_type\": \"DEEP\",
        \"include_schemas\": [\"$SCHEMA\"],
        \"exclude_schemas\": [],
        \"enable_rollback\": false,
        \"copy_permissions\": false,
        \"copy_ownership\": false
    }" 2>/dev/null)

CLONE_JOB=$(echo "$CLONE_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
check "Clone job submitted" "[ -n '$CLONE_JOB' ]"
echo -e "  Job ID: ${DIM}$CLONE_JOB${NC}"

echo -e "  Waiting for clone to finish..."
JOB_RESULT_FILE=$(mktemp)
poll_job "$CLONE_JOB" 120 5 > "$JOB_RESULT_FILE"
echo ""
CLONE_STATUS=$(python3 -c "import json; print(json.load(open('$JOB_RESULT_FILE')).get('status','unknown'))" 2>/dev/null)
SCHEMAS_PROCESSED=$(python3 -c "import json; r=json.load(open('$JOB_RESULT_FILE')).get('result',{}); print(r.get('schemas_processed',0))" 2>/dev/null)
check "Clone completed" "[ '$CLONE_STATUS' = 'completed' ]"
check "Schemas processed > 0" "[ '${SCHEMAS_PROCESSED:-0}' -gt 0 ]"

if [ "$CLONE_STATUS" != "completed" ] || [ "${SCHEMAS_PROCESSED:-0}" -eq 0 ]; then
    echo -e "  ${RED}Clone failed or processed 0 schemas — cannot continue${NC}"
    echo -e "  ${DIM}Logs:${NC}"
    python3 -c "
import json
j = json.load(open('$JOB_RESULT_FILE'))
for l in j.get('logs', []): print(f'    {l}')
if j.get('error'): print(f'  Error: {j[\"error\"]}')
r = j.get('result', {})
print(f'  schemas_processed={r.get(\"schemas_processed\",0)}, tables_success={r.get(\"tables\",{}).get(\"success\",0)}')
" 2>/dev/null
    rm -f "$JOB_RESULT_FILE"
    exit 1
fi
rm -f "$JOB_RESULT_FILE"

# Get source table row count as baseline
SRC_COUNT=$(run_sql "SELECT COUNT(*) as cnt FROM ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
DST_COUNT=$(run_sql "SELECT COUNT(*) as cnt FROM ${DEST_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
echo -e "  Source row count: ${BOLD}$SRC_COUNT${NC}"
echo -e "  Dest row count:   ${BOLD}$DST_COUNT${NC}"
check "Row counts match after clone" "[ '$SRC_COUNT' = '$DST_COUNT' ]"

# Get dest table version
DEST_VER_BEFORE=$(run_sql "DESCRIBE HISTORY ${DEST_CAT}.${SCHEMA}.${TEST_TABLE} LIMIT 1" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('version',0))" 2>/dev/null)
echo -e "  Dest table version: $DEST_VER_BEFORE"
echo ""

# ── Step 2: Modify destination table ────────────────────────
echo -e "${BLUE}━━━ Step 2: Modify Destination Table (simulate user edits) ━━━${NC}"

# Get columns to build an INSERT
COLS_JSON=$(run_sql "DESCRIBE TABLE ${DEST_CAT}.${SCHEMA}.${TEST_TABLE}")
FIRST_STRING_COL=$(echo "$COLS_JSON" | python3 -c "
import sys, json
cols = json.load(sys.stdin)
for c in cols:
    if 'string' in c.get('data_type','').lower():
        print(c.get('col_name',''))
        break
" 2>/dev/null)

if [ -n "$FIRST_STRING_COL" ]; then
    echo -e "  Inserting test row into ${DEST_CAT}.${SCHEMA}.${TEST_TABLE}..."
    INSERT_RESP=$(run_sql "INSERT INTO ${DEST_CAT}.${SCHEMA}.${TEST_TABLE} (${FIRST_STRING_COL}) VALUES ('__INCR_SYNC_TEST__')")
    DST_COUNT_AFTER_EDIT=$(run_sql "SELECT COUNT(*) as cnt FROM ${DEST_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
    echo -e "  Dest row count after edit: ${BOLD}$DST_COUNT_AFTER_EDIT${NC}"
    check "Destination has extra row" "[ '$DST_COUNT_AFTER_EDIT' -gt '$DST_COUNT' ]"
else
    echo -e "  ${YELLOW}No string column found — inserting a marker via SQL${NC}"
    run_sql "INSERT INTO ${DEST_CAT}.${SCHEMA}.${TEST_TABLE} SELECT * FROM ${DEST_CAT}.${SCHEMA}.${TEST_TABLE} LIMIT 1" >/dev/null 2>&1
    DST_COUNT_AFTER_EDIT=$(run_sql "SELECT COUNT(*) as cnt FROM ${DEST_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
    echo -e "  Dest row count after edit: ${BOLD}$DST_COUNT_AFTER_EDIT${NC}"
    check "Destination was modified" "[ '$DST_COUNT_AFTER_EDIT' != '$DST_COUNT' ]"
fi

DEST_VER_AFTER_EDIT=$(run_sql "DESCRIBE HISTORY ${DEST_CAT}.${SCHEMA}.${TEST_TABLE} LIMIT 1" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('version',0))" 2>/dev/null)
echo -e "  Dest table version after edit: $DEST_VER_AFTER_EDIT"
echo ""

# ── Step 3: Modify source table ─────────────────────────────
echo -e "${BLUE}━━━ Step 3: Modify Source Table (simulate prod change) ━━━${NC}"

SRC_VER_BEFORE=$(run_sql "DESCRIBE HISTORY ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE} LIMIT 1" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('version',0))" 2>/dev/null)
echo -e "  Source version before: $SRC_VER_BEFORE"

# Touch the source table to bump its version
if [ -n "$FIRST_STRING_COL" ]; then
    run_sql "INSERT INTO ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE} (${FIRST_STRING_COL}) VALUES ('__SRC_CHANGE_TEST__')" >/dev/null 2>&1
else
    run_sql "INSERT INTO ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE} SELECT * FROM ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE} LIMIT 1" >/dev/null 2>&1
fi

SRC_VER_AFTER=$(run_sql "DESCRIBE HISTORY ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE} LIMIT 1" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('version',0))" 2>/dev/null)
SRC_COUNT_AFTER=$(run_sql "SELECT COUNT(*) as cnt FROM ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
echo -e "  Source version after:  $SRC_VER_AFTER"
echo -e "  Source row count:      ${BOLD}$SRC_COUNT_AFTER${NC}"
check "Source version bumped" "[ '$SRC_VER_AFTER' -gt '$SRC_VER_BEFORE' ]"
echo ""

# ── Step 4: Check for changes ───────────────────────────────
echo -e "${BLUE}━━━ Step 4: Check for Changes (detect version drift) ━━━${NC}"

CHECK_RESP=$(curl -sf -X POST "$API/incremental/check" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_catalog\": \"$SOURCE_CAT\",
        \"destination_catalog\": \"$DEST_CAT\",
        \"schema_name\": \"$SCHEMA\"
    }" 2>/dev/null)

TABLES_NEEDING=$(echo "$CHECK_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tables_needing_sync',0))" 2>/dev/null)
echo -e "  Tables needing sync: ${BOLD}$TABLES_NEEDING${NC}"
check "Changed tables detected" "[ '$TABLES_NEEDING' -gt 0 ]"

# Show details
echo "$CHECK_RESP" | python3 -c "
import sys, json
resp = json.load(sys.stdin)
for t in resp.get('tables', []):
    name = t.get('table_name', '?')
    reason = t.get('reason', '?')
    if reason == 'changed':
        v_from = t.get('last_synced_version', '?')
        v_to = t.get('current_version', '?')
        ops = ', '.join(t.get('operations', []))
        print(f'    {name}: {reason} (v{v_from} → v{v_to}) [{ops}]')
    else:
        print(f'    {name}: {reason}')
" 2>/dev/null
echo ""

# ── Step 5: Run incremental sync ────────────────────────────
echo -e "${BLUE}━━━ Step 5: Run Incremental Sync (dest gets overwritten) ━━━${NC}"

SYNC_RESP=$(curl -sf -X POST "$API/incremental/sync" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_catalog\": \"$SOURCE_CAT\",
        \"destination_catalog\": \"$DEST_CAT\",
        \"schema_name\": \"$SCHEMA\",
        \"clone_type\": \"DEEP\"
    }" 2>/dev/null)

SYNC_JOB=$(echo "$SYNC_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
check "Sync job submitted" "[ -n '$SYNC_JOB' ]"
echo -e "  Job ID: ${DIM}$SYNC_JOB${NC}"

echo -e "  Waiting for sync to finish..."
SYNC_RESULT=$(poll_job "$SYNC_JOB" 90 3)
echo ""
SYNC_STATUS=$(echo "$SYNC_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null)
check "Sync completed" "[ '$SYNC_STATUS' = 'completed' ]"

echo "$SYNC_RESULT" | python3 -c "
import sys, json
job = json.load(sys.stdin)
r = job.get('result', {})
if isinstance(r, dict):
    print(f\"  Synced: {r.get('synced', '?')}, Failed: {r.get('failed', '?')}, Checked: {r.get('tables_checked', '?')}\")
logs = job.get('logs', [])
if logs:
    print('  Last log lines:')
    for line in logs[-5:]:
        print(f'    {line}')
" 2>/dev/null
echo ""

# ── Step 6: Verify — dest matches source, local edits gone ──
echo -e "${BLUE}━━━ Step 6: Verify Results ━━━${NC}"

# Row counts should match source (dest edits overwritten)
FINAL_SRC=$(run_sql "SELECT COUNT(*) as cnt FROM ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
FINAL_DST=$(run_sql "SELECT COUNT(*) as cnt FROM ${DEST_CAT}.${SCHEMA}.${TEST_TABLE}" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)

echo -e "  Source row count: ${BOLD}$FINAL_SRC${NC}"
echo -e "  Dest row count:   ${BOLD}$FINAL_DST${NC}"
echo -e "  Dest count before sync (with local edits): ${DIM}$DST_COUNT_AFTER_EDIT${NC}"
check "Dest matches source (local edits overwritten)" "[ '$FINAL_SRC' = '$FINAL_DST' ]"
check "Dest no longer has extra rows from Step 2" "[ '$FINAL_DST' != '$DST_COUNT_AFTER_EDIT' ] || [ '$FINAL_SRC' = '$FINAL_DST' ]"

# Check test marker row is gone from dest
if [ -n "$FIRST_STRING_COL" ]; then
    MARKER=$(run_sql "SELECT COUNT(*) as cnt FROM ${DEST_CAT}.${SCHEMA}.${TEST_TABLE} WHERE ${FIRST_STRING_COL} = '__INCR_SYNC_TEST__'" | python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('cnt',0))" 2>/dev/null)
    check "Test marker row gone from dest (overwritten)" "[ '${MARKER:-0}' -eq 0 ]"
fi

# Re-check — no tables should need sync
RECHECK=$(curl -sf -X POST "$API/incremental/check" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_catalog\": \"$SOURCE_CAT\",
        \"destination_catalog\": \"$DEST_CAT\",
        \"schema_name\": \"$SCHEMA\"
    }" 2>/dev/null)
REMAINING=$(echo "$RECHECK" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tables_needing_sync',0))" 2>/dev/null)
check "No tables need sync after incremental sync" "[ '$REMAINING' -eq 0 ]"

# Validate state file
if [ -f "$STATE_FILE" ]; then
    echo ""
    echo -e "  ${DIM}Sync state file:${NC}"
    python3 -c "
import json
with open('$STATE_FILE') as f:
    s = json.load(f)
print(f\"    Last sync: {s.get('last_sync')}\")
for k, v in sorted(s.get('tables', {}).items()):
    print(f\"    {k}: version={v.get('version')}\")
" 2>/dev/null
    check "State file updated" "true"
fi
echo ""

# ── Cleanup: remove test rows from source ────────────────────
echo -e "${BLUE}━━━ Cleanup ━━━${NC}"
if [ -n "$FIRST_STRING_COL" ]; then
    run_sql "DELETE FROM ${SOURCE_CAT}.${SCHEMA}.${TEST_TABLE} WHERE ${FIRST_STRING_COL} = '__SRC_CHANGE_TEST__'" >/dev/null 2>&1
    echo -e "  Removed test row from source"
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
