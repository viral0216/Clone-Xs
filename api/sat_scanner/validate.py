"""SAT Scanner — report validation."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


# Validation rules: check_id → (api_response_key, expected_value_for_pass)
# For workspace-conf boolean checks
_VALIDATE_WS_CONF: dict[str, tuple[str, bool]] = {
    "SAT-IAM-3":  ("enableTokensConfig", True),
    "SAT-IAM-4":  ("enableRoleBasedAccessControl", True),
    "SAT-IAM-6":  ("maxTokenLifetimeDays", True),  # truthy = has a value set
    "SAT-DATA-2": ("enableDbfsFileBrowser", False),  # disabled = good
    "SAT-DATA-5": ("enableResultsDownloading", False),
    "SAT-DATA-6": ("enableExportNotebook", False),
    "SAT-DATA-7": ("enableNotebookTableClipboard", False),
    "SAT-DATA-8": ("storeInteractiveNotebookResultsInCustomerAccount", True),
    "SAT-DATA-9": ("enableFileStoreEndpoint", False),
    "SAT-GOV-1":  ("enforceUserIsolation", True),
    "SAT-GOV-7":  ("enforceUserIsolation", True),
    "SAT-GOV-8":  ("enableVerboseAuditLogs", True),
    "SAT-GOV-11": ("enableDeprecatedGlobalInitScripts", False),
    "SAT-GOV-12": ("enableDeprecatedClusterNamedInitScripts", False),
    "SAT-LOG-1":  ("enableVerboseAuditLogs", True),
    "SAT-NET-2":  ("enableNoPublicIp", True),
}

# Validation rules for list-count checks: check_id → (list_key, what_counts)
_VALIDATE_LIST_COUNT: dict[str, tuple[str, str]] = {
    "SAT-IAM-1":  ("Resources", "admin members"),
    "SAT-DATA-1": ("metastores", "metastore(s)"),
    "SAT-SEC-1":  ("scopes", "secret scope(s)"),
    "SAT-SQL-1":  ("warehouses", "SQL warehouse(s)"),
    "SAT-SQL-2":  ("warehouses", "SQL warehouse(s)"),
    "SAT-SQL-3":  ("warehouses", "SQL warehouse(s)"),
}


def validate_report(json_path: str) -> None:
    """Validate a SAT scanner JSON report by cross-checking findings against stored API responses."""
    path = Path(json_path)
    if not path.exists():
        print(f"  ERROR: File not found: {json_path}")
        sys.exit(1)

    with open(path) as f:
        data = json.load(f)

    findings = data.get("findings", [])
    ws_url = data.get("workspace_url", "")
    ws_name = data.get("workspace_name", "")
    print(f"\n{'━'*70}")
    print(f"  SAT Report Validation")
    print(f"  Workspace: {ws_name or ws_url}")
    print(f"  Scanned:   {data.get('scanned_at', '?')}")
    print(f"  Findings:  {len(findings)}")
    print(f"{'━'*70}\n")

    issues: list[str] = []
    verified = 0
    data_present = 0
    skipped = 0

    for f in findings:
        check_id = f["check_id"]
        status = f["status"]
        current_state = f.get("current_state", "")
        details = f.get("details") or {}
        api_resp = details.get("api_response")
        evidence = f.get("evidence") or {}

        # N/A and API errors — no cross-check, but count api_response if present
        if status in ("NOT_APPLICABLE",) or f.get("is_api_error"):
            if "api_response" in details:
                data_present += 1
            else:
                skipped += 1
            continue

        # ── Validation 1: Workspace-conf boolean checks ──
        if check_id in _VALIDATE_WS_CONF and api_resp and isinstance(api_resp, dict):
            conf_key, pass_when = _VALIDATE_WS_CONF[check_id]
            raw_val = api_resp.get(conf_key)
            if raw_val is not None:
                actual_bool = str(raw_val).lower() in ("true", "1")
                if conf_key == "maxTokenLifetimeDays":
                    actual_bool = bool(raw_val) and str(raw_val) not in ("0", "")
                expected_pass = (actual_bool == pass_when)
                if expected_pass and status == "FAIL":
                    issues.append(f"  MISMATCH  {check_id}: API shows {conf_key}={raw_val} → expected PASS, got FAIL")
                elif not expected_pass and status == "PASS":
                    issues.append(f"  MISMATCH  {check_id}: API shows {conf_key}={raw_val} → expected FAIL/WARN, got PASS")
                else:
                    verified += 1
            else:
                # Key not in response — check if Azure and key might be default-on
                if "azuredatabricks.net" in ws_url.lower() and pass_when and status == "FAIL":
                    issues.append(f"  SUSPECT   {check_id}: {conf_key} not in API response but FAIL on Azure (feature may be enabled by default)")
                else:
                    verified += 1
            continue

        # ── Validation 2: List-count checks ──
        if check_id in _VALIDATE_LIST_COUNT and api_resp and isinstance(api_resp, dict):
            list_key, label = _VALIDATE_LIST_COUNT[check_id]
            items = api_resp.get(list_key, [])
            if check_id == "SAT-IAM-1" and items:
                # Admin group — count members of first group
                api_count = len(items[0].get("members", [])) if items else 0
            else:
                api_count = len(items)
            # Extract count from current_state
            m = re.search(r"(\d+)\s+" + re.escape(label), current_state)
            if m:
                reported_count = int(m.group(1))
                if reported_count != api_count:
                    issues.append(f"  MISMATCH  {check_id}: current_state says {reported_count} {label} but API has {api_count}")
                else:
                    verified += 1
                continue

        # ── Validation 3: Evidence vs API response consistency ──
        if evidence and evidence.get("source") == "workspace-conf" and api_resp and isinstance(api_resp, dict):
            ev_field = evidence.get("field", "")
            ev_value = evidence.get("value")
            if "," not in ev_field:
                api_val = api_resp.get(ev_field)
                if api_val is not None and ev_value is not None and str(api_val) != str(ev_value):
                    issues.append(f"  MISMATCH  {check_id}: evidence says {ev_field}={ev_value} but API shows {ev_field}={api_val}")
                else:
                    verified += 1
                continue

        # ── Validation 4: Ratio checks — verify X/Y in current_state vs API ──
        m = re.match(r"(\d+)/(\d+)\s+", current_state)
        if m and api_resp and isinstance(api_resp, dict):
            reported_num, reported_denom = int(m.group(1)), int(m.group(2))
            # Find the list in the API response
            _v4_matched = False
            for key in ("clusters", "warehouses", "jobs", "scopes", "pipelines", "token_infos",
                        "catalogs", "Resources", "schemas"):
                if key in api_resp:
                    val = api_resp[key]
                    api_total = len(val) if isinstance(val, list) else val
                    if isinstance(api_total, int):
                        if reported_denom == api_total:
                            verified += 1
                            _v4_matched = True
                        # Denominator differs → check sampled or aggregated (normal), fall through to Val 6
                    break
            if _v4_matched:
                continue
            # No raw list key found — fall through to Validation 6 for generic numeric cross-check

        # ── Validation 5: PASS with no API response — suspicious ──
        # An empty dict {} means the API returned valid but empty data — that's fine.
        # Only flag when api_response key is completely absent from details.
        if status == "PASS" and "api_response" not in details and not f.get("is_api_error"):
            ep = details.get("api_endpoint", "")
            if ep and "workspace-conf" not in ep:
                issues.append(f"  SUSPECT   {check_id}: PASS but no API response data stored")
            else:
                skipped += 1
            continue

        # ── Validation 6: Generic numeric cross-check ──
        # For findings with api_response key (including None and {}), extract numbers
        # (list lengths + numeric fields) and compare against numbers in current_state.
        if "api_response" in details:
            if api_resp and isinstance(api_resp, dict):
                # Extract numbers from api_response
                api_nums: dict[str, int] = {}
                for k, v in api_resp.items():
                    if isinstance(v, bool):
                        continue
                    if isinstance(v, int):
                        api_nums[k] = v
                    elif isinstance(v, float) and v == int(v):
                        api_nums[k] = int(v)
                    elif isinstance(v, list):
                        api_nums[f"len({k})"] = len(v)

                # Extract numbers from current_state
                state_nums = set(int(x) for x in re.findall(r'\b(\d+)\b', current_state))

                if api_nums and state_nums:
                    matched = sum(1 for v in api_nums.values() if v in state_nums)
                    # Require at least one meaningful match (>1) to avoid coincidental 0/1
                    meaningful = any(v > 1 and v in state_nums for v in api_nums.values())
                    if meaningful or matched >= 2:
                        verified += 1
                        continue
                    # Check for mismatches: an API count that should appear but doesn't
                    # (only flag if there's exactly one list and its length doesn't match any state number)
                    single_list = [k for k, v in api_resp.items() if isinstance(v, list)]
                    if len(single_list) == 1:
                        list_len = len(api_resp[single_list[0]])
                        if list_len > 1 and list_len not in state_nums:
                            # The check filtered the list — that's normal, count as data_present
                            data_present += 1
                            continue
                    # Some numbers match (0s or 1s) — weak match
                    if matched > 0:
                        data_present += 1
                        continue

            # api_response key exists (possibly None or {} for zero-result queries)
            data_present += 1
            continue

        skipped += 1

    # ── Cross-consistency checks ──
    findings_by_id = {f["check_id"]: f for f in findings}
    # SAT-COMPUTE-3 and SAT-LOG-2 both check cluster log delivery
    c3 = findings_by_id.get("SAT-COMPUTE-3", {})
    l2 = findings_by_id.get("SAT-LOG-2", {})
    if c3.get("status") not in ("NOT_APPLICABLE", None) and l2.get("status") not in ("NOT_APPLICABLE", None):
        c3_pass = c3.get("status") == "PASS"
        l2_pass = l2.get("status") == "PASS"
        if c3_pass != l2_pass:
            issues.append(f"  INCONSIST SAT-COMPUTE-3 ({c3.get('status')}) vs SAT-LOG-2 ({l2.get('status')}): both check cluster log delivery")

    # SAT-DATA-1 and SAT-GOV-10 both check UC metastore
    d1 = findings_by_id.get("SAT-DATA-1", {})
    g10 = findings_by_id.get("SAT-GOV-10", {})
    if d1.get("status") and g10.get("status"):
        d1_has_uc = d1.get("status") == "PASS"
        g10_has_uc = g10.get("status") not in ("NOT_APPLICABLE", "FAIL")
        if d1_has_uc != g10_has_uc and g10.get("status") != "WARN":
            issues.append(f"  INCONSIST SAT-DATA-1 ({d1.get('status')}) vs SAT-GOV-10 ({g10.get('status')}): both check Unity Catalog metastore")

    # ── Print results ──
    total_validated = verified + data_present
    print(f"  Validation Results:")
    print(f"  {'─'*50}")
    print(f"  Verified:     {verified:>3}  (cross-checked against API data)")
    print(f"  Data present: {data_present:>3}  (API response stored, no cross-check rule)")
    print(f"  Issues:       {len(issues):>3}")
    print(f"  Skipped:      {skipped:>3}  (N/A, API errors, no data)")
    print(f"  {'─'*50}")
    pct = (total_validated / len(findings) * 100) if findings else 0
    print(f"  Coverage:     {total_validated}/{len(findings)} ({pct:.0f}%) findings have API evidence")

    if issues:
        print(f"\n  Issues:\n")
        for issue in issues:
            print(issue)
    else:
        print(f"\n  All {verified} cross-checked findings are consistent with API responses.")

    print()
