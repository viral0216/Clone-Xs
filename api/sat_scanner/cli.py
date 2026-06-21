"""
Databricks SAT Scanner — Standalone CLI
=========================================

A standalone command-line tool that replicates ALL functionality of the
SAT Scanner UI. Connects to a Databricks workspace (via PAT token or
Azure AD login), runs 345 security and operational health checks, and exports results
in JSON, CSV, Excel (.xlsx), HTML, Jira, and/or Delta tables.
Ref: https://github.com/databricks-industry-solutions/security-analysis-tool/blob/main/configs/sat_dasf_mapping.csv
Usage examples:
    # Interactive — prompts for workspace URL and token
    sat-scanner

    # PAT token — explicit flags
    sat-scanner --host https://adb-xxxx.azuredatabricks.net --token dapi*****

    # Azure AD login — opens browser, picks tenant/sub/workspace
    sat-scanner --azure

    # Scan all workspaces in one Azure subscription
    sat-scanner --azure-all

    # Scan ALL workspaces across ALL subscriptions in the tenant
    sat-scanner --azure-tenant

    # Environment variables
    export DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
    export DATABRICKS_TOKEN=dapi*****
    sat-scanner

    # Export specific format(s)
    sat-scanner --host ... --token ... --format excel json html csv

    # Export to specific output directory
    sat-scanner --host ... --token ... --output ./my-reports

    # Check connectivity only
    sat-scanner --host ... --token ... --check-only

    # Quiet mode — no progress output, just export files
    sat-scanner --host ... --token ... --quiet

Install:
    pip install sat-scanner           # core
    pip install sat-scanner[excel]    # with Excel export
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Re-export all public symbols so existing imports keep working:
#   from sat_scanner.cli import SATFinding, SAT_CHECKS, run_scan, ...
# ─────────────────────────────────────────────────────────────────────────────

from .models import SATFinding, SATScanResult  # noqa: F401
from .checks import (  # noqa: F401
    SAT_CHECKS, _get_effort, _EFFORT_MAP, CHECK_API_ENDPOINTS, CHECK_BENEFITS,
    PORTAL_LINKS, _WS_CONF_EVIDENCE, _ITEM_EXTRACTORS, EXCEL_CELL_LIMIT,
    AZURE_REGION_TO_GEO, _resolve_geo, CROSS_GEO_DISABLED_BY_DEFAULT,
    _WORKSPACE_ACCOUNT_IDS, _WORKSPACE_REGIONS, _WORKSPACE_ARM_INFO,
    _AZURE_MGMT_TOKEN,
)
from .api import (  # noqa: F401
    _make_finding, _na, _dbx_get, _dbx_post, _dbx_get_all_jobs,
    _dbx_get_workspace_conf, _acct_get, _fetch_account_id,
    _ItemTrackingClient, _extract_api_items, _MAX_RETRIES, _RETRY_BACKOFF,
    _MAX_JOBS_LIMIT, _ACCT_API_BASE, _DOC_SUMMARIES, _fetch_doc_summaries,
)
from .helpers import (  # noqa: F401
    _pl, _log, setup_logging, _sanitize_name, _file_prefix, _extract_org_id,
    _resolve_portal_link, _auto_extract_evidence, _details_str, _format_scan_items,
    _render_secret_details_html, _render_scan_items_html,
)
from .scoring import (  # noqa: F401
    SEV_WEIGHT, _compute_sat_score, _build_endpoint_summary, _print_summary,
)
from .scanner import run_scan, check_connectivity  # noqa: F401
from .inventory import (  # noqa: F401
    run_inventory, compare_inventories, run_inventory_many, run_inventory_fleet,
    aggregate_inventories,
)
from .exporters import (  # noqa: F401
    export_json, export_api_dump, export_csv, export_excel, export_html,
    export_recommendation_summary, export_jira, export_ado, export_sarif,
    export_inventory_json, export_inventory_excel, export_inventory_html,
    export_inventory_hierarchy_html, export_inventory_fleet_html,
    export_source_diff_json, export_combined_inventory_json,
    export_combined_inventory_excel, export_combined_inventory_html,
    export_azure_hierarchy_json, export_azure_hierarchy_excel, export_azure_hierarchy_html,
)
from .delta import (  # noqa: F401
    export_delta, ensure_delta_tables, detect_and_store_changes,
    export_delta_report, cleanup_old_runs, _resolve_warehouse_id,
    _select_sql_warehouse, _select_catalog, _select_schema,
    _query_sql_statement, _exec_sql_statement, _sql_escape,
    _EXPECTED_TABLES, ensure_inventory_tables, export_inventory_delta,
    _INVENTORY_TABLES,
)
from .azure_auth import (  # noqa: F401
    azure_login_flow, azure_tenant_flow, fetch_tokens_from_existing_session,
    _activate_terminal, _is_headless, _run_az, _az_login,
)
from .combined import _print_combined_summary, _export_combined_html  # noqa: F401
from .profiles.modern import export_html_modern, export_combined_html_modern, export_recommendation_summary_modern  # noqa: F401
from .validate import validate_report  # noqa: F401
from .dashboard import create_or_update_dashboard  # noqa: F401


# ─────────────────────────────────────────────────────────────────────────────
# Main CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    try:
        _main_inner()
    except KeyboardInterrupt:
        print("\n\n  Scan cancelled by user (Ctrl+C). Exiting.")
        sys.exit(130)


def _resolve_inventory_targets(args, host, token, workspace_name) -> list[tuple[str, str, str]]:
    """Resolve (host, token, name) targets for inventory mode.

    Reuses the existing ``az`` CLI login for BOTH Databricks (an AAD token is
    minted from the current session when no PAT is given) and Azure ARM
    discovery — no interactive ``az login`` is triggered here.
    """
    if args.azure_tenant:
        return azure_tenant_flow()
    if args.azure or args.azure_all:
        return azure_login_flow(scan_all=args.azure_all)
    if host and token:
        return [(host, token, workspace_name)]
    if host and not token:
        try:
            dbx_token, _ = fetch_tokens_from_existing_session()
        except Exception as e:
            print(f"\n  No PAT token and could not use the existing az CLI login: {e}")
            print("  Provide --token, set DATABRICKS_TOKEN, run 'az login', or use --azure.")
            sys.exit(1)
        if not dbx_token:
            print("\n  Could not obtain a Databricks token from the existing az CLI login.")
            sys.exit(1)
        if not args.quiet:
            _log("Using existing az CLI login for Databricks + Azure authentication.")
        return [(host, dbx_token, workspace_name)]
    print("\n  --inventory requires --host (with --token, an active 'az login', or --azure).")
    sys.exit(1)


def _export_inventory(args, inv, target_dir, inv_formats):
    """Write one workspace's inventory files (json/excel/html). Delta is handled
    centrally by _export_inventory_delta_central."""
    if "json" in inv_formats:
        p = export_inventory_json(inv, target_dir)
        if not args.quiet:
            print(f"  JSON inventory: {p}")
    if "excel" in inv_formats:
        p = export_inventory_excel(inv, target_dir)
        if not args.quiet:
            print(f"  Excel inventory: {p}")
    if "html" in inv_formats:
        p = export_inventory_html(inv, target_dir)
        if not args.quiet:
            print(f"  HTML inventory: {p}")
        for p in export_inventory_hierarchy_html(inv, target_dir):
            if not args.quiet:
                print(f"  HTML hierarchy diagram: {p}")


def _setup_central_delta(args, targets):
    """Resolve ONE central Delta destination for all workspaces (host/token/catalog/
    schema/warehouse). Returns a dict or None if Delta can't be set up."""
    if args.inventory_delta_host:
        host = args.inventory_delta_host.rstrip("/")
        token = args.inventory_delta_token or ""
        if not token:
            try:
                token, _ = fetch_tokens_from_existing_session()
            except Exception:
                token = ""
        if not token:
            print("  --inventory-delta-host needs --inventory-delta-token or an az login; Delta skipped.")
            return None
    else:
        host, token, name = targets[0]
        host = host.rstrip("/")
        if not args.quiet:
            _log(f"Delta: storing all workspaces in the first target's metastore "
                 f"({name or host}); use --inventory-delta-host to centralize elsewhere.")
    try:
        wh = _resolve_warehouse_id(host, token, args.delta_warehouse)
        cat, sch = ensure_inventory_tables(args.delta_catalog, args.delta_schema, host, token, wh)
        if not args.quiet:
            _log(f"Delta ready -> `{cat}`.`{sch}` ({len(_INVENTORY_TABLES)} tables)")
        return {"host": host, "token": token, "warehouse_id": wh, "catalog": cat, "schema": sch}
    except Exception as e:
        print(f"  Delta setup failed: {e} — Delta export skipped.")
        return None


def _export_inventory_delta_central(args, inv, workspace, run_id, central):
    """Write one workspace's inventory to the shared central Delta tables."""
    try:
        export_inventory_delta(inv, run_id, workspace, central["catalog"], central["schema"],
                               central["host"], central["token"], central["warehouse_id"],
                               mode=args.delta_mode)
        if not args.quiet:
            print(f"  Delta: {workspace} -> `{central['catalog']}`.`{central['schema']}`")
    except Exception as e:
        print(f"  Delta export failed for {workspace}: {e}")


async def _inventory_for(args, ws_host, ws_token, ws_name, catalogs, source, warehouse_id, skip_azure):
    """Run a single inventory for a given source — shared by single & compare modes."""
    return await run_inventory(
        ws_host, ws_token, ws_name, quiet=args.quiet,
        concurrency=args.inventory_concurrency,
        catalogs=catalogs,
        include_system=args.inventory_include_system,
        grants=args.inventory_grants,
        effective_grants=args.inventory_effective_grants,
        tags_sql=args.inventory_tags_sql,
        warehouse_id=warehouse_id,
        max_catalogs=args.max_catalogs,
        max_schemas_per_catalog=args.max_schemas_per_catalog,
        max_tables_per_schema=args.max_tables_per_schema,
        skip_azure=skip_azure,
        source=source,
        monitors=args.inventory_monitors,
    )


def _print_source_diff(diff):
    """Print the api-vs-sql comparison table to the console."""
    print("\n  ── API vs SQL source comparison ──")
    print(f"  {'object':20s}{'api':>9s}{'sql':>9s}{'match':>8s}")
    for k, v in diff["counts"].items():
        print(f"  {k:20s}{v['api']:>9d}{v['sql']:>9d}{('YES' if v['match'] else 'DIFF'):>8s}")
    for name, d in diff["differences"].items():
        if not d["match"]:
            print(f"    {name}: only-in-api={d['only_in_api_total']}, only-in-sql={d['only_in_sql_total']}")
    if diff["column_count_mismatch_total"]:
        print(f"    column-count mismatches (shared tables): {diff['column_count_mismatch_total']}")
    print()


def _run_compare(args, ws_host, ws_token, ws_name, catalogs, warehouse_id, target_dir, inv_formats):
    """Run BOTH api and sql sources, write each + a source-diff report (Azure skipped)."""
    if not warehouse_id:
        print("  --inventory-compare needs a SQL warehouse for the sql source; none available. Skipping.")
        return
    if not args.quiet:
        _log("Comparison: enumerating via API source ...")
    api_inv = asyncio.run(_inventory_for(args, ws_host, ws_token, ws_name, catalogs, "api", "", True))
    if not args.quiet:
        _log("Comparison: enumerating via SQL source ...")
    sql_inv = asyncio.run(_inventory_for(args, ws_host, ws_token, ws_name, catalogs, "sql", warehouse_id, True))

    cmp_formats = inv_formats - {"delta"}
    api_dir = target_dir / "api"
    sql_dir = target_dir / "sql"
    api_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(parents=True, exist_ok=True)
    _export_inventory(args, api_inv, api_dir, cmp_formats)
    _export_inventory(args, sql_inv, sql_dir, cmp_formats)

    diff = compare_inventories(api_inv, sql_inv)
    p = export_source_diff_json(diff, target_dir, sql_inv.workspace_name or ws_name)
    if not args.quiet:
        print(f"  Source diff: {p}")
    _print_source_diff(diff)


def _run_inventory_mode(args, host, token, workspace_name):
    """Enumerate Unity Catalog objects + Azure infra for the resolved target(s).

    Logging is already configured by _main_inner before this runs.
    """
    targets = _resolve_inventory_targets(args, host, token, workspace_name)

    run_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = Path(args.output) / f"uc_inventory_{run_ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    inv_formats = set(args.inventory_format)
    catalogs = [c.strip() for c in args.inventory_catalogs.split(",") if c.strip()]

    # ── Compare mode: per-workspace api-vs-sql, handled sequentially ──
    if args.inventory_compare:
        for ws_host, ws_token, ws_name in targets:
            ws_host = ws_host.rstrip("/")
            if not asyncio.run(check_connectivity(ws_host, ws_token)):
                print(f"  Cannot reach workspace {ws_name or ws_host}. Skipping.")
                continue
            try:
                wh = _resolve_warehouse_id(ws_host, ws_token, args.delta_warehouse)
            except Exception as e:
                print(f"  Could not resolve a SQL warehouse: {e}")
                wh = ""
            tdir = run_dir / _sanitize_name(ws_name or ws_host) if len(targets) > 1 else run_dir
            tdir.mkdir(parents=True, exist_ok=True)
            _run_compare(args, ws_host, ws_token, ws_name, catalogs, wh, tdir, inv_formats)
        if not args.quiet:
            print(f"\n  Inventory written to: {run_dir}\n")
        return

    # ── Central Delta destination (resolved once for the whole fleet) ──
    central = _setup_central_delta(args, targets) if "delta" in inv_formats else None
    fleet_run_id = str(uuid.uuid4())

    # ── Enumerate all workspaces (dedup UC enumeration across shared metastores) ──
    dedup = (not args.inventory_no_metastore_dedup) and len(targets) > 1
    results = asyncio.run(run_inventory_fleet(
        targets,
        dedup=dedup,
        workspace_concurrency=args.inventory_workspace_concurrency,
        metastore_concurrency=args.inventory_metastore_concurrency,
        concurrency=args.inventory_concurrency,
        catalogs=catalogs,
        include_system=args.inventory_include_system,
        grants=args.inventory_grants,
        effective_grants=args.inventory_effective_grants,
        tags_sql=args.inventory_tags_sql,
        warehouse_id="",
        max_catalogs=args.max_catalogs,
        max_schemas_per_catalog=args.max_schemas_per_catalog,
        max_tables_per_schema=args.max_tables_per_schema,
        skip_azure=args.inventory_skip_azure,
        source=args.inventory_source,
        monitors=args.inventory_monitors,
    ))

    multi = len(targets) > 1
    for ws_name, ws_host, ws_token, inv in results:
        if inv is None:
            continue
        label = inv.workspace_name or ws_name or ws_host
        target_dir = run_dir / _sanitize_name(label) if multi else run_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        _export_inventory(args, inv, target_dir, inv_formats)
        if central is not None:
            _export_inventory_delta_central(args, inv, label, fleet_run_id, central)

    # ── Combined cross-workspace report ──
    reachable = [r for r in results if r[3] is not None]
    if len(reachable) > 1:
        agg = aggregate_inventories(results)
        if "json" in inv_formats:
            print(f"  Combined JSON: {export_combined_inventory_json(agg, run_dir)}")
        if "excel" in inv_formats:
            print(f"  Combined Excel: {export_combined_inventory_excel(agg, run_dir)}")
        if "html" in inv_formats:
            print(f"  Combined HTML: {export_combined_inventory_html(agg, run_dir)}")
            for p in export_inventory_fleet_html(results, run_dir):
                print(f"  Fleet diagram: {p}")

    if not args.quiet:
        print(f"\n  Inventory written to: {run_dir}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Azure Resource Hierarchy mode
# ─────────────────────────────────────────────────────────────────────────────

def _azure_tenant_id() -> str:
    """Tenant id of the current az session (empty if not logged in)."""
    try:
        data = _run_az(["account", "show"])
        return data.get("tenantId", "") if isinstance(data, dict) else ""
    except Exception:
        return ""


async def _collect_azure_hierarchy(args, token, csv_path, errors):
    """Assemble the MG→Subscription→RG→Resource tree from CSV and/or live ARM."""
    import httpx
    import sat_scanner.azure_hierarchy as azh
    from .azure_infra import _scoped_subscription_ids

    # MG → subscription tree: CSV is authoritative when the API is blocked; else live API.
    mg_tree = None
    if csv_path:
        try:
            mg_tree = azh.build_tree_from_csv(azh.load_rows(csv_path))
        except Exception as e:
            errors.append(f"CSV import ({csv_path}): {e}")

    subs, rgs, resources = [], [], []
    if token:
        async with httpx.AsyncClient(timeout=45) as client:
            tenant_id = _azure_tenant_id()
            if mg_tree is None:
                mg_tree = await azh.discover_management_groups(
                    client, token, tenant_id, args.azure_hierarchy_mg_root, errors)
            sub_ids = _scoped_subscription_ids(tenant_id)
            subs = await azh.discover_subscriptions(client, token, sub_ids, errors)
            if not args.azure_hierarchy_skip_resources:
                rgs = await azh.discover_resource_groups(client, token, sub_ids, errors)
                resources = await azh.discover_resources(client, token, sub_ids, errors)

    if mg_tree is None and not subs:
        return None
    return azh.build_full_tree(mg_tree, subs, rgs, resources)


def _export_azure_hierarchy(args, tree, out_dir, formats):
    """Write the requested Azure hierarchy formats (json / excel / html)."""
    if "json" in formats:
        p = export_azure_hierarchy_json(tree, out_dir)
        if not args.quiet:
            print(f"  JSON: {p}")
    if "excel" in formats:
        p = export_azure_hierarchy_excel(tree, out_dir)
        if not args.quiet:
            print(f"  Excel: {p}")
    if "html" in formats:
        for p in export_azure_hierarchy_html(tree, out_dir):
            if not args.quiet:
                print(f"  HTML: {p}")


def _run_azure_hierarchy_mode(args):
    """Build an Excel + HTML report of the full Azure resource tree (no Databricks needed)."""
    import sat_scanner.azure_hierarchy as azh

    run_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_dir = Path(args.output) / f"azure_hierarchy_{run_ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    formats = set(args.azure_hierarchy_format)
    if "all" in formats:
        formats = {"json", "excel", "html"}
    errors: list[str] = []

    # Mint the ARM management token from the existing az login (interactive if --azure*).
    token = ""
    try:
        _, token = fetch_tokens_from_existing_session()
    except Exception as e:
        if args.azure or args.azure_all or args.azure_tenant:
            try:
                _az_login()
                _, token = fetch_tokens_from_existing_session()
            except Exception as e2:
                errors.append(f"az login: {e2}")
        else:
            errors.append(f"az login: {e}")

    csv_path = args.azure_hierarchy_csv.strip()
    if not token and not csv_path:
        print("\n  --azure-hierarchy needs an active 'az login' (for live discovery) "
              "and/or --azure-hierarchy-csv <portal export>.")
        print("  Run 'az login' first, add --azure for an interactive login, "
              "or pass a Management-Groups CSV export.\n")
        sys.exit(1)

    tree = asyncio.run(_collect_azure_hierarchy(args, token, csv_path, errors))
    if tree is None:
        print("\n  Could not build the Azure resource hierarchy:")
        for e in errors:
            print(f"    - {e}")
        sys.exit(1)

    azh.assign_colours(tree)
    counts = azh.count_by_type(tree)
    if not args.quiet:
        print(f"\n  Hierarchy: {counts.get('Management group', 0)} management groups, "
              f"{counts.get('Subscription', 0)} subscriptions, "
              f"{counts.get('Resource group', 0)} resource groups, "
              f"{counts.get('Resource', 0)} resources.")
        for e in errors:
            print(f"    note: {e}")

    _export_azure_hierarchy(args, tree, out_dir, formats)
    if not args.quiet:
        print(f"\n  Azure hierarchy reports written to: {out_dir}\n")


def _main_inner():
    parser = argparse.ArgumentParser(
        description="Databricks SAT Scanner — Standalone CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive PAT token
  sat-scanner

  # Explicit credentials
  sat-scanner --host https://adb-xxxx.azuredatabricks.net --token dapi*****

  # Azure AD login (interactive browser flow)
  sat-scanner --azure

  # Scan ALL workspaces in an Azure subscription
  sat-scanner --azure-all

  # Scan ALL workspaces across ALL subscriptions in the Azure tenant
  sat-scanner --azure-tenant

  # Environment variables
  export DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
  export DATABRICKS_TOKEN=dapi*****
  sat-scanner

  # Specific exports
  sat-scanner --host ... --token ... --format excel html

  # Check connectivity only
  sat-scanner --host ... --token ... --check-only
        """,
    )
    parser.add_argument("--host", help="Databricks workspace URL (e.g. https://adb-xxxx.azuredatabricks.net)")
    parser.add_argument("--token", help="Databricks Personal Access Token (PAT)")
    parser.add_argument("--name", help="Workspace display name (for report filenames)")
    parser.add_argument("--azure", action="store_true", help="Use Azure AD login (interactive browser flow)")
    parser.add_argument("--azure-all", action="store_true", help="Azure AD login and scan ALL workspaces in the subscription")
    parser.add_argument("--azure-tenant", action="store_true", help="Azure AD login and scan ALL workspaces across ALL subscriptions in the tenant")
    login_group = parser.add_mutually_exclusive_group()
    login_group.add_argument("--browser", action="store_true",
        help="Force browser-based Azure login (default on desktop)")
    login_group.add_argument("--codespace", action="store_true",
        help="Force device-code Azure login (default in Codespaces/SSH/containers)")
    parser.add_argument("--format", nargs="+", choices=["json", "csv", "excel", "html", "all"],
        default=["all"], help="Export format(s) (default: all)")
    parser.add_argument("--output", default="reports", help="Output directory for reports (default: reports)")

    # ── Unity Catalog Inventory mode ──
    inv_group = parser.add_argument_group("Unity Catalog Inventory")
    inv_group.add_argument("--inventory", action="store_true",
        help="Enumerate ALL Unity Catalog objects (catalogs/schemas/tables/columns/grants) "
             "and the associated Azure infrastructure, instead of running the SAT scan.")
    inv_group.add_argument("--inventory-format", nargs="+", choices=["json", "excel", "delta", "html"],
        default=["json", "excel", "html"], help="Inventory export format(s) (default: json excel html)")
    inv_group.add_argument("--inventory-source", default="auto",
        choices=["auto", "api", "sql", "both"],
        help="Enumeration backend: auto (prefer system.information_schema for the once-per-metastore "
             "fleet scan when a warehouse is available, else api; resolves to api for single-workspace "
             "runs), api (REST), sql (system.information_schema, rate-limit friendly, needs a "
             "warehouse), or both (sql + api gap-fill). Default: auto")
    inv_group.add_argument("--inventory-compare", action="store_true",
        help="Run BOTH the api and sql sources and write a *-source-diff.json comparing them "
             "(per-type counts + object-level differences). Requires a SQL warehouse.")
    inv_group.add_argument("--with-inventory", action="store_true",
        help="During a normal SAT scan, ALSO enumerate the UC + Azure inventory and enrich the "
             "scan report with it (embedded in the JSON, added as 'UC *' sheets in the Excel, "
             "plus a companion inventory HTML). Reuses the --inventory-* options "
             "(e.g. --inventory-source both for the fullest picture).")
    inv_group.add_argument("--inventory-concurrency", type=int, default=5, metavar="N",
        help="Max concurrent UC API requests (default: 5; lower if you hit rate limits)")
    inv_group.add_argument("--inventory-catalogs", default="", metavar="CSV",
        help="Restrict inventory to a comma-separated list of catalog names")
    inv_group.add_argument("--inventory-include-system", action="store_true",
        help="Include system / information_schema / hive_metastore objects")
    inv_group.add_argument("--inventory-grants", default="coarse",
        choices=["none", "coarse", "table"],
        help="Grant depth: none, coarse (metastore/catalog/schema), or table (every object)")
    inv_group.add_argument("--inventory-effective-grants", action="store_true",
        help="Use effective (inherited) permissions instead of direct grants")
    inv_group.add_argument("--inventory-tags-sql", action="store_true",
        help="Enrich tags from system.information_schema (requires a SQL warehouse)")
    inv_group.add_argument("--inventory-skip-azure", action="store_true",
        help="Skip Azure infrastructure discovery (UC objects only)")
    inv_group.add_argument("--inventory-monitors", action="store_true",
        help="Also fetch Lakehouse Monitoring config per table (one extra API call per table)")
    inv_group.add_argument("--inventory-workspace-concurrency", type=int, default=3, metavar="N",
        help="Max workspaces inventoried in parallel for multi-workspace runs (default: 3)")
    inv_group.add_argument("--inventory-no-metastore-dedup", action="store_true",
        help="Disable metastore-level dedup; enumerate every workspace independently. By default, "
             "multi-workspace runs enumerate each shared UC metastore's catalog tree ONCE and reuse "
             "it (only Azure infra runs per workspace).")
    inv_group.add_argument("--inventory-metastore-concurrency", type=int, default=2, metavar="N",
        help="Max metastore leader enumerations run in parallel for fleet runs (default: 2)")
    inv_group.add_argument("--inventory-delta-host", default="", metavar="URL",
        help="Central workspace URL to store ALL workspaces' inventory Delta tables "
             "(default: the first scanned workspace)")
    inv_group.add_argument("--inventory-delta-token", default="", metavar="TOKEN",
        help="Token for --inventory-delta-host (falls back to an existing az login)")
    inv_group.add_argument("--max-catalogs", type=int, default=0, metavar="N",
        help="Cap catalogs enumerated (0 = all)")
    inv_group.add_argument("--max-schemas-per-catalog", type=int, default=0, metavar="N",
        help="Cap schemas per catalog (0 = all)")
    inv_group.add_argument("--max-tables-per-schema", type=int, default=0, metavar="N",
        help="Cap tables per schema (0 = all)")

    # ── Azure Resource Hierarchy mode ──
    azh_group = parser.add_argument_group("Azure Resource Hierarchy")
    azh_group.add_argument("--azure-hierarchy", action="store_true",
        help="Build an Excel + HTML report of the FULL Azure resource tree "
             "(Management group -> Subscription -> Resource group -> every resource), "
             "instead of running the SAT scan. Needs an 'az login' and/or a portal "
             "Management-Groups CSV export (--azure-hierarchy-csv). No Databricks workspace required.")
    azh_group.add_argument("--azure-hierarchy-csv", default="", metavar="PATH",
        help="Azure portal 'Management Groups' CSV export, used for the management-group / "
             "subscription tree (works even where the ARM Management API is 'Not Authorized'). "
             "Subscriptions are then enriched with live resource groups + resources.")
    azh_group.add_argument("--azure-hierarchy-mg-root", default="", metavar="ID",
        help="Root management group id to enumerate live (default: the tenant root). "
             "Ignored when --azure-hierarchy-csv is given and the API is not authorized.")
    azh_group.add_argument("--azure-hierarchy-format", nargs="+", default=["all"],
        choices=["json", "excel", "html", "all"],
        help="Azure hierarchy export format(s) (default: all)")
    azh_group.add_argument("--azure-hierarchy-skip-resources", action="store_true",
        help="Stop at subscriptions — skip live resource-group / resource discovery "
             "(matches the original management-groups-only view).")

    parser.add_argument("--report-profile", dest="report_profile", default="modern",
        choices=["classic", "modern"],
        help="HTML report profile: 'classic' or 'modern' (SchemaX-style) (default: modern)")
    parser.add_argument("--check-only", action="store_true", help="Check connectivity only — don't run a scan")
    parser.add_argument("--no-api-response", action="store_true", help="Exclude API Response column from CSV, Excel, and HTML exports")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    parser.add_argument("--scan-secrets", action="store_true",
        help="Enable TruffleHog-based secret scanning (requires trufflehog binary on PATH)")
    parser.add_argument("--scan-secrets-days", type=int, default=None, metavar="N",
        help="Only scan notebooks modified in the last N days (requires --scan-secrets)")
    parser.add_argument("--show-scan-items", action="store_true",
        help="Show the list of scanned items (notebooks, clusters, jobs, etc.) in the HTML report")
    parser.add_argument("--evidence", action="store_true",
        help="Show evidence (specific API field/value that drove each finding's result)")
    parser.add_argument("--effort", action="store_true",
        help="Show estimated remediation effort for each check (e.g. Quick Fix, Moderate, Significant, Project)")
    parser.add_argument("--architecture", action="store_true",
                        help="Include architecture diagrams in HTML report")
    parser.add_argument("--show-cost", action="store_true",
        help="Show estimated monthly cloud operational cost of each misconfiguration (illustrative examples only)")
    parser.add_argument("--max-jobs", type=int, default=0, metavar="N",
        help="Maximum number of jobs to fetch via pagination (default: 0 = all jobs)")
    parser.add_argument("--delta", action="store_true",
        help="Write scan results to Unity Catalog Delta tables")
    parser.add_argument("--delta-catalog", default="sat_scanner", metavar="NAME",
        help="Unity Catalog catalog name (default: sat_scanner)")
    parser.add_argument("--delta-schema", default="results", metavar="NAME",
        help="Schema/database name within the catalog (default: results)")
    parser.add_argument("--delta-warehouse", default="", metavar="ID",
        help="SQL warehouse ID for Delta writes (auto-discovered if not set)")
    parser.add_argument("--delta-host", default="", metavar="URL",
        help="Central workspace URL for Delta writes (default: write to each scanned workspace)")
    parser.add_argument("--delta-token", default="", metavar="TOKEN",
        help="PAT token for the central Delta workspace (required with --delta-host)")
    parser.add_argument("--delta-mode", default="merge", choices=["merge", "append", "overwrite"],
        help="Write mode: merge (upsert, default), append (blind insert), overwrite (replace run)")
    parser.add_argument("--delta-retain", type=int, default=0, metavar="N",
        help="Keep only the last N scans per workspace (0 = keep all)")
    parser.add_argument("--dashboard", action="store_true",
        help="Create a Lakeview dashboard from Delta tables (requires --delta)")
    parser.add_argument("--dashboard-only", action="store_true",
        help="Create/update dashboard without running a scan (requires --host, --token, and --delta flags)")
    parser.add_argument("--sarif", nargs="?", const="sat-scan.sarif", default=None, metavar="FILE",
        help="Export results in SARIF 2.1.0 format for CI/CD integration (default filename: sat-scan.sarif)")
    parser.add_argument("--dump-api", action="store_true",
        help="Export all raw API responses to a standalone JSON file for independent verification")
    parser.add_argument("--include-checks", default="", metavar="IDS",
        help="Only include these check IDs (comma-separated, e.g. SAT-IAM-1,SAT-NET-1)")
    parser.add_argument("--exclude-checks", default="", metavar="IDS",
        help="Exclude these check IDs from results (comma-separated)")
    parser.add_argument("--fail-on", default="", choices=["", "high", "medium", "low"],
        help="Exit with code 1 if any findings at or above this severity (for CI/CD)")
    parser.add_argument("--webhook", default="", metavar="URL",
        help="POST scan summary to a webhook URL (Slack, Teams, Discord, or any HTTP endpoint)")
    parser.add_argument("--validate", nargs="?", const="__auto__", metavar="JSON_REPORT",
        help="Validate report by cross-checking findings against stored API responses. "
             "Pass a JSON report path to validate an existing report, or use with --azure/scan flags to auto-validate after scan.")

    # ── Jira export ──
    jira_group = parser.add_argument_group("Jira export", "Create Jira issues from scan findings")
    jira_group.add_argument("--jira", action="store_true",
        help="Export findings to Jira (creates Story per category, Subtask per finding)")
    jira_group.add_argument("--jira-url", metavar="URL",
        help="Jira base URL (e.g. https://your-org.atlassian.net)")
    jira_group.add_argument("--jira-project", default="SAT", metavar="KEY",
        help="Jira project key (default: SAT)")
    jira_group.add_argument("--jira-email", metavar="EMAIL",
        help="Jira account email")
    jira_group.add_argument("--jira-token", metavar="TOKEN",
        help="Jira API token (or set JIRA_TOKEN env var)")
    jira_group.add_argument("--jira-parent-type", default="Story", metavar="TYPE",
        help="Parent issue type name (default: Story)")
    jira_group.add_argument("--jira-child-type", default="Subtask", metavar="TYPE",
        help="Child issue type name (default: Subtask)")
    jira_group.add_argument("--jira-dry-run", action="store_true",
        help="Print what would be created without actually creating issues")
    jira_group.add_argument("--jira-from-report", metavar="JSON_PATH",
        help="Create Jira issues from an existing JSON report (skip scan)")

    # ── Azure DevOps export ──
    ado_group = parser.add_argument_group("Azure DevOps export", "Create work items from scan findings")
    ado_group.add_argument("--ado", action="store_true",
        help="Export findings to Azure DevOps (creates Epic per category, User Story per finding)")
    ado_group.add_argument("--ado-org", metavar="URL",
        help="Azure DevOps organization URL (e.g. https://dev.azure.com/your-org)")
    ado_group.add_argument("--ado-project", default="SAT", metavar="NAME",
        help="Azure DevOps project name (default: SAT)")
    ado_group.add_argument("--ado-token", metavar="TOKEN",
        help="Azure DevOps Personal Access Token (or set ADO_TOKEN env var)")
    ado_group.add_argument("--ado-parent-type", default="Epic", metavar="TYPE",
        help="Parent work item type (default: Epic)")
    ado_group.add_argument("--ado-child-type", default="User Story", metavar="TYPE",
        help="Child work item type (default: User Story)")
    ado_group.add_argument("--ado-area-path", default="", metavar="PATH",
        help="Area path for work items (e.g. Project\\Team)")
    ado_group.add_argument("--ado-iteration-path", default="", metavar="PATH",
        help="Iteration path for work items (e.g. Project\\Sprint 1)")
    ado_group.add_argument("--ado-dry-run", action="store_true",
        help="Print what would be created without actually creating work items")
    ado_group.add_argument("--ado-from-report", metavar="JSON_PATH",
        help="Create Azure DevOps work items from an existing JSON report (skip scan)")

    args = parser.parse_args()
    setup_logging(quiet=args.quiet)

    # ── Jira-from-report mode: create tickets from existing report, no scan ──
    if args.jira_from_report:
        import json as _json
        from .models import SATFinding
        report_path = Path(args.jira_from_report)
        if not report_path.exists():
            print(f"  Report not found: {report_path}")
            sys.exit(1)
        data = _json.loads(report_path.read_text(encoding="utf-8"))
        findings = [
            SATFinding(**{k: v for k, v in f.items() if k in SATFinding.__slots__})
            for f in data.get("findings", [])
        ]
        if not findings:
            print("  No findings found in report.")
            sys.exit(1)
        print(f"  Loaded {len(findings)} findings from {report_path}")

        _jira_url = args.jira_url or os.getenv("JIRA_URL", "")
        _jira_email = args.jira_email or os.getenv("JIRA_EMAIL", "")
        _jira_token = args.jira_token or os.getenv("JIRA_TOKEN", "")
        if not _jira_url:
            _jira_url = input("  Jira Base URL (e.g. https://your-org.atlassian.net): ").strip()
        if not _jira_email:
            _jira_email = input("  Jira Email: ").strip()
        if not _jira_token:
            import getpass
            _jira_token = getpass.getpass("  Jira API Token: ")
        if _jira_url and _jira_email and _jira_token:
            _log(f"Creating Jira issues in project {args.jira_project}...")
            jira_result = export_jira(
                None,
                jira_url=_jira_url,
                jira_email=_jira_email,
                jira_token=_jira_token,
                project_key=args.jira_project,
                parent_type=args.jira_parent_type,
                child_type=args.jira_child_type,
                dry_run=args.jira_dry_run,
                findings=findings,
            )
            _log(f"Jira: {jira_result['created']} issues created, {jira_result['errors']} errors")
        else:
            print("  Jira export skipped — missing URL, email, or token.")
        sys.exit(0)

    # ── ADO-from-report mode: create work items from existing report, no scan ──
    if args.ado_from_report:
        import json as _json
        from .models import SATFinding
        report_path = Path(args.ado_from_report)
        if not report_path.exists():
            print(f"  Report not found: {report_path}")
            sys.exit(1)
        data = _json.loads(report_path.read_text(encoding="utf-8"))
        findings = [
            SATFinding(**{k: v for k, v in f.items() if k in SATFinding.__slots__})
            for f in data.get("findings", [])
        ]
        if not findings:
            print("  No findings found in report.")
            sys.exit(1)
        print(f"  Loaded {len(findings)} findings from {report_path}")

        _ado_org = args.ado_org or os.getenv("ADO_ORG", "")
        _ado_token = args.ado_token or os.getenv("ADO_TOKEN", "")
        if not _ado_org:
            _ado_org = input("  Azure DevOps Org URL (e.g. https://dev.azure.com/your-org): ").strip()
        if not _ado_token:
            import getpass
            _ado_token = getpass.getpass("  Azure DevOps PAT: ")
        if _ado_org and _ado_token:
            _log(f"Creating Azure DevOps work items in project {args.ado_project}...")
            ado_result = export_ado(
                None,
                ado_org=_ado_org,
                ado_project=args.ado_project,
                ado_token=_ado_token,
                parent_type=args.ado_parent_type,
                child_type=args.ado_child_type,
                dry_run=args.ado_dry_run,
                findings=findings,
                area_path=args.ado_area_path,
                iteration_path=args.ado_iteration_path,
            )
            _log(f"ADO: {ado_result['created']} work items created, {ado_result['errors']} errors")
        else:
            print("  Azure DevOps export skipped — missing org URL or token.")
        sys.exit(0)

    # ── Validate mode: read existing report and validate ──
    if args.validate and args.validate != "__auto__":
        validate_report(args.validate)
        sys.exit(0)

    # ── Dashboard-only mode: create/update dashboard without scanning ──
    if args.dashboard_only:
        _dh = args.delta_host or args.host or os.getenv("DATABRICKS_HOST", "")
        _dt = args.delta_token or args.token or os.getenv("DATABRICKS_TOKEN", "")

        # Azure AD login — get host/token from Azure flow
        if not _dh or not _dt:
            if args.azure or args.azure_all or args.azure_tenant:
                import sat_scanner.azure_auth as _auth_mod
                _auth_mod._LOGIN_MODE = "codespace" if args.codespace else ("browser" if args.browser else "auto")
                try:
                    if args.azure_tenant:
                        _targets = azure_tenant_flow()
                    else:
                        _targets = azure_login_flow(scan_all=args.azure_all)
                except Exception as e:
                    print(f"\n  Azure login failed: {e}")
                    sys.exit(1)
                if not _targets:
                    print("  No workspaces found.")
                    sys.exit(1)
                if len(_targets) == 1:
                    _dh, _dt, _ = _targets[0]
                else:
                    print("\n  Select a workspace for the dashboard:\n")
                    for i, (h, t, n) in enumerate(_targets, 1):
                        print(f"    {i}. {n or h}")
                    print()
                    while True:
                        choice = input(f"  Choose [1-{len(_targets)}]: ").strip()
                        try:
                            idx = int(choice) - 1
                            if 0 <= idx < len(_targets):
                                _dh, _dt, _ = _targets[idx]
                                break
                        except ValueError:
                            pass
                        print(f"  Invalid choice. Enter a number between 1 and {len(_targets)}.")

        if not _dh or not _dt:
            print("  --dashboard-only requires --host/--token, --delta-host/--delta-token, or --azure")
            sys.exit(1)
        _dh = _dh.rstrip("/")
        _dwid = _resolve_warehouse_id(_dh, _dt, args.delta_warehouse)
        from .dashboard import create_or_update_dashboard
        try:
            url = create_or_update_dashboard(
                args.delta_catalog, args.delta_schema,
                _dh, _dt, _dwid, quiet=args.quiet,
            )
            if not args.quiet:
                print(f"\n  Dashboard: {url}\n")
        except Exception as e:
            print(f"\n  Dashboard creation failed: {e}")
            sys.exit(1)
        sys.exit(0)

    # ── Set globals early (before login flows that depend on them) ──
    import sat_scanner.api as _api_mod
    import sat_scanner.azure_auth as _auth_mod
    _api_mod._MAX_JOBS_LIMIT = args.max_jobs
    _auth_mod._LOGIN_MODE = "codespace" if args.codespace else ("browser" if args.browser else "auto")

    host = args.host or os.getenv("DATABRICKS_HOST", "")
    token = args.token or os.getenv("DATABRICKS_TOKEN", "")
    workspace_name = args.name or ""

    # ── Azure Resource Hierarchy mode (no Databricks workspace required) ──
    if args.azure_hierarchy:
        _run_azure_hierarchy_mode(args)
        sys.exit(0)

    # ── Unity Catalog Inventory mode (short-circuits the SAT scan) ──
    if args.inventory:
        _run_inventory_mode(args, host, token, workspace_name)
        sys.exit(0)

    # ── Resolve workspace targets ──
    targets: list[tuple[str, str, str]] = []  # (host, token, workspace_name)

    if args.azure_tenant:
        try:
            targets = azure_tenant_flow()
        except Exception as e:
            print(f"\n  ❌ Azure tenant scan failed: {e}")
            sys.exit(1)
    elif args.azure or args.azure_all:
        try:
            targets = azure_login_flow(scan_all=args.azure_all)
        except Exception as e:
            print(f"\n  ❌ Azure login failed: {e}")
            sys.exit(1)
    elif host and token:
        targets = [(host, token, workspace_name)]
    elif host and not token:
        # ── Host given without a PAT: reuse the existing az CLI login (no prompt) ──
        try:
            _dbx_token, _ = fetch_tokens_from_existing_session()
        except Exception:
            _dbx_token = ""
        if _dbx_token:
            if not args.quiet:
                _log("Using existing az CLI login for Databricks authentication.")
            targets = [(host, _dbx_token, workspace_name)]
        else:
            print("\n  --host was provided without --token and no usable 'az login' session was found.")
            print("  Provide --token, set DATABRICKS_TOKEN, run 'az login', or use --azure.")
            sys.exit(1)
    else:
        # ── Interactive mode if no host/token ──
        print("\n  No workspace credentials provided.")
        print("  Options:")
        print("    1. Enter PAT token manually")
        print("    2. Azure AD login (browser)")
        print("    3. Exit")
        choice = input("\n  Choose [1/2/3]: ").strip()
        if choice == "2":
            try:
                targets = azure_login_flow()
            except Exception as e:
                print(f"\n  ❌ Azure login failed: {e}")
                sys.exit(1)
        elif choice == "1":
            host = input("  Workspace URL: ").strip().rstrip("/")
            token = input("  PAT Token: ").strip()
            if not host or not token:
                print("  ❌ Both workspace URL and token are required.")
                sys.exit(1)
            workspace_name = input("  Workspace name (optional): ").strip()
            targets = [(host, token, workspace_name)]
        else:
            sys.exit(0)

    # ── Export settings ──
    output_base = Path(args.output)
    run_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = output_base / f"sat_run_{run_ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    formats = set(args.format)
    if "all" in formats:
        formats = {"json", "csv", "excel", "html"}
    # Auto-validate needs JSON export
    auto_validate = args.validate == "__auto__"
    if auto_validate and "json" not in formats:
        formats.add("json")
    include_api = not args.no_api_response
    show_evidence = args.evidence
    show_effort = args.effort

    # ── Delta: select workspace for Delta table writes (REST API — no SQL connector needed) ──
    _delta_host = ""
    _delta_token = ""
    _delta_warehouse_id = ""

    if args.delta and targets:
        if args.delta_host:
            # Explicit workspace via --delta-host / --delta-token flags
            if not args.delta_token:
                print("  --delta-token is required when using --delta-host")
                sys.exit(1)
            _delta_host = args.delta_host.rstrip("/")
            _delta_token = args.delta_token
        elif len(targets) == 1:
            # Single workspace — confirm it
            _h, _t, _n = targets[0]
            print(f"\n  Delta results will be stored in: {_n or _h}")
            _delta_host = _h.rstrip("/")
            _delta_token = _t
        else:
            # Multiple workspaces — prompt user to pick one
            print("\n  Select a workspace to store Delta table results:\n")
            for i, (h, t, n) in enumerate(targets, 1):
                print(f"    {i}. {n or h}")
            print()
            while True:
                choice = input(f"  Choose [1-{len(targets)}]: ").strip()
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(targets):
                        _delta_host = targets[idx][0].rstrip("/")
                        _delta_token = targets[idx][1]
                        print(f"  Selected: {targets[idx][2] or _delta_host}")
                        break
                except ValueError:
                    pass
                print(f"  Invalid choice. Enter a number between 1 and {len(targets)}.")

        if not args.quiet:
            _log(f"Connecting to Delta workspace: {_delta_host}")
        try:
            _delta_warehouse_id = _resolve_warehouse_id(
                _delta_host, _delta_token, args.delta_warehouse
            )
            _resolved_cat, _resolved_sch = ensure_delta_tables(
                args.delta_catalog, args.delta_schema,
                _delta_host, _delta_token, _delta_warehouse_id,
            )
            args.delta_catalog = _resolved_cat
            args.delta_schema = _resolved_sch
            if not args.quiet:
                _log(f"Delta ready -> `{_resolved_cat}`.`{_resolved_sch}` ({len(_EXPECTED_TABLES)} tables)")
        except Exception as e:
            print(f"\n  Delta setup failed: {e}")
            print("    Scan will continue but Delta export will be skipped.\n")
            _delta_warehouse_id = ""

    # ── Process each workspace ──
    scan_results: list[tuple[str, SATScanResult | None]] = []
    skipped: list[str] = []

    for ws_idx, (ws_host, ws_token, ws_name) in enumerate(targets):
        ws_host = ws_host.rstrip("/")

        if len(targets) > 1 and not args.quiet:
            print(f"\n{'━'*70}")
            print(f"  Workspace {ws_idx+1}/{len(targets)}: {ws_name or ws_host}")
            print(f"{'━'*70}")

        # ── Check connectivity ──
        if not args.quiet:
            _log("Checking connectivity...")
        reachable = asyncio.run(check_connectivity(ws_host, ws_token))
        if not reachable:
            print(f"  Cannot reach workspace {ws_name or ws_host}. Skipping.")
            skipped.append(ws_name or ws_host)
            scan_results.append((ws_name or ws_host, None))
            continue

        if args.check_only:
            continue

        # ── Run the scan ──
        result = asyncio.run(run_scan(
            ws_host, ws_token, ws_name, args.quiet,
            scan_secrets=args.scan_secrets,
            scan_secrets_days=args.scan_secrets_days,
        ))

        # ── Apply --include-checks / --exclude-checks filtering ──
        if args.include_checks or args.exclude_checks:
            _inc = {s.strip().upper() for s in args.include_checks.split(",") if s.strip()} if args.include_checks else set()
            _exc = {s.strip().upper() for s in args.exclude_checks.split(",") if s.strip()} if args.exclude_checks else set()
            before = len(result.findings)
            if _inc:
                result.findings = [f for f in result.findings if f.check_id.upper() in _inc]
            if _exc:
                result.findings = [f for f in result.findings if f.check_id.upper() not in _exc]
            if not args.quiet and len(result.findings) != before:
                _log(f"Check filter: {before} → {len(result.findings)} findings")

        scan_results.append((ws_name or ws_host, result))

        # ── Export to per-workspace subfolder ──
        if len(targets) > 1:
            ws_dir = run_dir / _sanitize_name(ws_name or ws_host)
        else:
            ws_dir = run_dir
        ws_dir.mkdir(parents=True, exist_ok=True)

        # ── Optional: enrich this scan with a UC + Azure inventory ──
        if args.with_inventory:
            _inv_wh = ""
            if args.inventory_source in ("sql", "both") or args.inventory_tags_sql:
                try:
                    _inv_wh = _resolve_warehouse_id(ws_host, ws_token, args.delta_warehouse)
                except Exception as e:
                    print(f"  Inventory: could not resolve a SQL warehouse: {e}")
            _inv_cats = [c.strip() for c in args.inventory_catalogs.split(",") if c.strip()]
            if not args.quiet:
                _log(f"Enriching report with UC + Azure inventory (source={args.inventory_source}) ...")
            try:
                result.inventory_obj = asyncio.run(_inventory_for(
                    args, ws_host, ws_token, ws_name, _inv_cats,
                    args.inventory_source, _inv_wh, args.inventory_skip_azure))
            except Exception as e:
                print(f"  Inventory enrichment failed: {e}")

        exported: list[str] = []
        _json_path = ""
        if "json" in formats:
            _json_path = export_json(result, ws_dir)
            exported.append(_json_path)
        if "csv" in formats:
            exported.append(export_csv(result, ws_dir, include_api_response=include_api, show_scan_items=args.show_scan_items, show_evidence=show_evidence, show_effort=show_effort, show_cost=args.show_cost))
        if "excel" in formats:
            exported.append(export_excel(result, ws_dir, include_api_response=include_api, show_scan_items=args.show_scan_items, show_evidence=show_evidence, show_effort=show_effort, show_cost=args.show_cost))
        if "html" in formats:
            _sum_link = f"../sat-combined-summary-{datetime.now().strftime('%Y-%m-%d')}.html" if len(targets) > 1 else ""
            _html_fn = export_html_modern if args.report_profile == "modern" else export_html
            exported.append(_html_fn(result, ws_dir, include_api_response=include_api, summary_link=_sum_link, show_scan_items=args.show_scan_items, show_evidence=show_evidence, show_effort=show_effort, show_cost=args.show_cost))
        if args.dump_api:
            exported.append(export_api_dump(result, ws_dir))
        if args.sarif:
            _sarif_path = str(ws_dir / args.sarif)
            exported.append(export_sarif(result, _sarif_path, quiet=args.quiet))
        if "html" in formats:
            _rec_fn = export_recommendation_summary_modern if args.report_profile == "modern" else export_recommendation_summary
            _recom_path = _rec_fn(result, ws_dir, show_cost=args.show_cost,
                                  show_architecture=args.architecture)
            if _recom_path:
                exported.append(_recom_path)
        # Companion UC + Azure inventory HTML (the JSON/Excel are already enriched in-place)
        if args.with_inventory and getattr(result, "inventory_obj", None) is not None and "html" in formats:
            exported.append(export_inventory_html(result.inventory_obj, ws_dir))

        if not args.quiet:
            _log(f"Exported {len(exported)} report(s) → {ws_dir}/")
            for path in exported:
                print(f"     → {path}")
            print()

        # ── Webhook notification ──
        _webhook_url = args.webhook or os.getenv("SAT_WEBHOOK_URL", "")
        if _webhook_url:
            from .exporters import export_webhook
            export_webhook(result, _webhook_url, quiet=args.quiet)

        # ── Jira export ──
        if args.jira:
            _jira_url = args.jira_url or os.getenv("JIRA_URL", "")
            _jira_email = args.jira_email or os.getenv("JIRA_EMAIL", "")
            _jira_token = args.jira_token or os.getenv("JIRA_TOKEN", "")
            if not _jira_url:
                _jira_url = input("  Jira Base URL (e.g. https://your-org.atlassian.net): ").strip()
            if not _jira_email:
                _jira_email = input("  Jira Email: ").strip()
            if not _jira_token:
                import getpass
                _jira_token = getpass.getpass("  Jira API Token: ")
            if _jira_url and _jira_email and _jira_token:
                if not args.quiet:
                    _log(f"Creating Jira issues in project {args.jira_project}...")
                jira_result = export_jira(
                    result,
                    jira_url=_jira_url,
                    jira_email=_jira_email,
                    jira_token=_jira_token,
                    project_key=args.jira_project,
                    parent_type=args.jira_parent_type,
                    child_type=args.jira_child_type,
                    dry_run=args.jira_dry_run,
                )
                if not args.quiet:
                    _log(f"Jira: {jira_result['created']} issues created, {jira_result['errors']} errors")
            else:
                print("  Jira export skipped — missing URL, email, or token.")

        # ── Azure DevOps export ──
        if args.ado:
            _ado_org = args.ado_org or os.getenv("ADO_ORG", "")
            _ado_token = args.ado_token or os.getenv("ADO_TOKEN", "")
            if not _ado_org:
                _ado_org = input("  Azure DevOps Org URL (e.g. https://dev.azure.com/your-org): ").strip()
            if not _ado_token:
                import getpass
                _ado_token = getpass.getpass("  Azure DevOps PAT: ")
            if _ado_org and _ado_token:
                if not args.quiet:
                    _log(f"Creating Azure DevOps work items in project {args.ado_project}...")
                ado_result = export_ado(
                    result,
                    ado_org=_ado_org,
                    ado_project=args.ado_project,
                    ado_token=_ado_token,
                    parent_type=args.ado_parent_type,
                    child_type=args.ado_child_type,
                    dry_run=args.ado_dry_run,
                    area_path=args.ado_area_path,
                    iteration_path=args.ado_iteration_path,
                )
                if not args.quiet:
                    _log(f"ADO: {ado_result['created']} work items created, {ado_result['errors']} errors")
            else:
                print("  Azure DevOps export skipped — missing org URL or token.")

        # ── Delta table export (via SQL Statement REST API — fast batch writes) ──
        if args.delta and _delta_warehouse_id:
            import uuid as _uuid
            _run_id = str(_uuid.uuid4())
            _cat, _sch = args.delta_catalog, args.delta_schema
            if not args.quiet:
                _log(f"Writing to Delta tables: `{_cat}`.`{_sch}`...")
            try:
                export_delta(
                    result, _run_id, _cat, _sch,
                    _delta_host, _delta_token, _delta_warehouse_id,
                    mode=args.delta_mode,
                )
                # Upload report files in parallel
                from concurrent.futures import ThreadPoolExecutor
                def _upload_report(path):
                    ext = Path(path).suffix.lstrip(".")
                    rtype = {"xlsx": "excel", "htm": "html"}.get(ext, ext)
                    export_delta_report(
                        _run_id, ws_name or ws_host, rtype, path,
                        _cat, _sch,
                        _delta_host, _delta_token, _delta_warehouse_id,
                    )
                with ThreadPoolExecutor(max_workers=4) as _pool:
                    list(_pool.map(_upload_report, exported))
                if not args.quiet:
                    _ep_count = len(getattr(result, "endpoint_summary", {}).get("endpoints", []))
                    print(f"  Delta export complete (run_id: {_run_id})")
                    print(f"     -> {_cat}.{_sch}.scan_runs (1 row)")
                    print(f"     -> {_cat}.{_sch}.findings ({len(result.findings)} rows)")
                    print(f"     -> {_cat}.{_sch}.category_scores ({len(result.category_scores)} rows)")
                    print(f"     -> {_cat}.{_sch}.api_endpoints ({_ep_count} rows)")
                    print(f"     -> {_cat}.{_sch}.all_checks ({len(SAT_CHECKS)} rows)")
                    print(f"     -> {_cat}.{_sch}.reports ({len(exported)} files)")

                # Detect and store changes vs previous run
                detect_and_store_changes(
                    result, _run_id, _cat, _sch,
                    _delta_host, _delta_token, _delta_warehouse_id,
                    quiet=args.quiet,
                )

                # Retention cleanup
                if args.delta_retain > 0:
                    cleanup_old_runs(
                        _cat, _sch, _delta_host, _delta_token, _delta_warehouse_id,
                        key_column="workspace_name",
                        retain=args.delta_retain,
                        quiet=args.quiet,
                    )
            except Exception as e:
                print(f"  Delta export failed: {e}")
                print(f"     File exports were still saved to {ws_dir}/")
                print()

        # ── Auto-validate after scan ──
        if auto_validate and _json_path:
            validate_report(_json_path)

    if args.check_only:
        sys.exit(0)

    # ── Combined summary for multi-workspace scans ──
    successful = [(name, r) for name, r in scan_results if r is not None]
    if len(successful) > 1:
        _print_combined_summary(successful, skipped, run_dir, formats, show_scan_items=args.show_scan_items, show_effort=show_effort, show_cost=args.show_cost, report_profile=args.report_profile)

    # ── Lakeview Dashboard (created once after all scans) ──
    if args.dashboard:
        if not args.delta or not _delta_warehouse_id:
            print("\n  --dashboard requires --delta with a working SQL warehouse.")
            sys.exit(1)
        from .dashboard import create_or_update_dashboard
        try:
            dash_url = create_or_update_dashboard(
                args.delta_catalog, args.delta_schema,
                _delta_host, _delta_token, _delta_warehouse_id,
                quiet=args.quiet,
            )
            if not args.quiet:
                _log(f"Dashboard: {dash_url}")
        except Exception as e:
            print(f"\n  Dashboard creation failed: {e}")

    # ── --fail-on: exit non-zero if findings meet severity threshold ──
    if args.fail_on:
        _sev_rank = {"low": 1, "medium": 2, "high": 3}
        _threshold = _sev_rank.get(args.fail_on, 0)
        _fail_statuses = {"FAIL", "WARN"}
        _breaches = 0
        for _, r in scan_results:
            if r is None:
                continue
            for f in r.findings:
                if f.status in _fail_statuses and _sev_rank.get(f.severity, 0) >= _threshold:
                    _breaches += 1
        if _breaches:
            print(f"\n  --fail-on {args.fail_on}: {_breaches} finding(s) at or above '{args.fail_on}' severity.")
            sys.exit(1)


if __name__ == "__main__":
    main()
