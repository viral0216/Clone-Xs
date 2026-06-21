"""SAT Scanner — Unity Catalog inventory orchestrator.

Enumerates the full Unity Catalog object tree (metastore → catalogs → schemas →
tables/views/volumes/functions/registered-models) with low-level detail
(columns, properties, tags, grants) and — when an Azure context is available —
maps it to the backing Azure infrastructure.

This is a sibling to ``scanner.run_scan`` but produces a ``UCInventoryResult``
(an object inventory) rather than PASS/FAIL ``SATFinding`` objects.  It reuses
the existing async HTTP plumbing in ``api`` (retry/backoff, SQL execution).
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any

import httpx

from .api import _dbx_get, _dbx_sql_query, _find_running_warehouse
from .helpers import _log
from .inventory_models import (
    UCColumn, UCGrant, UCTable, UCVolume, UCFunction, UCModel,
    UCSchema, UCCatalog, UCInventoryResult,
)
from .azure_infra import build_azure_inventory, parse_arm_id

_UC = "/api/2.1/unity-catalog"

# Catalogs/schemas skipped by default (opt in with include_system=True)
_SYSTEM_CATALOGS = {"system", "__databricks_internal"}
_SYSTEM_SCHEMAS = {"information_schema"}


# ─────────────────────────────────────────────────────────────────────────────
# Pagination
# ─────────────────────────────────────────────────────────────────────────────

async def _list_all(client: httpx.AsyncClient, host: str, path: str, token: str,
                    params: dict | None, list_key: str, sem: asyncio.Semaphore,
                    max_items: int = 0) -> tuple[list, int, Any]:
    """Follow ``next_page_token``/``has_more`` and return all items for an endpoint.

    Returns ``(items, status, error)``.  On a first-page failure ``items`` is
    empty and ``status``/``error`` describe the failure; later-page failures
    return what was collected so far.
    """
    items: list = []
    page_params = dict(params or {})
    seen_token: str | None = None
    while True:
        async with sem:
            data, status, err = await _dbx_get(client, host, path, token, page_params)
        if data is None:
            return items, status, err
        items.extend(data.get(list_key, []) or [])
        if max_items and len(items) >= max_items:
            return items[:max_items], 200, None
        next_token = data.get("next_page_token") or ""
        if not next_token or data.get("has_more") is False or next_token == seen_token:
            break
        seen_token = next_token
        page_params["page_token"] = next_token
    return items, 200, None


# ─────────────────────────────────────────────────────────────────────────────
# Grants
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_grants(client: httpx.AsyncClient, host: str, token: str,
                        securable_type: str, full_name: str, sem: asyncio.Semaphore,
                        result: UCInventoryResult, effective: bool = False) -> list[UCGrant]:
    base = "effective-permissions" if effective else "permissions"
    path = f"{_UC}/{base}/{securable_type}/{full_name}"
    async with sem:
        data, status, err = await _dbx_get(client, host, path, token)
    if data is None:
        if status not in (404, 0):
            result.record_error(f"grants:{securable_type}", full_name, status, err)
        return []
    grants: list[UCGrant] = []
    for pa in data.get("privilege_assignments", []) or []:
        principal = pa.get("principal", "")
        privs: list[str] = []
        inherited = ""
        for p in pa.get("privileges", []) or []:
            if isinstance(p, dict):
                privs.append(p.get("privilege", ""))
                inherited = inherited or p.get("inherited_from_name", "") or ""
            else:
                privs.append(p)
        grants.append(UCGrant(securable_type, full_name, principal, privs, inherited))
    return grants


def _want_grants(level: str, grants_mode: str) -> bool:
    if grants_mode == "none":
        return False
    if grants_mode == "coarse":
        return level in ("metastore", "catalog", "schema")
    return True  # "table" tier → all levels


# ─────────────────────────────────────────────────────────────────────────────
# Per-object detail fetchers (catalog bindings, model versions, monitors, shares)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_catalog_bindings(client, host, token, catalog_name, sem, result) -> list:
    async with sem:
        data, status, err = await _dbx_get(
            client, host, f"{_UC}/bindings/catalog/{catalog_name}", token)
    if data is None:
        if status not in (404, 0):
            result.record_error("catalog:bindings", catalog_name, status, err)
        return []
    return data.get("bindings", []) or data.get("workspaces", []) or []


async def _fetch_model_versions(client, host, token, full_name, sem, result) -> list:
    async with sem:
        data, status, err = await _dbx_get(
            client, host, f"{_UC}/models/{full_name}/versions", token)
    if data is None:
        if status not in (404, 0):
            result.record_error("model:versions", full_name, status, err)
        return []
    return [{"version": v.get("version"), "status": v.get("status", ""),
             "created_at": v.get("created_at"), "run_id": v.get("run_id", ""),
             "comment": v.get("comment", "")}
            for v in (data.get("model_versions", []) or [])]


async def _fetch_monitor(client, host, token, full_name, sem, result):
    async with sem:
        data, status, err = await _dbx_get(client, host, f"{_UC}/tables/{full_name}/monitor", token)
    if data is None:
        if status not in (404, 0):   # 404 = no monitor on this table (expected)
            result.record_error("table:monitor", full_name, status, err)
        return None
    return {"status": data.get("status", ""), "monitor_version": data.get("monitor_version"),
            "output_schema_name": data.get("output_schema_name", ""),
            "assets_dir": data.get("assets_dir", ""), "schedule": data.get("schedule"),
            "profile_metrics_table_name": data.get("profile_metrics_table_name", "")}


async def _fetch_share_objects(client, host, token, share_name, sem) -> list:
    async with sem:
        data, _, _ = await _dbx_get(client, host, f"{_UC}/shares/{share_name}", token)
    return (data or {}).get("objects", []) or []


async def _enrich_monitors(client, host, token, sem, result) -> None:
    """Fetch the Lakehouse Monitoring config for every table (opt-in; one call per table)."""
    tables = [t for c in result.catalogs for s in c.schemas for t in s.tables]

    async def _one(t):
        t.monitor = await _fetch_monitor(client, host, token, t.full_name, sem, result)

    await asyncio.gather(*[_one(t) for t in tables])


# ─────────────────────────────────────────────────────────────────────────────
# Object loaders
# ─────────────────────────────────────────────────────────────────────────────

def _columns_from(tbl: dict) -> list[UCColumn]:
    cols: list[UCColumn] = []
    for c in tbl.get("columns", []) or []:
        cols.append(UCColumn(
            name=c.get("name", ""),
            type_text=c.get("type_text", ""),
            type_name=c.get("type_name", ""),
            position=c.get("position"),
            nullable=c.get("nullable"),
            comment=c.get("comment", ""),
            mask=c.get("mask"),          # UC column mask (inline in /tables)
        ))
    return cols


async def _load_schema(client, host, token, sem, result, catalog_name, sch, opts) -> UCSchema:
    schema_name = sch.get("name", "")
    full_name = f"{catalog_name}.{schema_name}"
    schema = UCSchema(
        full_name=full_name, catalog=catalog_name, name=schema_name,
        owner=sch.get("owner", ""), comment=sch.get("comment", ""),
        properties=sch.get("properties", {}) or {}, tags=sch.get("tags", {}) or {},
    )

    common = {"catalog_name": catalog_name, "schema_name": schema_name}
    tbl_items, t_st, t_err = await _list_all(client, host, f"{_UC}/tables", token, common, "tables", sem,
                                             max_items=opts["max_tables"])
    if t_err and not tbl_items:
        result.record_error("schema:tables", full_name, t_st, t_err)
    vol_items, _, _ = await _list_all(client, host, f"{_UC}/volumes", token, common, "volumes", sem)
    fn_items, _, _ = await _list_all(client, host, f"{_UC}/functions", token, common, "functions", sem)
    mdl_items, m_st, m_err = await _list_all(client, host, f"{_UC}/models", token, common, "registered_models", sem)
    if m_err and m_st not in (404, 0) and not mdl_items:
        result.record_error("schema:models", full_name, m_st, m_err)

    grant_table = _want_grants("table", opts["grants"])

    for t in tbl_items:
        tfn = t.get("full_name", f"{full_name}.{t.get('name','')}")
        tbl = UCTable(
            full_name=tfn, catalog=catalog_name, schema=schema_name, name=t.get("name", ""),
            table_type=t.get("table_type", ""), data_source_format=t.get("data_source_format", ""),
            storage_location=t.get("storage_location", ""), owner=t.get("owner", ""),
            comment=t.get("comment", ""), created_at=t.get("created_at"),
            updated_at=t.get("updated_at"), view_definition=t.get("view_definition", ""),
            properties=t.get("properties", {}) or {}, tags=t.get("tags", {}) or {},
            columns=_columns_from(t),
            constraints=t.get("table_constraints", []) or [],   # PK/FK/CHECK (inline in /tables)
            row_filter=t.get("row_filter"),                     # row-level security (inline)
        )
        if grant_table:
            tbl.grants = await _fetch_grants(client, host, token, "table", tfn, sem, result,
                                             opts["effective_grants"])
        schema.tables.append(tbl)

    for v in vol_items:
        vfn = v.get("full_name", f"{full_name}.{v.get('name','')}")
        vol = UCVolume(
            full_name=vfn, catalog=catalog_name, schema=schema_name, name=v.get("name", ""),
            volume_type=v.get("volume_type", ""), storage_location=v.get("storage_location", ""),
            owner=v.get("owner", ""), comment=v.get("comment", ""), tags=v.get("tags", {}) or {},
        )
        if grant_table:
            vol.grants = await _fetch_grants(client, host, token, "volume", vfn, sem, result,
                                             opts["effective_grants"])
        schema.volumes.append(vol)

    for fn in fn_items:
        ffn = fn.get("full_name", f"{full_name}.{fn.get('name','')}")
        func = UCFunction(
            full_name=ffn, catalog=catalog_name, schema=schema_name, name=fn.get("name", ""),
            data_type=fn.get("data_type", ""), routine_body=fn.get("routine_body", ""),
            owner=fn.get("owner", ""), comment=fn.get("comment", ""),
        )
        if grant_table:
            func.grants = await _fetch_grants(client, host, token, "function", ffn, sem, result,
                                              opts["effective_grants"])
        schema.functions.append(func)

    for md in mdl_items:
        mfn = md.get("full_name", f"{full_name}.{md.get('name','')}")
        model = UCModel(
            full_name=mfn, catalog=catalog_name, schema=schema_name, name=md.get("name", ""),
            owner=md.get("owner", ""), comment=md.get("comment", ""),
        )
        if grant_table:
            model.grants = await _fetch_grants(client, host, token, "function", mfn, sem, result,
                                               opts["effective_grants"])
        model.versions = await _fetch_model_versions(client, host, token, mfn, sem, result)
        schema.models.append(model)

    if _want_grants("schema", opts["grants"]):
        schema.grants = await _fetch_grants(client, host, token, "schema", full_name, sem, result,
                                            opts["effective_grants"])
    return schema


async def _load_catalog(client, host, token, sem, result, cat, opts) -> UCCatalog:
    catalog_name = cat.get("name", "")
    catalog = UCCatalog(
        name=catalog_name, catalog_type=cat.get("catalog_type", ""),
        owner=cat.get("owner", ""), comment=cat.get("comment", ""),
        storage_root=cat.get("storage_root", ""), isolation_mode=cat.get("isolation_mode", ""),
        properties=cat.get("properties", {}) or {}, tags=cat.get("tags", {}) or {},
    )

    sch_items, s_st, s_err = await _list_all(
        client, host, f"{_UC}/schemas", token, {"catalog_name": catalog_name},
        "schemas", sem, max_items=opts["max_schemas"])
    if s_err and not sch_items:
        result.record_error("catalog:schemas", catalog_name, s_st, s_err)

    schemas_to_load = [
        s for s in sch_items
        if opts["include_system"] or s.get("name", "") not in _SYSTEM_SCHEMAS
    ]
    loaded = await asyncio.gather(*[
        _load_schema(client, host, token, sem, result, catalog_name, s, opts)
        for s in schemas_to_load
    ])
    catalog.schemas = list(loaded)

    if _want_grants("catalog", opts["grants"]):
        catalog.grants = await _fetch_grants(client, host, token, "catalog", catalog_name, sem, result,
                                             opts["effective_grants"])
    catalog.bindings = await _fetch_catalog_bindings(client, host, token, catalog_name, sem, result)
    return catalog


# ─────────────────────────────────────────────────────────────────────────────
# Tags via information_schema (optional, bulk)
# ─────────────────────────────────────────────────────────────────────────────

async def _enrich_tags_sql(client, host, token, warehouse_id, result: UCInventoryResult) -> None:
    """Best-effort: attach tags from system.information_schema for each catalog."""
    if not warehouse_id:
        result.record_error("tags_sql", "", 0, "no SQL warehouse available for information_schema tags")
        return
    for catalog in result.catalogs:
        cname = catalog.name
        # table tags
        rows, err = await _dbx_sql_query(
            client, host, token, warehouse_id,
            f"SELECT schema_name, table_name, tag_name, tag_value "
            f"FROM `{cname}`.information_schema.table_tags")
        if err:
            result.record_error("tags_sql:table", cname, 0, err)
            continue
        tbl_index = {t.full_name: t for s in catalog.schemas for t in s.tables}
        for r in rows or []:
            schema_name, table_name, tag_name, tag_value = (r + ["", "", "", ""])[:4]
            t = tbl_index.get(f"{cname}.{schema_name}.{table_name}")
            if t is not None and tag_name:
                t.tags[tag_name] = tag_value
        # column tags
        crows, cerr = await _dbx_sql_query(
            client, host, token, warehouse_id,
            f"SELECT schema_name, table_name, column_name, tag_name, tag_value "
            f"FROM `{cname}`.information_schema.column_tags")
        if not cerr:
            for r in crows or []:
                schema_name, table_name, column_name, tag_name, tag_value = (r + ["", "", "", "", ""])[:5]
                t = tbl_index.get(f"{cname}.{schema_name}.{table_name}")
                if t is not None and tag_name:
                    for col in t.columns:
                        if col.name == column_name:
                            col.tags[tag_name] = tag_value
                            break


# ─────────────────────────────────────────────────────────────────────────────
# Stats
# ─────────────────────────────────────────────────────────────────────────────

def _compute_stats(result: UCInventoryResult) -> dict:
    n_schema = n_tab = n_view = n_vol = n_fn = n_mdl = n_col = n_grant = 0
    n_cons = n_mask = n_rowfilter = n_mver = n_monitor = n_bind = 0
    for c in result.catalogs:
        n_grant += len(c.grants)
        n_bind += len(c.bindings)
        for s in c.schemas:
            n_schema += 1
            n_grant += len(s.grants)
            for t in s.tables:
                if t.table_type == "VIEW":
                    n_view += 1
                else:
                    n_tab += 1
                n_col += len(t.columns)
                n_grant += len(t.grants)
                n_cons += len(t.constraints)
                n_mask += sum(1 for col in t.columns if col.mask)
                if t.row_filter:
                    n_rowfilter += 1
                if t.monitor:
                    n_monitor += 1
            n_vol += len(s.volumes)
            n_fn += len(s.functions)
            n_mdl += len(s.models)
            n_mver += sum(len(m.versions) for m in s.models)
            n_grant += sum(len(v.grants) for v in s.volumes)
            n_grant += sum(len(f.grants) for f in s.functions)
            n_grant += sum(len(m.grants) for m in s.models)
    n_grant += len(result.metastore_grants)
    return {
        "catalogs": len(result.catalogs), "schemas": n_schema,
        "tables": n_tab, "views": n_view, "volumes": n_vol,
        "functions": n_fn, "registered_models": n_mdl, "model_versions": n_mver,
        "columns": n_col, "grants": n_grant,
        "constraints": n_cons, "masked_columns": n_mask, "row_filters": n_rowfilter,
        "monitored_tables": n_monitor, "catalog_bindings": n_bind,
        "external_locations": len(result.external_locations),
        "storage_credentials": len(result.storage_credentials),
        "service_credentials": len(result.service_credentials),
        "connections": len(result.connections),
        "shares": len(result.shares), "recipients": len(result.recipients),
        "providers": len(result.providers),
        "errors": len(result.errors),
    }


# ─────────────────────────────────────────────────────────────────────────────
# System-tables (information_schema) enumeration
# ─────────────────────────────────────────────────────────────────────────────

_IS = "system.information_schema"

# Internal/hidden tables that information_schema exposes but the UC REST API hides:
# materialized-view / DLT backing tables (``__materialization_...``) and DLT pipeline
# event logs (``event_log_<hex>``). Filtered by default; included with include_system.
_INTERNAL_TABLE_RE = re.compile(r"^(__|event_log_[0-9a-f]{8})", re.IGNORECASE)


def _pad(row: list, n: int) -> list:
    return (list(row) + [None] * n)[:n]


def _int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _catalog_obj(cat: dict) -> UCCatalog:
    return UCCatalog(
        name=cat.get("name", ""), catalog_type=cat.get("catalog_type", ""),
        owner=cat.get("owner", ""), comment=cat.get("comment", ""),
        storage_root=cat.get("storage_root", ""), isolation_mode=cat.get("isolation_mode", ""),
        properties=cat.get("properties", {}) or {}, tags=cat.get("tags", {}) or {})


async def _enumerate_via_sql(client, host, token, warehouse_id, result: UCInventoryResult,
                             cat_objs: dict, opts: dict, catalog_filter: set,
                             include_system: bool) -> None:
    """Populate the catalog tree from system.information_schema via bulk SQL queries.

    One query per object type for the whole metastore — far fewer calls than the
    REST API, so it avoids UC rate limits on large metastores.  ``cat_objs`` is a
    pre-built {name: UCCatalog} from the API ``/catalogs`` metadata.
    """
    def cat_pred(col: str) -> str:
        if catalog_filter:
            names = ",".join("'" + n.replace("'", "") + "'" for n in catalog_filter)
            return f"WHERE {col} IN ({names})"
        if not include_system:
            skip = ",".join("'" + s + "'" for s in sorted(_SYSTEM_CATALOGS | {"hive_metastore", "samples"}))
            return f"WHERE {col} NOT IN ({skip})"
        return ""

    async def _q(sql: str, label: str) -> list:
        rows, err = await _dbx_sql_query(client, host, token, warehouse_id, sql, timeout=120)
        if err:
            result.record_error(f"sql:{label}", "", 0, err)
            return []
        return rows or []

    schema_objs: dict[str, UCSchema] = {}
    table_objs: dict[str, UCTable] = {}

    # ── Schemas ──
    for row in await _q(
        f"SELECT catalog_name, schema_name, schema_owner, comment FROM {_IS}.schemata "
        f"{cat_pred('catalog_name')}", "schemata"):
        cat, sch, owner, comment = _pad(row, 4)
        if not include_system and sch == "information_schema":
            continue
        c = cat_objs.get(cat)
        if c is None:
            continue
        so = UCSchema(full_name=f"{cat}.{sch}", catalog=cat, name=sch,
                      owner=owner or "", comment=comment or "")
        c.schemas.append(so)
        schema_objs[so.full_name] = so

    # ── Tables / Views ──
    for row in await _q(
        f"SELECT table_catalog, table_schema, table_name, table_type, data_source_format, "
        f"storage_path, table_owner, comment, created, last_altered FROM {_IS}.tables "
        f"{cat_pred('table_catalog')}", "tables"):
        cat, sch, name, ttype, fmt, spath, owner, comment, created, altered = _pad(row, 10)
        so = schema_objs.get(f"{cat}.{sch}")
        if so is None:
            continue
        if not include_system and _INTERNAL_TABLE_RE.match(name or ""):
            continue  # hidden materialization / DLT event-log table (API also hides these)
        to = UCTable(full_name=f"{cat}.{sch}.{name}", catalog=cat, schema=sch, name=name,
                     table_type=ttype or "", data_source_format=fmt or "",
                     storage_location=spath or "", owner=owner or "", comment=comment or "",
                     created_at=created, updated_at=altered)
        so.tables.append(to)
        table_objs[to.full_name] = to

    # ── Columns ──
    for row in await _q(
        f"SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, "
        f"full_data_type, data_type, is_nullable, comment FROM {_IS}.columns "
        f"{cat_pred('table_catalog')}", "columns"):
        cat, sch, tab, cname, pos, ftype, dtype, nullable, comment = _pad(row, 9)
        to = table_objs.get(f"{cat}.{sch}.{tab}")
        if to is None:
            continue
        to.columns.append(UCColumn(
            name=cname or "", type_text=ftype or "", type_name=dtype or "",
            position=_int(pos), nullable=(str(nullable).upper() == "YES"), comment=comment or ""))

    # ── Volumes ──
    for row in await _q(
        f"SELECT volume_catalog, volume_schema, volume_name, volume_type, storage_location, "
        f"volume_owner, comment FROM {_IS}.volumes {cat_pred('volume_catalog')}", "volumes"):
        cat, sch, name, vtype, vloc, owner, comment = _pad(row, 7)
        so = schema_objs.get(f"{cat}.{sch}")
        if so is not None:
            so.volumes.append(UCVolume(
                full_name=f"{cat}.{sch}.{name}", catalog=cat, schema=sch, name=name,
                volume_type=vtype or "", storage_location=vloc or "", owner=owner or "",
                comment=comment or ""))

    # ── Functions (routines) ──
    for row in await _q(
        f"SELECT routine_catalog, routine_schema, routine_name, data_type, routine_body, "
        f"routine_owner, comment FROM {_IS}.routines {cat_pred('routine_catalog')}", "routines"):
        cat, sch, name, dtype, body, owner, comment = _pad(row, 7)
        so = schema_objs.get(f"{cat}.{sch}")
        if so is not None:
            so.functions.append(UCFunction(
                full_name=f"{cat}.{sch}.{name}", catalog=cat, schema=sch, name=name,
                data_type=dtype or "", routine_body=body or "", owner=owner or "",
                comment=comment or ""))

    # ── Grants (privileges) ──
    if opts["grants"] != "none":
        await _sql_grants(_q, cat_objs, schema_objs, table_objs, cat_pred, opts["grants"])

    # ── Tags ──
    await _sql_tags(_q, cat_objs, schema_objs, table_objs, cat_pred)

    # ── Constraints + row filters + column masks ──
    await _sql_constraints(_q, table_objs, cat_pred)
    await _sql_masking(_q, table_objs, cat_pred)

    # ── Catalog ↔ workspace bindings (API per catalog — not in information_schema) ──
    bsem = asyncio.Semaphore(8)
    for cobj in cat_objs.values():
        cobj.bindings = await _fetch_catalog_bindings(client, host, token, cobj.name, bsem, result)


async def _sql_constraints(_q, table_objs, cat_pred) -> None:
    cons: dict[tuple, dict] = {}
    for row in await _q(
        f"SELECT table_catalog, table_schema, table_name, constraint_name, constraint_type "
        f"FROM {_IS}.table_constraints {cat_pred('table_catalog')}", "table_constraints"):
        cat, sch, tab, cname, ctype = _pad(row, 5)
        fqn = f"{cat}.{sch}.{tab}"
        if fqn in table_objs:
            cons[(fqn, cname)] = {"name": cname, "type": ctype, "columns": []}
    for row in await _q(
        f"SELECT table_catalog, table_schema, table_name, constraint_name, column_name "
        f"FROM {_IS}.key_column_usage {cat_pred('table_catalog')}", "key_column_usage"):
        cat, sch, tab, cname, col = _pad(row, 5)
        key = (f"{cat}.{sch}.{tab}", cname)
        if key in cons and col:
            cons[key]["columns"].append(col)
    for (fqn, _), c in cons.items():
        table_objs[fqn].constraints.append(c)


async def _sql_masking(_q, table_objs, cat_pred) -> None:
    for row in await _q(
        f"SELECT table_catalog, table_schema, table_name, filter_name "
        f"FROM {_IS}.row_filters {cat_pred('table_catalog')}", "row_filters"):
        cat, sch, tab, fname = _pad(row, 4)
        to = table_objs.get(f"{cat}.{sch}.{tab}")
        if to is not None:
            to.row_filter = {"function_name": fname}
    for row in await _q(
        f"SELECT table_catalog, table_schema, table_name, column_name, mask_name "
        f"FROM {_IS}.column_masks {cat_pred('table_catalog')}", "column_masks"):
        cat, sch, tab, col, mname = _pad(row, 5)
        to = table_objs.get(f"{cat}.{sch}.{tab}")
        if to is not None:
            for c in to.columns:
                if c.name == col:
                    c.mask = {"function_name": mname}
                    break


def _aggregate_grants(rows: list, n_parts: int, securable_type: str) -> dict[str, list[UCGrant]]:
    """Group privilege rows (one privilege per row) into UCGrant objects per securable."""
    acc: dict[tuple, UCGrant] = {}
    for row in rows:
        parts = _pad(row, n_parts + 3)
        names = parts[:n_parts]
        grantee, priv, inherited = parts[n_parts], parts[n_parts + 1], parts[n_parts + 2]
        full = ".".join(p for p in names if p)
        key = (full, grantee)
        g = acc.get(key)
        if g is None:
            g = UCGrant(securable_type, full, grantee or "", [], inherited or "")
            acc[key] = g
        if priv and priv not in g.privileges:
            g.privileges.append(priv)
    by_obj: dict[str, list[UCGrant]] = {}
    for (full, _), g in acc.items():
        by_obj.setdefault(full, []).append(g)
    return by_obj


async def _sql_grants(_q, cat_objs, schema_objs, table_objs, cat_pred, grants_mode) -> None:
    cat_g = _aggregate_grants(await _q(
        f"SELECT catalog_name, grantee, privilege_type, inherited_from "
        f"FROM {_IS}.catalog_privileges {cat_pred('catalog_name')}", "catalog_privileges"),
        1, "catalog")
    for full, glist in cat_g.items():
        if full in cat_objs:
            cat_objs[full].grants.extend(glist)

    sch_g = _aggregate_grants(await _q(
        f"SELECT catalog_name, schema_name, grantee, privilege_type, inherited_from "
        f"FROM {_IS}.schema_privileges {cat_pred('catalog_name')}", "schema_privileges"),
        2, "schema")
    for full, glist in sch_g.items():
        if full in schema_objs:
            schema_objs[full].grants.extend(glist)

    if grants_mode == "table":
        tab_g = _aggregate_grants(await _q(
            f"SELECT table_catalog, table_schema, table_name, grantee, privilege_type, inherited_from "
            f"FROM {_IS}.table_privileges {cat_pred('table_catalog')}", "table_privileges"),
            3, "table")
        for full, glist in tab_g.items():
            if full in table_objs:
                table_objs[full].grants.extend(glist)


async def _sql_tags(_q, cat_objs, schema_objs, table_objs, cat_pred) -> None:
    for row in await _q(
        f"SELECT catalog_name, tag_name, tag_value FROM {_IS}.catalog_tags "
        f"{cat_pred('catalog_name')}", "catalog_tags"):
        cat, k, v = _pad(row, 3)
        if cat in cat_objs and k:
            cat_objs[cat].tags[k] = v
    for row in await _q(
        f"SELECT catalog_name, schema_name, tag_name, tag_value FROM {_IS}.schema_tags "
        f"{cat_pred('catalog_name')}", "schema_tags"):
        cat, sch, k, v = _pad(row, 4)
        so = schema_objs.get(f"{cat}.{sch}")
        if so is not None and k:
            so.tags[k] = v
    for row in await _q(
        f"SELECT catalog_name, schema_name, table_name, tag_name, tag_value FROM {_IS}.table_tags "
        f"{cat_pred('catalog_name')}", "table_tags"):
        cat, sch, tab, k, v = _pad(row, 5)
        to = table_objs.get(f"{cat}.{sch}.{tab}")
        if to is not None and k:
            to.tags[k] = v
    for row in await _q(
        f"SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value "
        f"FROM {_IS}.column_tags {cat_pred('catalog_name')}", "column_tags"):
        cat, sch, tab, col, k, v = _pad(row, 6)
        to = table_objs.get(f"{cat}.{sch}.{tab}")
        if to is not None and k:
            for c in to.columns:
                if c.name == col:
                    c.tags[k] = v
                    break


async def _augment_with_api(client, host, token, sem, result: UCInventoryResult,
                            cat_objs: dict, catalogs_to_load: list, opts: dict) -> None:
    """Fill gaps the SQL path can't cover: registered models, and catalogs with no
    information_schema rows (FOREIGN / DELTASHARING)."""
    raw_by_name = {c.get("name", ""): c for c in catalogs_to_load}
    for name, cobj in cat_objs.items():
        for sch in cobj.schemas:
            mdl_items, m_st, m_err = await _list_all(
                client, host, f"{_UC}/models", token,
                {"catalog_name": name, "schema_name": sch.name}, "registered_models", sem)
            if m_err and m_st not in (404, 0) and not mdl_items:
                result.record_error("schema:models", sch.full_name, m_st, m_err)
            for md in mdl_items:
                mfn = md.get("full_name", f"{sch.full_name}.{md.get('name','')}")
                sch.models.append(UCModel(
                    full_name=mfn, catalog=name, schema=sch.name, name=md.get("name", ""),
                    owner=md.get("owner", ""), comment=md.get("comment", ""),
                    versions=await _fetch_model_versions(client, host, token, mfn, sem, result)))
    # Catalogs SQL returned nothing for (foreign / delta-sharing) → enumerate via API
    for name in list(cat_objs.keys()):
        if not cat_objs[name].schemas and name in raw_by_name:
            cat_objs[name] = await _load_catalog(
                client, host, token, sem, result, raw_by_name[name], opts)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def run_inventory(
    host: str,
    token: str,
    workspace_name: str = "",
    quiet: bool = False,
    *,
    concurrency: int = 5,
    catalogs: list[str] | None = None,
    include_system: bool = False,
    grants: str = "coarse",
    effective_grants: bool = False,
    tags_sql: bool = False,
    warehouse_id: str = "",
    max_catalogs: int = 0,
    max_schemas_per_catalog: int = 0,
    max_tables_per_schema: int = 0,
    skip_azure: bool = False,
    source: str = "api",
    monitors: bool = False,
) -> UCInventoryResult:
    """Enumerate Unity Catalog objects (and Azure infra) for one workspace.

    ``source`` selects the enumeration backend:
      - ``api``  — UC REST API (default).
      - ``sql``  — system.information_schema bulk queries (needs a SQL warehouse;
                   fewest calls → rate-limit friendly).
      - ``both`` — SQL for the bulk tree + API to fill gaps (registered models,
                   FOREIGN/DELTASHARING catalogs).
    """
    host = host.rstrip("/")
    source = "api" if source == "auto" else source   # single-workspace default is API
    result = UCInventoryResult(host, workspace_name, datetime.now().isoformat())
    sem = asyncio.Semaphore(max(1, concurrency))
    catalog_filter = {c.strip() for c in (catalogs or []) if c.strip()}
    opts = {
        "include_system": include_system, "grants": grants,
        "effective_grants": effective_grants, "monitors": monitors,
        "max_schemas": max_schemas_per_catalog, "max_tables": max_tables_per_schema,
    }

    async with httpx.AsyncClient(timeout=45) as client:
        await _enumerate_metastore_scoped(
            client, host, token, result, sem,
            source=source, warehouse_id=warehouse_id, catalog_filter=catalog_filter,
            include_system=include_system, opts=opts, tags_sql=tags_sql,
            monitors=monitors, max_catalogs=max_catalogs, quiet=quiet)
        await _enumerate_azure_for(client, host, token, result,
                                   skip_azure=skip_azure, quiet=quiet)

    result.stats = _compute_stats(result)
    if not quiet:
        s = result.stats
        who = f" [{result.workspace_name}]" if result.workspace_name else ""
        _log(f"Inventory complete{who}: {s['catalogs']} catalogs, {s['schemas']} schemas, "
             f"{s['tables']} tables, {s['columns']} columns, {s['grants']} grants")
    return result


async def _enumerate_metastore_scoped(
    client, host, token, result, sem, *, source, warehouse_id,
    catalog_filter, include_system, opts, tags_sql, monitors, max_catalogs, quiet,
) -> tuple[str, dict]:
    """Enumerate the **metastore-scoped** UC tree (metastore + securables +
    catalogs → schemas → objects + tags/monitors) onto ``result``.

    Returns ``(metastore_id, {catalog_name: UCCatalog})`` — the catalog cache the
    multi-workspace dedup path reuses. Behaviour is identical to the original
    inline block in :func:`run_inventory`; ``source`` is already resolved (no
    ``auto``).
    """
    grants = opts["grants"]
    effective_grants = opts["effective_grants"]

    # ── Metastore ──
    ms_data, _, _ = await _dbx_get(client, host, f"{_UC}/metastores", token)
    metastores = (ms_data or {}).get("metastores", []) if ms_data else []
    cur, _, _ = await _dbx_get(client, host, f"{_UC}/current-metastore-assignment", token)
    result.metastore = {
        "current_assignment": cur or {},
        "metastores": metastores,
    }
    metastore_id = (cur or {}).get("metastore_id", "") or (
        metastores[0].get("metastore_id", "") if metastores else "")
    if metastore_id and _want_grants("metastore", grants):
        result.metastore_grants = await _fetch_grants(
            client, host, token, "metastore", metastore_id, sem, result, effective_grants)

    # ── Metastore-level securables ──
    result.external_locations, _, _ = await _list_all(
        client, host, f"{_UC}/external-locations", token, None, "external_locations", sem)
    result.storage_credentials, _, _ = await _list_all(
        client, host, f"{_UC}/storage-credentials", token, None, "storage_credentials", sem)
    result.connections, _, _ = await _list_all(
        client, host, f"{_UC}/connections", token, None, "connections", sem)
    result.shares, _, _ = await _list_all(
        client, host, f"{_UC}/shares", token, None, "shares", sem)
    result.recipients, _, _ = await _list_all(
        client, host, f"{_UC}/recipients", token, None, "recipients", sem)
    result.providers, _, _ = await _list_all(
        client, host, f"{_UC}/providers", token, None, "providers", sem)
    # Service (non-storage) credentials — newer unified credentials API; 404 on older workspaces
    sc_data, _, _ = await _dbx_get(client, host, f"{_UC}/credentials", token,
                                   {"purpose": "SERVICE"})
    result.service_credentials = (sc_data or {}).get("credentials", []) if sc_data else []
    # Enrich each outbound share with the objects it shares
    for sh in result.shares:
        sh["objects"] = await _fetch_share_objects(client, host, token, sh.get("name", ""), sem)

    # ── Catalogs → schemas → objects ──
    cat_items, c_st, c_err = await _list_all(
        client, host, f"{_UC}/catalogs", token, None, "catalogs", sem, max_items=max_catalogs)
    if c_err and not cat_items:
        result.record_error("catalogs", "", c_st, c_err)

    catalogs_to_load = []
    for cat in cat_items:
        name = cat.get("name", "")
        if catalog_filter:
            if name not in catalog_filter:
                continue
        elif not include_system and (name in _SYSTEM_CATALOGS or name == "hive_metastore"):
            continue
        catalogs_to_load.append(cat)

    if not quiet:
        _log(f"Inventory ({source}): {len(catalogs_to_load)} catalog(s), "
             f"{len(result.external_locations)} external location(s)")

    async def _load_via_api():
        loaded = await asyncio.gather(*[
            _load_catalog(client, host, token, sem, result, cat, opts)
            for cat in catalogs_to_load])
        result.catalogs = list(loaded)

    cat_cache: dict = {}
    if source == "api":
        await _load_via_api()
        cat_cache = {c.name: c for c in result.catalogs}
    else:
        wh = warehouse_id or (await _find_running_warehouse(client, host, token) or "")
        if not wh:
            result.record_error("inventory_source", "", 0,
                f"source={source} needs a SQL warehouse but none is available — using API")
            await _load_via_api()
            cat_cache = {c.name: c for c in result.catalogs}
        else:
            cat_objs = {c.get("name", ""): _catalog_obj(c) for c in catalogs_to_load}
            await _enumerate_via_sql(client, host, token, wh, result, cat_objs,
                                     opts, catalog_filter, include_system)
            if source == "both":
                await _augment_with_api(client, host, token, sem, result,
                                        cat_objs, catalogs_to_load, opts)
            result.catalogs = list(cat_objs.values())
            cat_cache = cat_objs

    # ── Optional tag enrichment via information_schema (API source only;
    #    the SQL source already loads tags as part of its bulk queries) ──
    if tags_sql and source == "api":
        wh = warehouse_id or (await _find_running_warehouse(client, host, token) or "")
        await _enrich_tags_sql(client, host, token, wh, result)

    # ── Optional: Lakehouse Monitoring config per table (one call per table) ──
    if monitors:
        await _enrich_monitors(client, host, token, sem, result)

    return metastore_id, cat_cache


async def _enumerate_azure_for(client, host, token, result, *, skip_azure, quiet) -> None:
    """Enumerate the **workspace-specific** Azure infra + UC↔Azure mapping onto
    ``result`` — the only per-workspace part of an inventory."""
    if skip_azure:
        return
    metastores = (result.metastore or {}).get("metastores", []) or []
    secret_scopes: list[dict] = []
    sd, _, _ = await _dbx_get(client, host, "/api/2.0/secrets/scopes/list", token)
    if sd:
        secret_scopes = sd.get("scopes", []) or []
    azure_inv = await build_azure_inventory(
        client, host, result.external_locations, result.storage_credentials,
        metastores, secret_scopes)
    result.azure = azure_inv.to_dict()
    _denormalize_azure(result, azure_inv)
    # Derive the Databricks workspace name from the resolved ARM resource when the
    # caller didn't supply one (e.g. --host with existing az login).
    if not result.workspace_name and azure_inv.available and azure_inv.workspace:
        ids = parse_arm_id(azure_inv.workspace.resource_id) or {}
        if ids.get("name"):
            result.workspace_name = ids["name"]
    if not quiet:
        if azure_inv.available:
            _log(f"Azure: {len(azure_inv.storage_accounts)} storage account(s), "
                 f"{len(azure_inv.mappings)} mapping(s)")
        else:
            _log(f"Azure discovery skipped — {azure_inv.reason}")


def _denormalize_azure(result: UCInventoryResult, azure_inv) -> None:
    """Attach each external location's Azure mapping onto its raw payload node."""
    by_name = {
        m.uc_name: m for m in azure_inv.mappings
        if m.uc_object_type == "external_location"
    }
    for loc in result.external_locations:
        m = by_name.get(loc.get("name", ""))
        if m is not None:
            loc["azure"] = m.to_dict()


# ─────────────────────────────────────────────────────────────────────────────
# Source comparison (api vs sql)
# ─────────────────────────────────────────────────────────────────────────────

def _index_inventory(inv: UCInventoryResult) -> dict:
    """Flatten an inventory into sets/maps keyed by full name for diffing."""
    idx = {"tables": {}, "columns": {}, "schemas": set(), "volumes": set(),
           "functions": set(), "models": set(), "grants": set()}
    for c in inv.catalogs:
        for g in c.grants:
            idx["grants"].add((g.securable_type, g.full_name, g.principal))
        for s in c.schemas:
            idx["schemas"].add(s.full_name)
            for g in s.grants:
                idx["grants"].add((g.securable_type, g.full_name, g.principal))
            for t in s.tables:
                idx["tables"][t.full_name] = t.table_type
                idx["columns"][t.full_name] = len(t.columns)
                for g in t.grants:
                    idx["grants"].add((g.securable_type, g.full_name, g.principal))
            for v in s.volumes:
                idx["volumes"].add(v.full_name)
            for f in s.functions:
                idx["functions"].add(f.full_name)
            for m in s.models:
                idx["models"].add(m.full_name)
    return idx


def compare_inventories(api_inv: UCInventoryResult, sql_inv: UCInventoryResult,
                        sample: int = 100) -> dict:
    """Diff two inventories of the same workspace produced by different sources.

    Returns a structured report: per-type counts (with match flag), set
    differences (capped to ``sample`` examples each), and per-table column-count
    mismatches.  Source-independent sections (Azure) are not compared.
    """
    a, s = _index_inventory(api_inv), _index_inventory(sql_inv)

    def _set_diff(sa, sb) -> dict:
        only_a, only_b = sorted(sa - sb), sorted(sb - sa)
        return {
            "api_count": len(sa), "sql_count": len(sb), "match": sa == sb,
            "only_in_api": [list(x) if isinstance(x, tuple) else x for x in only_a[:sample]],
            "only_in_sql": [list(x) if isinstance(x, tuple) else x for x in only_b[:sample]],
            "only_in_api_total": len(only_a), "only_in_sql_total": len(only_b),
        }

    a_tabs, s_tabs = set(a["tables"]), set(s["tables"])
    common = a_tabs & s_tabs
    col_mm = [{"table": t, "api": a["columns"][t], "sql": s["columns"][t]}
              for t in sorted(common) if a["columns"][t] != s["columns"][t]]

    counts = {
        "catalogs": {"api": len(api_inv.catalogs), "sql": len(sql_inv.catalogs)},
        "schemas": {"api": len(a["schemas"]), "sql": len(s["schemas"])},
        "tables": {"api": len(a_tabs), "sql": len(s_tabs)},
        "columns": {"api": sum(a["columns"].values()), "sql": sum(s["columns"].values())},
        "volumes": {"api": len(a["volumes"]), "sql": len(s["volumes"])},
        "functions": {"api": len(a["functions"]), "sql": len(s["functions"])},
        "registered_models": {"api": len(a["models"]), "sql": len(s["models"])},
        "grants": {"api": len(a["grants"]), "sql": len(s["grants"])},
    }
    for v in counts.values():
        v["match"] = v["api"] == v["sql"]

    return {
        "workspace_url": api_inv.workspace_url,
        "workspace_name": api_inv.workspace_name or sql_inv.workspace_name,
        "counts": counts,
        "differences": {
            "tables": _set_diff(a_tabs, s_tabs),
            "schemas": _set_diff(a["schemas"], s["schemas"]),
            "volumes": _set_diff(a["volumes"], s["volumes"]),
            "functions": _set_diff(a["functions"], s["functions"]),
            "registered_models": _set_diff(a["models"], s["models"]),
            "grants": _set_diff(a["grants"], s["grants"]),
        },
        "column_count_mismatches": col_mm[:sample],
        "column_count_mismatch_total": len(col_mm),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Multi-workspace: parallel runner + aggregation
# ─────────────────────────────────────────────────────────────────────────────

async def run_inventory_many(
    targets: list[tuple[str, str, str]],
    *,
    workspace_concurrency: int = 3,
    **opts,
) -> list[tuple[str, str, str, "UCInventoryResult | None"]]:
    """Inventory many workspaces concurrently (bounded).

    ``targets`` is a list of ``(host, token, name)``. ``opts`` are forwarded to
    :func:`run_inventory`. Each workspace auto-discovers its own enumeration
    warehouse (do not pass a shared ``warehouse_id``). Returns
    ``[(name, host, token, UCInventoryResult|None)]`` — ``None`` when the
    workspace was unreachable.
    """
    from .scanner import check_connectivity
    sem = asyncio.Semaphore(max(1, workspace_concurrency))

    async def _one(host: str, token: str, name: str):
        host = host.rstrip("/")
        async with sem:
            if not await check_connectivity(host, token):
                _log(f"Cannot reach workspace {name or host}. Skipping.")
                return (name, host, token, None)
            try:
                inv = await run_inventory(host, token, name, **opts)
            except Exception as exc:
                _log(f"Inventory failed for {name or host}: {exc}")
                return (name, host, token, None)
            return (name, host, token, inv)

    return await asyncio.gather(*[_one(h, t, n) for (h, t, n) in targets])


# ─────────────────────────────────────────────────────────────────────────────
# Metastore-level deduplication for multi-workspace runs
#   Many workspaces can share one UC metastore, so the (metastore-scoped) catalog
#   tree is identical across them — enumerate it ONCE per metastore and reuse it;
#   only the Azure infra block is genuinely per-workspace.
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_source(source: str, *, prefer_sql: bool) -> str:
    """Resolve the ``auto`` source. For the once-per-metastore scan we prefer the
    metastore-wide ``sql`` (system.information_schema) backend; the enumerator
    gracefully falls back to ``api`` if no SQL warehouse is available."""
    if source != "auto":
        return source
    return "sql" if prefer_sql else "api"


async def _probe_metastore(host: str, token: str, sem: asyncio.Semaphore) -> dict:
    """Cheaply identify a workspace's metastore + visible catalog names.

    One or two light API calls; doubles as a reachability check. Returns
    ``{host, metastore_id, catalog_names: set[str], reachable: bool}``.
    """
    host = host.rstrip("/")
    out = {"host": host, "metastore_id": "", "catalog_names": set(), "reachable": False}
    async with httpx.AsyncClient(timeout=30) as client:
        cur, status, _ = await _dbx_get(
            client, host, f"{_UC}/current-metastore-assignment", token)
        if status == 0:                       # connect error / timeout → unreachable
            return out
        out["reachable"] = True
        mid = (cur or {}).get("metastore_id", "")
        if not mid:                           # fall back to the metastores list
            ms_data, _, _ = await _dbx_get(client, host, f"{_UC}/metastores", token)
            metastores = (ms_data or {}).get("metastores", []) if ms_data else []
            mid = metastores[0].get("metastore_id", "") if metastores else ""
        out["metastore_id"] = mid or ""
        cats, _, _ = await _list_all(
            client, host, f"{_UC}/catalogs", token, None, "catalogs", sem)
        out["catalog_names"] = {c.get("name", "") for c in cats if c.get("name")}
    return out


async def _probe_all(targets: list, *, concurrency: int) -> dict:
    """Probe every target's metastore in parallel → ``{host: probe}``."""
    sem = asyncio.Semaphore(max(1, concurrency))

    async def _one(host, token, _name):
        return await _probe_metastore(host, token, sem)

    probes = await asyncio.gather(*[_one(h, t, n) for (h, t, n) in targets])
    return {p["host"]: p for p in probes}


def _group_by_metastore(targets: list, probes: dict) -> list[dict]:
    """Group ``(host, token, name)`` targets by metastore id.

    A target with an unknown/empty metastore id (or unreachable) becomes its own
    singleton group, so two workspaces are never merged unless we are certain they
    share a metastore. Leader = first member by host (deterministic).
    """
    groups: dict[str, dict] = {}
    singletons: list[dict] = []
    for host, token, name in targets:
        h = host.rstrip("/")
        p = probes.get(h) or {"metastore_id": "", "catalog_names": set(), "reachable": False}
        member = (h, token, name, set(p.get("catalog_names") or set()))
        mid = p.get("metastore_id", "")
        if not p.get("reachable", False) or not mid:
            singletons.append({"metastore_id": mid, "members": [member], "leader_host": h,
                               "reachable": p.get("reachable", False), "dedup": False})
            continue
        g = groups.setdefault(mid, {"metastore_id": mid, "members": [], "leader_host": h,
                                    "reachable": True, "dedup": False})
        g["members"].append(member)
    out = []
    for g in groups.values():
        g["members"].sort(key=lambda m: m[0])
        g["leader_host"] = g["members"][0][0]
        g["dedup"] = len(g["members"]) > 1
        out.append(g)
    out.extend(singletons)
    return out


async def _enumerate_catalogs_subset(client, host, token, names: set, sem, *,
                                     source, warehouse_id, opts, include_system,
                                     sink: UCInventoryResult) -> dict:
    """Enumerate only the named catalogs from one workspace → ``{name: UCCatalog}``.

    Used by the union cache to pick up catalogs ISOLATED to a non-leader workspace.
    ``sink`` collects any enumeration errors (per-workspace attribution is approximate).
    """
    if not names:
        return {}
    cat_items, _, _ = await _list_all(
        client, host, f"{_UC}/catalogs", token, None, "catalogs", sem)
    subset = [c for c in cat_items if c.get("name", "") in names]
    if not subset:
        return {}
    eff = source
    wh = ""
    if eff != "api":
        wh = warehouse_id or (await _find_running_warehouse(client, host, token) or "")
        if not wh:
            eff = "api"                       # no warehouse → API fallback (same shape)
    if eff == "api":
        loaded = await asyncio.gather(*[
            _load_catalog(client, host, token, sem, sink, cat, opts) for cat in subset])
        return {c.name: c for c in loaded}
    cat_objs = {c.get("name", ""): _catalog_obj(c) for c in subset}
    await _enumerate_via_sql(client, host, token, wh, sink, cat_objs, opts, names, include_system)
    if source == "both":
        await _augment_with_api(client, host, token, sem, sink, cat_objs, subset, opts)
    return cat_objs


async def _inventory_group(group: dict, *, opts: dict) -> list[tuple]:
    """Enumerate one metastore group.

    For a dedup group the metastore-scoped UC tree is enumerated once on the
    leader and reused across members; any catalogs a member can see but the leader
    couldn't (ISOLATED) are enumerated once via a union cache. Azure infra runs
    per workspace. Returns ``[(name, host, token, UCInventoryResult|None)]``.

    Invariant: shared ``UCCatalog`` objects are immutable after enumeration, so
    members share them by reference — but ``external_locations`` dicts are copied
    per member because :func:`_enumerate_azure_for` mutates them with
    workspace-specific Azure mappings.
    """
    members = group["members"]
    source = opts.get("source", "api")
    skip_azure = opts.get("skip_azure", False)
    quiet = opts.get("quiet", False)
    concurrency = opts.get("concurrency", 5)
    warehouse_id = opts.get("warehouse_id", "")
    include_system = opts.get("include_system", False)
    catalog_filter = set(opts.get("catalogs") or [])
    enum_opts = {
        "include_system": include_system, "grants": opts.get("grants", "coarse"),
        "effective_grants": opts.get("effective_grants", False),
        "monitors": opts.get("monitors", False),
        "max_schemas": opts.get("max_schemas_per_catalog", 0),
        "max_tables": opts.get("max_tables_per_schema", 0),
    }

    def _run_one(host, token, name):
        return run_inventory(
            host, token, name, quiet=quiet, concurrency=concurrency,
            catalogs=list(catalog_filter) or None, include_system=include_system,
            grants=opts.get("grants", "coarse"),
            effective_grants=opts.get("effective_grants", False),
            tags_sql=opts.get("tags_sql", False), warehouse_id="",
            max_catalogs=opts.get("max_catalogs", 0),
            max_schemas_per_catalog=opts.get("max_schemas_per_catalog", 0),
            max_tables_per_schema=opts.get("max_tables_per_schema", 0),
            skip_azure=skip_azure, source=source, monitors=opts.get("monitors", False))

    # ── No-dedup group (singleton / unknown metastore) → run each member normally ──
    if not group.get("dedup"):
        out = []
        for host, token, name, _names in members:
            if not group.get("reachable", True):
                out.append((name, host, token, None))
                continue
            try:
                inv = await _run_one(host, token, name)
            except Exception as exc:
                _log(f"Inventory failed for {name or host}: {exc}")
                inv = None
            out.append((name, host, token, inv))
        return out

    # ── Dedup group: enumerate the metastore once on the leader ──
    leader_host = group["leader_host"]
    leader_token = next(t for (h, t, _n, _ns) in members if h == leader_host)
    leader_name = next(n for (h, _t, n, _ns) in members if h == leader_host)
    eff_source = _resolve_source(source, prefer_sql=True)
    sem = asyncio.Semaphore(max(1, concurrency))

    leader = UCInventoryResult(leader_host, leader_name, datetime.now().isoformat())
    try:
        async with httpx.AsyncClient(timeout=45) as client:
            await _enumerate_metastore_scoped(
                client, leader_host, leader_token, leader, sem,
                source=eff_source, warehouse_id=warehouse_id, catalog_filter=catalog_filter,
                include_system=include_system, opts=enum_opts,
                tags_sql=opts.get("tags_sql", False), monitors=opts.get("monitors", False),
                max_catalogs=opts.get("max_catalogs", 0), quiet=quiet)
            cat_cache = {c.name: c for c in leader.catalogs}
    except Exception as exc:
        _log(f"Metastore {group['metastore_id']} enumeration failed (leader {leader_host}); "
             f"falling back to per-workspace: {exc}")
        fallback = dict(group, dedup=False)
        return await _inventory_group(fallback, opts=opts)

    if not quiet:
        ls = leader.stats or _compute_stats(leader)
        _log(f"Metastore {group['metastore_id']}: enumerated {ls.get('catalogs', len(leader.catalogs))} "
             f"catalog(s) once on leader {leader_name or leader_host}; "
             f"reusing across {len(members)} workspace(s).")

    enumerated = set(cat_cache)
    results = []
    for host, token, name, names in members:
        is_leader = host == leader_host
        if not quiet:
            _log(f"[{name or host}] {'leader — building report' if is_leader else 'reusing metastore inventory'}…")
        async with httpx.AsyncClient(timeout=45) as client:
            missing = set(names) - enumerated
            if missing:
                if not quiet:
                    _log(f"[{name or host}] enumerating {len(missing)} catalog(s) not visible to the leader…")
                try:
                    new = await _enumerate_catalogs_subset(
                        client, host, token, missing, sem, source=eff_source,
                        warehouse_id=warehouse_id, opts=enum_opts,
                        include_system=include_system, sink=leader)
                    cat_cache.update(new)
                    enumerated |= set(new)
                except Exception as exc:
                    _log(f"Extra-catalog enumeration failed for {name or host}: {exc}")
            r = UCInventoryResult(host, name, datetime.now().isoformat())
            # Shared metastore-level securables (read-only → share references)…
            r.metastore = leader.metastore
            r.metastore_grants = leader.metastore_grants
            r.storage_credentials = leader.storage_credentials
            r.service_credentials = leader.service_credentials
            r.connections = leader.connections
            r.shares = leader.shares
            r.recipients = leader.recipients
            r.providers = leader.providers
            # …except external_locations, which _denormalize_azure mutates per workspace.
            r.external_locations = [dict(loc) for loc in leader.external_locations]
            # Per-workspace catalog visibility.
            visible = names or set(cat_cache)
            r.catalogs = [cat_cache[n] for n in sorted(visible) if n in cat_cache]
            try:
                await _enumerate_azure_for(client, host, token, r,
                                           skip_azure=skip_azure, quiet=quiet)
            except Exception as exc:
                _log(f"Azure enumeration failed for {name or host}: {exc}")
            r.stats = _compute_stats(r)
        if not quiet:
            st = r.stats
            az = r.azure or {}
            az_note = (f"Azure: {len(az.get('storage_accounts', []) or [])} storage account(s)"
                       if az.get("available") else "Azure: not discovered")
            _log(f"Inventory complete [{name or host}] "
                 f"({'leader' if is_leader else 'metastore reuse'}): "
                 f"{st['catalogs']} catalogs, {st['schemas']} schemas, {st['tables']} tables, "
                 f"{st['columns']} columns, {st['grants']} grants · {az_note}")
        results.append((name, host, token, r))
    return results


async def run_inventory_fleet(
    targets: list[tuple[str, str, str]],
    *,
    dedup: bool = True,
    workspace_concurrency: int = 3,
    metastore_concurrency: int = 2,
    **opts,
) -> list[tuple[str, str, str, "UCInventoryResult | None"]]:
    """Inventory many workspaces, deduplicating UC enumeration by metastore.

    Workspaces sharing a UC metastore enumerate the metastore-scoped catalog tree
    once and reuse it; only Azure infra runs per workspace. Returns the same
    ``[(name, host, token, UCInventoryResult|None)]`` contract as
    :func:`run_inventory_many`. With ``dedup=False`` or a single target it
    delegates straight to :func:`run_inventory_many`.
    """
    if not dedup or len(targets) <= 1:
        return await run_inventory_many(
            targets, workspace_concurrency=workspace_concurrency, **opts)

    probes = await _probe_all(targets, concurrency=workspace_concurrency)
    groups = _group_by_metastore(targets, probes)
    if not opts.get("quiet", False):
        for g in groups:
            if g["dedup"]:
                _log(f"{len(g['members'])} workspaces share metastore {g['metastore_id']} "
                     f"— enumerating once (leader {g['leader_host']}).")

    ms_sem = asyncio.Semaphore(max(1, metastore_concurrency))

    async def _run_group(g):
        async with ms_sem:
            return await _inventory_group(g, opts=opts)

    nested = await asyncio.gather(*[_run_group(g) for g in groups])
    return [item for sub in nested for item in sub]


_DEDUP_KEYS = ["catalogs", "schemas", "tables", "views", "columns", "grants",
               "volumes", "functions", "registered_models", "external_locations"]


def _metastore_id_of(inv) -> str:
    return (((inv.metastore or {}).get("current_assignment") or {}).get("metastore_id", "")) or ""


def _dedup_metastores(results: list[tuple]) -> tuple[list[dict], dict]:
    """Group reachable inventories by metastore and count each securable ONCE
    (set-union by full name), so workspaces sharing a metastore don't inflate the
    totals. Returns ``(per-metastore rows, fleet deduped totals)``."""
    acc: dict[str, dict] = {}
    order: list[str] = []

    def _bucket(mid: str, ws_label: str) -> dict:
        key = mid or f"(workspace) {ws_label}"
        a = acc.get(key)
        if a is None:
            a = {"metastore_id": mid, "workspaces": [],
                 "catalogs": set(), "schemas": set(), "tables": set(), "views": set(),
                 "columns": {}, "grants": set(), "volumes": set(), "functions": set(),
                 "models": set(), "external_locations": set()}
            acc[key] = a
            order.append(key)
        return a

    def _grant(a, g):
        a["grants"].add((g.securable_type, g.full_name, g.principal))

    for name, host, _token, inv in results:
        if inv is None:
            continue
        ws_label = inv.workspace_name or name or inv.workspace_url
        a = _bucket(_metastore_id_of(inv), ws_label)
        if ws_label not in a["workspaces"]:
            a["workspaces"].append(ws_label)
        for g in inv.metastore_grants:
            _grant(a, g)
        for c in inv.catalogs:
            a["catalogs"].add(c.name)
            for g in c.grants:
                _grant(a, g)
            for s in c.schemas:
                a["schemas"].add(s.full_name)
                for g in s.grants:
                    _grant(a, g)
                for t in s.tables:
                    (a["views"] if t.table_type == "VIEW" else a["tables"]).add(t.full_name)
                    a["columns"][t.full_name] = len(t.columns)
                    for g in t.grants:
                        _grant(a, g)
                for v in s.volumes:
                    a["volumes"].add(v.full_name)
                    for g in v.grants:
                        _grant(a, g)
                for f in s.functions:
                    a["functions"].add(f.full_name)
                    for g in f.grants:
                        _grant(a, g)
                for m in s.models:
                    a["models"].add(m.full_name)
                    for g in m.grants:
                        _grant(a, g)
        for el in inv.external_locations:
            a["external_locations"].add(el.get("name", ""))

    fleet = dict.fromkeys(_DEDUP_KEYS, 0)
    rows = []
    for key in order:
        a = acc[key]
        counts = {
            "catalogs": len(a["catalogs"]), "schemas": len(a["schemas"]),
            "tables": len(a["tables"]), "views": len(a["views"]),
            "columns": sum(a["columns"].values()), "grants": len(a["grants"]),
            "volumes": len(a["volumes"]), "functions": len(a["functions"]),
            "registered_models": len(a["models"]),
            "external_locations": len(a["external_locations"]),
        }
        for k in _DEDUP_KEYS:
            fleet[k] += counts[k]
        rows.append({"metastore_id": a["metastore_id"], "workspaces": a["workspaces"],
                     "deduped_totals": counts})
    return rows, fleet


def aggregate_inventories(results: list[tuple]) -> dict:
    """Roll up per-workspace inventories into a fleet-wide summary.

    ``results`` is the output of :func:`run_inventory_many` / :func:`run_inventory_fleet`.
    Produces summed fleet totals + per-workspace rows + the Azure storage-account
    footprint, **plus** a metastore-deduped view (``metastores`` / ``deduped_totals``)
    that counts each shared UC securable once per metastore.
    """
    _count_keys = ["catalogs", "schemas", "tables", "views", "columns", "grants",
                   "volumes", "functions", "registered_models", "external_locations"]
    totals = {k: 0 for k in _count_keys}
    totals["storage_accounts"] = 0
    workspaces: list[dict] = []
    footprint: list[dict] = []
    reachable = 0

    for name, host, token, inv in results:
        if inv is None:
            workspaces.append({"workspace": name or host, "url": host.rstrip("/"),
                               "status": "unreachable"})
            continue
        reachable += 1
        s = inv.stats or {}
        az = inv.azure or {}
        azw = az.get("workspace") or {}
        accounts = az.get("storage_accounts", []) or []
        ws_label = inv.workspace_name or name or inv.workspace_url
        row = {"workspace": ws_label, "url": inv.workspace_url, "status": "ok",
               "resource_group": azw.get("resource_group", ""),
               "region": azw.get("location", ""), "geo": azw.get("geo", ""),
               "azure_available": az.get("available", False),
               "storage_accounts": len(accounts)}
        for k in _count_keys:
            row[k] = s.get(k, 0)
            totals[k] += s.get(k, 0)
        row["errors"] = s.get("errors", 0)
        totals["storage_accounts"] += len(accounts)
        workspaces.append(row)
        for a in accounts:
            footprint.append({"workspace": ws_label, "name": a.get("name", ""),
                              "resource_group": a.get("resource_group", ""),
                              "subscription_id": a.get("subscription_id", ""),
                              "location": a.get("location", ""),
                              "hns_enabled": a.get("hns_enabled", ""),
                              "public_network_access": a.get("public_network_access", "")})

    metastores, deduped_totals = _dedup_metastores(results)
    return {
        "workspace_count": len(results),
        "reachable": reachable,
        "totals": totals,
        "deduped_totals": deduped_totals,
        "metastores": metastores,
        "workspaces": workspaces,
        "azure_storage_footprint": footprint,
    }
