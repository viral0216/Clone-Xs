"""SAT Scanner — Unity Catalog inventory hierarchy → offline report tree.

Turns an enumerated Unity Catalog inventory (:class:`UCInventoryResult`, or its
``to_dict()`` form) into a plain nested-dict tree::

    Metastore → Catalog → Schema → {Table / View, Volume, Function, Model}

and renders it as three self-contained, offline HTML views — exactly mirroring
the Azure resource-hierarchy feature (:mod:`azure_hierarchy`):

  * **Tree** — collapsible indented tree with search, type chips, object-type /
    owner filters and a click-through detail panel;
  * **Sunburst** — zoomable concentric rings (click a ring to zoom in, the
    centre to zoom out);
  * **Hub & spoke** — radial drill-down with breadcrumbs and a jump-to picker.

The node shape matches the Azure renderers' contract::

    {"name", "type", "children": [...]}

with each node additionally carrying UC metadata (``full_name``, ``owner``,
``comment`` …) plus ``resource_type`` (object kind) and ``location`` (owner) so
the two generic filter drop-downs work unchanged.

Columns are intentionally *not* expanded into nodes — a workspace can carry tens
of thousands of them, which would make the sunburst / hub views unusable and
bloat the HTML.  Each table instead carries a column *count*; the full column
detail lives in the JSON / Excel inventory exports.  No data leaves the machine.
"""
from __future__ import annotations

import html
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

# Node type labels (matched verbatim by the embedded JS).
METASTORE = "Metastore"
CATALOG = "Catalog"
SCHEMA = "Schema"
TABLE = "Table"
VIEW = "View"
VOLUME = "Volume"
FUNCTION = "Function"
MODEL = "Model"

# Per-type fill colour, dark-text flag (True ⇒ light fill ⇒ dark label) and a
# darker stroke.  Databricks-inspired palette.
#   type: (fill, light_fill?, stroke)
_PALETTE: dict[str, tuple[str, bool, str]] = {
    METASTORE: ("#1b3139", False, "#0d1a1f"),
    CATALOG:   ("#ff3621", False, "#c41e0c"),
    SCHEMA:    ("#fb7359", False, "#d34f37"),
    TABLE:     ("#00a972", False, "#007a52"),
    VIEW:      ("#2272b4", False, "#155085"),
    VOLUME:    ("#8a63d2", False, "#5f3fa3"),
    FUNCTION:  ("#e0a800", True,  "#a87e00"),
    MODEL:     ("#00b0c7", False, "#007e90"),
    # Fleet root (multi-metastore combined view)
    "Fleet":   ("#0d2a30", False, "#061418"),
    # Infrastructure-topology node types (Feature: topology view)
    "Group":             ("#5a6b7d", False, "#3d4a58"),
    "StorageAccount":    ("#2f6fb0", False, "#1d4a7a"),
    "ExternalLocation":  ("#0a9396", False, "#066a6c"),
    "StorageCredential": ("#8a63d2", False, "#5f3fa3"),
    "Connection":        ("#e0a800", True,  "#a87e00"),
}

_DOT = {t: v[0] for t, v in _PALETTE.items()}


# ─────────────────────────────────────────────────────────────────────────────
# 1. Tree assembly  (UCInventoryResult / inventory dict → nested node tree)
# ─────────────────────────────────────────────────────────────────────────────

def _short(val: Any, limit: int = 240) -> str:
    """Stringify and truncate a value so embedded metadata stays compact."""
    if val is None:
        return ""
    s = str(val)
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _as_dict(inv: Any) -> dict:
    """Accept either a ``UCInventoryResult`` or its ``to_dict()`` form."""
    return inv.to_dict() if hasattr(inv, "to_dict") else (inv or {})


def _grants_count(obj: dict) -> int:
    return len(obj.get("grants") or [])


# ── Table statistics (from Delta ``properties``) + object classification ─────

_DELTA_FEATURE_KEYS = {
    "delta.enableChangeDataFeed": "CDF",
    "delta.enableDeletionVectors": "Deletion vectors",
    "delta.enableRowTracking": "Row tracking",
}


def _parse_int(v: Any) -> int | None:
    """Tolerant string/number → non-negative int; ``None`` on empty/garbage."""
    if v is None:
        return None
    try:
        n = int(float(str(v).strip()))
    except (ValueError, TypeError):
        return None
    return n if n >= 0 else None


def _humanize_bytes(n: int | None) -> str:
    if n is None:
        return ""
    val = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if val < 1024 or unit == "PB":
            if unit == "B":
                return f"{int(val)} B"
            return f"{val:.1f} {unit}" if val < 10 else f"{val:.0f} {unit}"
        val /= 1024
    return ""


def _humanize_int(n: int | None) -> str:
    if n is None:
        return ""
    val = float(n)
    for suffix in ("", "K", "M", "B", "T"):
        if val < 1000 or suffix == "T":
            if suffix == "":
                return f"{int(val)}"
            return f"{val:.1f}{suffix}" if val < 10 else f"{val:.0f}{suffix}"
        val /= 1000
    return ""


def _table_stats(props: dict) -> dict:
    """Row-count / size / last-modified / Delta-feature flags from a table's
    ``properties`` map.  Returns only the keys actually present (raw ints for
    JS maths + pre-humanized strings for display)."""
    out: dict = {}
    rows = _parse_int(props.get("spark.sql.statistics.numRows"))
    if rows is not None:
        out["rows"], out["rows_h"] = rows, _humanize_int(rows)
    nbytes = _parse_int(props.get("spark.sql.statistics.totalSize"))
    if nbytes is not None:
        out["bytes"], out["bytes_h"] = nbytes, _humanize_bytes(nbytes)
    ms = _parse_int(props.get("delta.lastCommitTimestamp"))
    if ms:
        try:
            out["modified"] = ms
            out["modified_h"] = (datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
                                 .strftime("%Y-%m-%d %H:%M UTC"))
        except (ValueError, OverflowError, OSError):
            pass
    feats = [label for key, label in _DELTA_FEATURE_KEYS.items()
             if str(props.get(key, "")).lower() == "true"]
    if feats:
        out["features"] = feats
    return out


def _is_view(table_type: str) -> bool:
    """True for view-like objects (plain / materialized / metric views)."""
    return (table_type or "").upper() in {"VIEW", "MATERIALIZED_VIEW", "METRIC_VIEW"}


def _grants_compact(obj: dict) -> list[dict]:
    """Compact grant records ``{p: principal, v: [privileges]}`` (skips empties)."""
    out = []
    for g in obj.get("grants") or []:
        principal, privs = g.get("principal", ""), (g.get("privileges") or [])
        if principal or privs:
            out.append({"p": principal, "v": list(privs)})
    return out


def _col_compact(c: dict) -> dict:
    """A minimal column record for the click-to-inspect panels: name + type,
    plus optional NOT-NULL (``x``), column-mask (``m``) and comment (``c``)
    markers.  Kept tiny (short keys, only present-when-set) so embedding every
    column across the whole metastore stays affordable in the offline HTML."""
    out = {"n": c.get("name", ""), "t": c.get("type_text") or c.get("type_name") or ""}
    if c.get("nullable") is False:
        out["x"] = 1
    mask = c.get("mask")
    if mask:
        out["m"] = mask.get("function_name", "") if isinstance(mask, dict) else str(mask)
    cm = c.get("comment")
    if cm:
        out["c"] = _short(cm, 160)
    return out


def _table_node(t: dict) -> dict:
    obj_type = t.get("table_type") or "TABLE"
    cols = t.get("columns") or []
    node = {
        "name": t.get("name", ""),
        "type": VIEW if _is_view(obj_type) else TABLE,
        "full_name": t.get("full_name", ""),
        "catalog": t.get("catalog", ""),
        "schema": t.get("schema", ""),
        "table_type": obj_type,
        "data_source_format": t.get("data_source_format", ""),
        "storage_location": _short(t.get("storage_location", "")),
        "owner": t.get("owner", ""),
        "comment": _short(t.get("comment", "")),
        "n_columns": len(cols),
        "n_grants": _grants_count(t),
        "grants": _grants_compact(t),
        "columns": [_col_compact(c) for c in cols],  # click-to-inspect detail
        "resource_type": obj_type,             # object-type filter
        "location": t.get("owner", ""),        # owner filter
    }
    node.update(_table_stats(t.get("properties") or {}))  # rows/bytes/modified/features
    return node


def _volume_node(v: dict) -> dict:
    return {
        "name": v.get("name", ""),
        "type": VOLUME,
        "full_name": v.get("full_name", ""),
        "volume_type": v.get("volume_type", ""),
        "storage_location": _short(v.get("storage_location", "")),
        "owner": v.get("owner", ""),
        "comment": _short(v.get("comment", "")),
        "n_grants": _grants_count(v),
        "grants": _grants_compact(v),
        "resource_type": "VOLUME",
        "location": v.get("owner", ""),
    }


def _function_node(f: dict) -> dict:
    return {
        "name": f.get("name", ""),
        "type": FUNCTION,
        "full_name": f.get("full_name", ""),
        "data_type": f.get("data_type", ""),
        "routine_body": f.get("routine_body", ""),
        "owner": f.get("owner", ""),
        "comment": _short(f.get("comment", "")),
        "n_grants": _grants_count(f),
        "grants": _grants_compact(f),
        "resource_type": "FUNCTION",
        "location": f.get("owner", ""),
    }


def _model_node(m: dict) -> dict:
    return {
        "name": m.get("name", ""),
        "type": MODEL,
        "full_name": m.get("full_name", ""),
        "owner": m.get("owner", ""),
        "comment": _short(m.get("comment", "")),
        "n_versions": len(m.get("versions") or []),
        "n_grants": _grants_count(m),
        "grants": _grants_compact(m),
        "resource_type": "MODEL",
        "location": m.get("owner", ""),
    }


def _schema_node(s: dict) -> dict:
    tables = s.get("tables") or []
    volumes = s.get("volumes") or []
    functions = s.get("functions") or []
    models = s.get("models") or []
    children: list[dict] = []
    children += [_table_node(t) for t in tables]
    children += [_volume_node(v) for v in volumes]
    children += [_function_node(f) for f in functions]
    children += [_model_node(m) for m in models]
    n_view = sum(1 for t in tables if _is_view(t.get("table_type")))
    return {
        "name": s.get("name", ""),
        "type": SCHEMA,
        "full_name": s.get("full_name", ""),
        "catalog": s.get("catalog", ""),
        "owner": s.get("owner", ""),
        "comment": _short(s.get("comment", "")),
        "tags": s.get("tags") or {},
        "n_tables": len(tables) - n_view,
        "n_views": n_view,
        "n_volumes": len(volumes),
        "n_functions": len(functions),
        "n_models": len(models),
        "n_grants": _grants_count(s),
        "grants": _grants_compact(s),
        "resource_type": SCHEMA,
        "location": s.get("owner", ""),
        "children": children,
    }


def _catalog_node(c: dict) -> dict:
    schemas = c.get("schemas") or []
    return {
        "name": c.get("name", ""),
        "type": CATALOG,
        "full_name": c.get("name", ""),
        "catalog_type": c.get("catalog_type", ""),
        "owner": c.get("owner", ""),
        "comment": _short(c.get("comment", "")),
        "isolation_mode": c.get("isolation_mode", ""),
        "storage_root": _short(c.get("storage_root", "")),
        "tags": c.get("tags") or {},
        "n_schemas": len(schemas),
        "n_grants": _grants_count(c),
        "grants": _grants_compact(c),
        "resource_type": c.get("catalog_type", "") or "CATALOG",
        "location": c.get("owner", ""),
        "children": [_schema_node(s) for s in schemas],
    }


def _metastore_name(inv: dict) -> str:
    ms = inv.get("metastore") or {}
    cur = ms.get("current_assignment") or {}
    for key in ("metastore_name", "name"):
        if cur.get(key):
            return str(cur[key])
    metastores = ms.get("metastores") or []
    if metastores and isinstance(metastores[0], dict) and metastores[0].get("name"):
        return str(metastores[0]["name"])
    return inv.get("workspace_name") or "Unity Catalog Metastore"


def build_uc_tree(inv: Any) -> dict:
    """Build the ``Metastore → Catalog → Schema → object`` node tree.

    ``inv`` may be a :class:`UCInventoryResult` or its ``to_dict()`` dict form.
    """
    d = _as_dict(inv)
    root = {
        "name": _metastore_name(d),
        "type": METASTORE,
        "workspace": d.get("workspace_name", ""),
        "workspace_url": d.get("workspace_url", ""),
        "scanned_at": d.get("scanned_at", ""),
        "n_grants": len(d.get("metastore_grants") or []),
        "children": [_catalog_node(c) for c in (d.get("catalogs") or [])],
    }
    assign_colours(root)
    return root


def _fleet_metastore_id(d: dict) -> str:
    return (((d.get("metastore") or {}).get("current_assignment") or {}).get("metastore_id", "")) or ""


def _set_visibility(node: dict, widx: list[int]) -> None:
    """Stamp the workspace-visibility index list ``w`` onto a node and every
    descendant (so the fleet tree's workspace filter can hide whole subtrees)."""
    node["w"] = widx
    for c in node.get("children", []):
        _set_visibility(c, widx)


def build_fleet_tree(inventories: list[Any]) -> dict:
    """Combine many per-workspace inventories into one fleet tree, grouped by
    metastore: ``Fleet → Metastore → Catalog → Schema → object``.

    Catalogs are unioned by name within each metastore (deduped — workspaces that
    share a metastore contribute the same catalogs). With a single metastore the
    Metastore node is returned directly (no ``Fleet`` wrapper). Every node carries
    a ``w`` list of workspace indices that can see it (for the workspace filter);
    the root carries the ordered ``workspaces`` name list."""
    ws_list: list[str] = []
    ws_index: dict[str, int] = {}
    groups: dict[str, dict] = {}
    order: list[str] = []
    for inv in inventories:
        d = _as_dict(inv)
        wsn = d.get("workspace_name", "")
        if wsn and wsn not in ws_index:
            ws_index[wsn] = len(ws_list)
            ws_list.append(wsn)
        mid = _fleet_metastore_id(d)
        key = mid or f"(workspace) {wsn}"
        g = groups.get(key)
        if g is None:
            g = {"metastore_id": mid, "metastore": d.get("metastore") or {},
                 "workspaces": [], "catalogs_by_name": {}, "catalog_ws": {}}
            groups[key] = g
            order.append(key)
        if wsn and wsn not in g["workspaces"]:
            g["workspaces"].append(wsn)
        wi = ws_index.get(wsn)
        for c in (d.get("catalogs") or []):
            name = c.get("name", "")
            g["catalogs_by_name"].setdefault(name, c)
            if wi is not None:
                g["catalog_ws"].setdefault(name, set()).add(wi)

    ms_nodes = []
    for key in order:
        g = groups[key]
        synthetic = {"workspace_name": _metastore_name({"metastore": g["metastore"]}),
                     "metastore": g["metastore"],
                     "catalogs": list(g["catalogs_by_name"].values())}
        node = build_uc_tree(synthetic)            # METASTORE root w/ _catalog_node children
        node["metastore_id"] = g["metastore_id"]
        node["workspaces"] = g["workspaces"]
        node["n_workspaces"] = len(g["workspaces"])
        ms_widx: set[int] = set()
        for cat_node in node["children"]:
            widx = sorted(g["catalog_ws"].get(cat_node["name"], []))
            _set_visibility(cat_node, widx)         # catalog subtree → its workspaces
            ms_widx.update(widx)
        node["w"] = sorted(ms_widx)
        ms_nodes.append(node)

    all_w = list(range(len(ws_list)))
    if len(ms_nodes) == 1:
        root = ms_nodes[0]
    else:
        n_ws = sum(n["n_workspaces"] for n in ms_nodes)
        root = {"name": f"Fleet — {n_ws} workspaces · {len(ms_nodes)} metastores",
                "type": "Fleet", "n_workspaces": n_ws, "w": all_w, "children": ms_nodes}
    root["workspaces"] = ws_list                    # dropdown source (plural ⇒ fleet)
    assign_colours(root)
    return root


# ─────────────────────────────────────────────────────────────────────────────
# 2. Colour, counting & traversal helpers  (per-type colouring)
# ─────────────────────────────────────────────────────────────────────────────

def assign_colours(root: dict) -> None:
    """Colour every node by its UC object type (catalogs red, tables green …)."""
    stack = [root]
    while stack:
        n = stack.pop()
        fill, light, stroke = _PALETTE.get(n.get("type", ""), ("#9aa8b5", True, "#6b7886"))
        n["_color"] = fill
        n["_dark"] = not light          # _dark ⇒ dark fill ⇒ white label
        n["_stroke"] = stroke
        stack.extend(n.get("children", []))


def count_by_type(root: dict) -> dict[str, int]:
    """Count nodes per type across the whole tree."""
    counts: dict[str, int] = defaultdict(int)
    stack = [root]
    while stack:
        n = stack.pop()
        counts[n.get("type", "")] += 1
        stack.extend(n.get("children", []))
    return dict(counts)


def walk_tree(root: dict):
    """Depth-first generator yielding ``(depth, node)`` for every node."""
    stack = [(0, root)]
    while stack:
        depth, node = stack.pop()
        yield depth, node
        for c in reversed(node.get("children", [])):
            stack.append((depth + 1, c))


# ─────────────────────────────────────────────────────────────────────────────
# 3. HTML renderers  (self-contained, offline)
# ─────────────────────────────────────────────────────────────────────────────

def _stats_line(root: dict) -> str:
    c = count_by_type(root)
    parts = [
        (c.get(CATALOG, 0), "catalogs"),
        (c.get(SCHEMA, 0), "schemas"),
        (c.get(TABLE, 0), "tables"),
        (c.get(VIEW, 0), "views"),
        (c.get(VOLUME, 0), "volumes"),
        (c.get(FUNCTION, 0), "functions"),
        (c.get(MODEL, 0), "models"),
    ]
    return " &nbsp;·&nbsp; ".join(f"<b>{n}</b> {label}" for n, label in parts)


def _json_inline(obj: Any) -> str:
    """Serialise for safe embedding in a ``<script>`` — escape ``</`` so a value
    containing ``</script>`` (e.g. a comment) can't terminate the tag early."""
    return json.dumps(obj).replace("</", "<\\/")


# Cross-diagram artifact suffixes (filename = ``{prefix}{suffix}``).
_ARTIFACTS = [
    ("Overview", "-overview.html"),
    ("Tree", "-tree.html"),
    ("Sunburst", "-star.html"),
    ("Hub & Spoke", "-hubspoke.html"),
    ("Infrastructure", "-topology.html"),
    ("Report", ".html"),
]


def build_nav(prefix: str, active: str = "") -> str:
    """A row of links to every sibling diagram (same ``prefix``), for in-page
    navigation. ``active`` highlights the current artifact by label."""
    items = "".join(
        f'<a class="{"on" if label == active else ""}" href="{html.escape(prefix + suffix)}">{html.escape(label)}</a>'
        for label, suffix in _ARTIFACTS
    )
    return f'<nav class="diagnav">{items}</nav>'


def render_tree_html(root: dict, title: str = "Unity Catalog Inventory — Tree", nav: str = "") -> str:
    return (_TREE_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _stats_line(root))
            .replace("__NAV__", nav)
            .replace("__DATA__", _json_inline(root)))


def render_star_html(root: dict, title: str = "Unity Catalog Inventory — Sunburst", nav: str = "") -> str:
    return (_STAR_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _stats_line(root))
            .replace("__NAV__", nav)
            .replace("__DATA__", _json_inline(root)))


def render_hub_html(root: dict, title: str = "Unity Catalog Inventory — Hub & Spoke", nav: str = "") -> str:
    return (_HUB_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _stats_line(root))
            .replace("__NAV__", nav)
            .replace("__DATA__", _json_inline(root)))


_LEGEND = (
    '<div class="legend">'
    '<span><i class="dot" style="background:#1b3139"></i> Metastore</span>'
    '<span><i class="dot" style="background:#ff3621"></i> Catalog</span>'
    '<span><i class="dot" style="background:#fb7359"></i> Schema</span>'
    '<span><i class="dot" style="background:#00a972"></i> Table</span>'
    '<span><i class="dot" style="background:#2272b4"></i> View</span>'
    '<span><i class="dot" style="background:#8a63d2"></i> Volume</span>'
    '<span><i class="dot" style="background:#e0a800"></i> Function</span>'
    '<span><i class="dot" style="background:#00b0c7"></i> Model</span>'
    '</div>'
)


# ── Shared click-to-inspect detail drawer (used by all three views) ──────────
# Columns + data types for tables/views, plus key metadata for any node.

_COLS_CSS = """
  .cols-h { font-weight:600; margin:16px 0 6px; font-size:12.5px; color:#1b2733; display:flex; align-items:center; gap:8px; }
  .cols-h .cc { background:#eef3fb; color:#3d5168; border-radius:10px; padding:1px 8px; font-size:11px; font-weight:600; }
  .cols-h .colq { margin-left:auto; font:inherit; font-size:12px; padding:3px 8px; border:1px solid #cdd8e6; border-radius:6px; width:150px; }
  .cols { border-collapse:collapse; width:100%; font-size:12px; }
  .cols th { text-align:left; color:#5a6b7d; font-weight:600; border-bottom:1px solid #e6ecf4; padding:4px 6px; position:sticky; top:0; background:#fff; }
  .cols td { padding:3px 6px; border-bottom:1px solid #f3f6fb; vertical-align:top; }
  .cols td.ci { color:#9aa7b5; width:34px; }
  .cols td.ct { font-family:"SF Mono",Menlo,Consolas,monospace; color:#2272b4; white-space:nowrap; }
  .cols td.cn { color:#b03a2e; font-size:10px; white-space:nowrap; }
"""

_PANEL_CSS = """
  .panel { position:fixed; top:0; right:0; width:420px; max-width:94vw; height:100%; background:#fff; border-left:1px solid #e3e9f1;
           box-shadow:-4px 0 18px rgba(20,40,80,.08); transform:translateX(100%); transition:transform .18s; z-index:30;
           display:flex; flex-direction:column; }
  .panel.open { transform:none; }
  .panel-hd { padding:14px 18px; border-bottom:1px solid #eef2f7; display:flex; align-items:center; gap:10px; }
  .panel-hd b { font-size:14px; word-break:break-word; }
  .panel-hd .x { margin-left:auto; cursor:pointer; color:#8a97a6; font-size:16px; }
  .panel-bd { padding:14px 18px; overflow:auto; font-size:13px; }
  .panel-bd table.kv { border-collapse:collapse; width:100%; }
  .panel-bd table.kv td { padding:5px 6px; border-bottom:1px solid #f0f3f8; vertical-align:top; word-break:break-word; }
  .panel-bd table.kv td.k { color:#5a6b7d; width:124px; }
  .panel-bd pre { margin:0; white-space:pre-wrap; font:12px/1.5 "SF Mono",Menlo,Consolas,monospace; }
""" + _COLS_CSS

# Cross-diagram nav bar (Overview · Tree · Sunburst · Hub · Infrastructure · Report).
_NAV_CSS = """
  .diagnav { background:#fff; border-bottom:1px solid #e3e9f1; padding:8px 24px; display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .diagnav a { font-size:12.5px; color:#1b3139; text-decoration:none; border:1px solid #d7e0ec; background:#f6f9fd; border-radius:7px; padding:4px 12px; }
  .diagnav a:hover { background:#eaf1fb; border-color:#b9cdec; }
  .diagnav a.on { background:#1b3139; color:#fff; border-color:#1b3139; }
"""
_PANEL_CSS = _PANEL_CSS + _NAV_CSS

_PANEL_HTML = """<div class="panel" id="panel">
  <div class="panel-hd"><b id="p-title"></b><span class="x" onclick="hidePanel()">✕</span></div>
  <div class="panel-bd" id="p-body"></div>
</div>"""

# Detail helpers — depend on an `esc()` defined by each template.
_DETAIL_JS = """
function escA(s){ return esc(s).replace(/"/g,'&quot;'); }
function detailRows(n){
  const rows=[['Type',n.type]];
  if(n.full_name && n.full_name!==n.name) rows.push(['Full name',n.full_name]);
  if(n.catalog_type) rows.push(['Catalog type',n.catalog_type]);
  if(n.table_type) rows.push(['Object type',n.table_type]);
  if(n.volume_type) rows.push(['Volume type',n.volume_type]);
  if(n.data_source_format) rows.push(['Format',n.data_source_format]);
  if(n.data_type) rows.push(['Returns',n.data_type]);
  if(n.rows_h) rows.push(['Rows',n.rows_h]);
  if(n.bytes_h) rows.push(['Size',n.bytes_h]);
  if(n.modified_h) rows.push(['Last modified',n.modified_h]);
  if(n.features && n.features.length) rows.push(['Delta features',n.features.join(', ')]);
  if(n.owner) rows.push(['Owner',n.owner]);
  if(n.isolation_mode) rows.push(['Isolation',n.isolation_mode]);
  if(n.storage_root) rows.push(['Storage root',n.storage_root]);
  if(n.storage_location) rows.push(['Storage location',n.storage_location]);
  if(n.url) rows.push(['URL',n.url]);
  if(n.read_only!=null) rows.push(['Read only',String(n.read_only)]);
  if(n.credential_name) rows.push(['Credential',n.credential_name]);
  if(n.connection_type) rows.push(['Connection type',n.connection_type]);
  if(n.resource_group) rows.push(['Resource group',n.resource_group]);
  if(n.subscription_id) rows.push(['Subscription',n.subscription_id]);
  if(n.region) rows.push(['Region',n.region]);
  if(n.sku) rows.push(['SKU',n.sku]);
  if(n.hns_enabled!=null) rows.push(['HNS (ADLS Gen2)',String(n.hns_enabled)]);
  if(n.public_network_access) rows.push(['Public network',n.public_network_access]);
  if(n.network_default_action) rows.push(['Network default',n.network_default_action]);
  if(n.n_schemas!=null) rows.push(['Schemas',n.n_schemas]);
  if(n.n_tables!=null) rows.push(['Tables',n.n_tables]);
  if(n.n_views) rows.push(['Views',n.n_views]);
  if(n.n_volumes) rows.push(['Volumes',n.n_volumes]);
  if(n.n_functions) rows.push(['Functions',n.n_functions]);
  if(n.n_models) rows.push(['Models',n.n_models]);
  if(n.n_columns!=null) rows.push(['Columns',n.n_columns]);
  if(n.n_versions!=null) rows.push(['Versions',n.n_versions]);
  if(n.n_grants!=null) rows.push(['Grants',n.n_grants]);
  if(n.workspace) rows.push(['Workspace',n.workspace]);
  return rows;
}
function kvHtml(n){
  let h='<table class="kv">'+detailRows(n).map(r=>'<tr><td class="k">'+esc(r[0])+'</td><td>'+esc(r[1])+'</td></tr>').join('');
  if(n.comment) h+='<tr><td class="k">Comment</td><td>'+esc(n.comment)+'</td></tr>';
  if(n.tags && Object.keys(n.tags).length) h+='<tr><td class="k">Tags</td><td><pre>'+esc(JSON.stringify(n.tags,null,2))+'</pre></td></tr>';
  return h+'</table>';
}
function colsHtml(n){
  if(!n.columns || !n.columns.length) return '';
  const rows=n.columns.map((c,i)=>'<tr'+(c.c?' title="'+escA(c.c)+'"':'')+'><td class="ci">'+(i+1)+'</td><td>'+esc(c.n)
      +'</td><td class="ct">'+esc(c.t)+'</td><td class="cn">'+(c.x?'NOT NULL':'')+(c.m?' 🔒':'')+'</td></tr>').join('');
  return '<div class="cols-h">Columns <span class="cc">'+n.columns.length+'</span>'
       +'<input class="colq" type="search" placeholder="filter columns…" oninput="filterCols(this)"></div>'
       +'<table class="cols"><thead><tr><th>#</th><th>Name</th><th>Type</th><th></th></tr></thead><tbody>'+rows+'</tbody></table>';
}
function filterCols(inp){
  const q=(inp.value||'').trim().toLowerCase();
  const tb=inp.closest('.cols-h').nextElementSibling.querySelector('tbody');
  tb.querySelectorAll('tr').forEach(tr=>{ const t=(tr.children[1].textContent+' '+tr.children[2].textContent).toLowerCase();
    tr.style.display=(!q||t.includes(q))?'':'none'; });
}
function grantsHtml(n){
  if(!n.grants || !n.grants.length) return '';
  const rows=n.grants.map(g=>'<tr><td>'+esc(g.p)+'</td><td class="ct">'+esc((g.v||[]).join(', '))+'</td></tr>').join('');
  return '<div class="cols-h">Grants <span class="cc">'+n.grants.length+'</span></div>'
       +'<table class="cols"><thead><tr><th>Principal</th><th>Privileges</th></tr></thead><tbody>'+rows+'</tbody></table>';
}
function panelBody(n){ return kvHtml(n)+colsHtml(n)+grantsHtml(n); }
"""


_TREE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --bg:#f6f8fb; --line:#c7d4e6; }
  * { box-sizing: border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:var(--bg); color:#1b2733; }
  header { background:#fff; padding:14px 24px; border-bottom:1px solid #e3e9f1; position:sticky; top:0; z-index:6;
           display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  .stats { font-size:13px; color:#5a6b7d; }
  .stats b { color:#1b2733; }
  .legend { display:flex; gap:14px; font-size:12px; color:#5a6b7d; align-items:center; flex-wrap:wrap; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .dot { width:12px; height:12px; border-radius:3px; }
  .controls { background:#fff; border-bottom:1px solid #e3e9f1; padding:10px 24px; position:sticky; top:56px; z-index:5;
              display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .controls input[type=search], .controls select { font:inherit; font-size:13px; padding:6px 10px;
              border:1px solid #cdd8e6; border-radius:6px; background:#fff; }
  .controls input[type=search] { width:280px; }
  .controls button { font:inherit; font-size:13px; padding:6px 12px; border:1px solid #cdd8e6; background:#fff; border-radius:6px; cursor:pointer; }
  .controls button:hover { background:#eef3fb; }
  .chips { display:inline-flex; gap:6px; flex-wrap:wrap; }
  .chip { font-size:12px; padding:4px 10px; border-radius:14px; border:1px solid #cdd8e6; cursor:pointer; user-select:none;
          display:inline-flex; align-items:center; gap:6px; }
  .chip .d { width:9px; height:9px; border-radius:50%; }
  .chip.off { opacity:.45; text-decoration:line-through; }
  .status { font-size:12px; color:#5a6b7d; margin-left:auto; }
  #tree { padding:18px 28px 80px; }
  ul { list-style:none; margin:0; padding-left:26px; position:relative; }
  #tree > ul { padding-left:0; }
  li { position:relative; padding:3px 0; }
  li::before { content:""; position:absolute; top:-3px; left:-14px; bottom:50%; width:14px;
               border-left:1px solid var(--line); border-bottom:1px solid var(--line); border-bottom-left-radius:6px; }
  #tree > ul > li::before { display:none; }
  li.has-sib::after { content:""; position:absolute; top:0; left:-14px; bottom:-3px; border-left:1px solid var(--line); }
  li.last::after { display:none; }
  .node { display:inline-flex; align-items:center; gap:8px; padding:5px 11px; border-radius:8px; background:#fff;
          border:1px solid #dbe4f0; box-shadow:0 1px 1px rgba(20,40,80,.04); cursor:pointer; }
  .node:hover { border-color:#9db8de; }
  .node.sel { border-color:#ff3621; box-shadow:0 0 0 2px rgba(255,54,33,.18); }
  .node .tw { cursor:pointer; width:16px; height:16px; display:inline-flex; align-items:center; justify-content:center;
              color:#8095ab; font-size:11px; user-select:none; }
  .node .tw.empty { visibility:hidden; }
  .badge { width:20px; height:20px; border-radius:5px; display:inline-flex; align-items:center; justify-content:center;
           font-size:11px; font-weight:700; color:#fff; }
  .name { font-size:13.5px; }
  .metastore > .name, .cat > .name { font-weight:600; }
  .name mark { background:#ffe08a; padding:0 1px; border-radius:2px; }
  .count { font-size:11px; color:#fff; background:#7c8da3; border-radius:10px; padding:1px 7px; }
  .cat .count { background:#c41e0c; }
  .meta { font-size:10.5px; color:#8a97a6; }
  .meta.sub { color:#fff; background:#5a6b7d; border-radius:9px; padding:1px 7px; font-weight:600; font-size:9.5px; letter-spacing:.02em; }
  .meta.sz { color:#3d5168; background:#eef3fb; border-radius:9px; padding:1px 7px; font-weight:600; }
  .heat-legend { display:none; align-items:center; gap:6px; font-size:11px; color:#5a6b7d; }
  .heat-legend.on { display:inline-flex; }
  .heat-legend .ramp { width:120px; height:10px; border-radius:5px; background:linear-gradient(90deg,#00a972,#e0a800,#d34f37); }
  li.collapsed > ul { display:none; }
  li.hide { display:none; }
__PANEL_CSS__
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="stats">__STATS__</div>
  __LEGEND__
</header>
__NAV__
<div class="controls">
  <input type="search" id="q" placeholder="Search name, type, full name, owner…" oninput="onSearch(this.value)">
  <span class="chips" id="chips"></span>
  <select id="rtype" onchange="state.rtype=this.value; apply()"><option value="">All object types</option></select>
  <select id="loc" onchange="state.loc=this.value; apply()"><option value="">All owners</option></select>
  <select id="fmt" onchange="state.fmt=this.value; apply()"><option value="">All formats</option></select>
  <select id="wsfilter" style="display:none" onchange="state.ws=this.value; apply()"><option value="">All workspaces</option></select>
  <button onclick="setAll(false)">Expand</button>
  <button onclick="setAll(true)">Collapse</button>
  <button id="heatbtn" onclick="toggleHeat()">Heatmap: off</button>
  <button onclick="resetFilters()">Reset</button>
  <span class="heat-legend" id="heatleg"><span>less</span><span class="ramp"></span><span>more data</span></span>
  <span class="status" id="status"></span>
</div>
<div id="tree"></div>
<div class="panel" id="panel">
  <div class="panel-hd"><b id="p-title"></b><span class="x" onclick="hidePanel()">✕</span></div>
  <div class="panel-bd" id="p-body"></div>
</div>
<script>
const DATA = __DATA__;
const TYPES = ['Metastore','Catalog','Schema','Table','View','Volume','Function','Model'];
const TYPE_CLS = {'Metastore':'metastore','Catalog':'cat','Schema':'sch','Table':'tbl','View':'vw','Volume':'vol','Function':'fn','Model':'mdl'};
const TYPE_BADGE = {'Metastore':'M','Catalog':'C','Schema':'S','Table':'T','View':'V','Volume':'B','Function':'ƒ','Model':'◆'};
const TYPE_DOT = {'Metastore':'#1b3139','Catalog':'#ff3621','Schema':'#fb7359','Table':'#00a972','View':'#2272b4','Volume':'#8a63d2','Function':'#e0a800','Model':'#00b0c7'};
const TABLE_SUBTYPE = {'STREAMING_TABLE':'STREAMING','MATERIALIZED_VIEW':'MATERIALIZED','METRIC_VIEW':'METRIC','FOREIGN':'FOREIGN','EXTERNAL':'EXTERNAL','MANAGED_SHALLOW_CLONE':'CLONE'};
const state = { q:'', types:new Set(TYPES), rtype:'', loc:'', fmt:'', ws:'' };
let selected = null;
let HEAT=false, heatLo=0, heatHi=0;

function el(tag, cls, txt){ const e=document.createElement(tag); if(cls)e.className=cls; if(txt!=null)e.textContent=txt; return e; }
function esc(s){ return (s+'').replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
__DETAIL_JS__
function render(node){
  const cls = TYPE_CLS[node.type] || 'tbl';
  const li = el('li', cls); node.__li = li;
  const box = el('div','node'); node.__box = box;
  const tw = el('span','tw');
  const hasKids = node.children && node.children.length;
  if(hasKids){ tw.textContent='▼'; tw.onclick=(e)=>{ e.stopPropagation(); li.classList.toggle('collapsed'); }; }
  else { tw.classList.add('empty'); }
  box.appendChild(tw);
  const badge = el('span','badge', TYPE_BADGE[node.type] || 'T');
  if(node._color) badge.style.background = node._color;
  node.__badge = badge;
  box.appendChild(badge);
  const nameEl = el('span','name', node.name); node.__name = nameEl;
  box.appendChild(nameEl);
  const kids = hasKids ? node.children.length : 0;
  if(kids && (node.type==='Catalog' || node.type==='Metastore')) box.appendChild(el('span','count', kids));
  if(node.type==='Table' || node.type==='View'){
    const sub=TABLE_SUBTYPE[node.table_type]; if(sub) box.appendChild(el('span','meta sub', sub));
    if(node.bytes_h) box.appendChild(el('span','meta sz', node.bytes_h));
    else if(node.rows_h) box.appendChild(el('span','meta sz', node.rows_h+' rows'));
    if(node.n_columns) box.appendChild(el('span','meta', node.n_columns+' col'+(node.n_columns>1?'s':'')));
  }
  else if(node.resource_type && node.type!=='Schema' && node.type!=='Catalog') box.appendChild(el('span','meta', node.resource_type));
  box.onclick = ()=>select(node);
  li.appendChild(box);
  if(hasKids){
    const ul = el('ul');
    node.children.forEach((c,i)=>{
      const cli = render(c);
      cli.classList.add('has-sib');
      if(i===node.children.length-1) cli.classList.add('last');
      ul.appendChild(cli);
    });
    li.appendChild(ul);
  }
  return li;
}
(function(){ const u=el('ul'); u.appendChild(render(DATA)); document.getElementById('tree').appendChild(u);
  // collapse below catalog level by default so big metastores open fast
  document.querySelectorAll('#tree li.sch').forEach(li=>{ if(li.querySelector(':scope > ul')) li.classList.add('collapsed'); });
})();

(function init(){
  const rtypes=new Set(), owners=new Set(), fmts=new Set();
  (function scan(n){ if(n.resource_type) rtypes.add(n.resource_type); if(n.location) owners.add(n.location);
    if(n.data_source_format) fmts.add(n.data_source_format); (n.children||[]).forEach(scan); })(DATA);
  const rt=document.getElementById('rtype');
  [...rtypes].sort().forEach(t=>{ const o=document.createElement('option'); o.value=t; o.textContent=t; rt.appendChild(o); });
  const lc=document.getElementById('loc');
  [...owners].sort().forEach(t=>{ const o=document.createElement('option'); o.value=t; o.textContent=t; lc.appendChild(o); });
  const fm=document.getElementById('fmt');
  [...fmts].sort().forEach(t=>{ const o=document.createElement('option'); o.value=t; o.textContent=t; fm.appendChild(o); });
  const wsf=document.getElementById('wsfilter');
  if(DATA.workspaces && DATA.workspaces.length>1){
    DATA.workspaces.forEach((name,i)=>{ const o=document.createElement('option'); o.value=String(i); o.textContent=name; wsf.appendChild(o); });
    wsf.style.display='';
  }
  const chips=document.getElementById('chips');
  TYPES.forEach(t=>{
    const c=el('span','chip'); c.dataset.t=t;
    c.innerHTML='<i class="d" style="background:'+TYPE_DOT[t]+'"></i>'+t;
    c.onclick=()=>{ if(state.types.has(t)){ state.types.delete(t); c.classList.add('off'); } else { state.types.add(t); c.classList.remove('off'); } apply(); };
    chips.appendChild(c);
  });
})();

function leafMatch(n){
  if(!state.types.has(n.type)) return false;
  if(state.rtype && n.resource_type!==state.rtype) return false;
  if(state.loc && n.location!==state.loc) return false;
  if(state.fmt && n.data_source_format!==state.fmt) return false;
  if(state.ws!=='' && !((n.w||[]).includes(+state.ws))) return false;
  if(state.q){
    const hay=(n.name+' '+(n.type||'')+' '+(n.resource_type||'')+' '+(n.owner||'')+' '+(n.full_name||'')).toLowerCase();
    if(!hay.includes(state.q)) return false;
  }
  return true;
}
function compute(n){
  let self=leafMatch(n), kid=false;
  (n.children||[]).forEach(c=>{ if(compute(c)) kid=true; });
  n.__self=self; n.__vis=self||kid; return n.__vis;
}
function highlight(n){
  const e=n.__name; if(!e) return;
  if(state.q && n.__self){
    const name=n.name, i=name.toLowerCase().indexOf(state.q);
    if(i>=0){ e.innerHTML=esc(name.slice(0,i))+'<mark>'+esc(name.slice(i,i+state.q.length))+'</mark>'+esc(name.slice(i+state.q.length)); return; }
  }
  e.textContent=n.name;
}
function apply(){
  compute(DATA);
  const filtering = !!(state.q || state.rtype || state.loc || state.fmt || state.ws!=='' || state.types.size<TYPES.length);
  const shown={};
  (function walk(n){
    n.__li.classList.toggle('hide', !n.__vis);
    if(n.__self) shown[n.type]=(shown[n.type]||0)+1;
    if(filtering && n.__vis && n.children && n.children.length) n.__li.classList.remove('collapsed');
    highlight(n);
    (n.children||[]).forEach(walk);
  })(DATA);
  const st=document.getElementById('status');
  if(filtering){
    const parts=TYPES.filter(t=>shown[t]).map(t=>shown[t]+' '+t.toLowerCase()+(shown[t]>1?'s':''));
    st.textContent='Matches: '+(parts.length?parts.join(' · '):'none');
  } else st.textContent='';
}
function onSearch(v){ state.q=(v||'').trim().toLowerCase(); apply(); }
function setAll(collapsed){ document.querySelectorAll('#tree li').forEach(li=>{ if(li.querySelector(':scope > ul')) li.classList.toggle('collapsed', collapsed); }); }
function resetFilters(){
  state.q=''; state.rtype=''; state.loc=''; state.fmt=''; state.ws=''; state.types=new Set(TYPES);
  document.getElementById('q').value=''; document.getElementById('rtype').value=''; document.getElementById('loc').value=''; document.getElementById('fmt').value='';
  document.getElementById('wsfilter').value='';
  document.querySelectorAll('.chip').forEach(c=>c.classList.remove('off'));
  apply();
}
function heatColor(b){
  if(!b) return '#cfd8e3';
  const t=Math.max(0,Math.min(1,(Math.log10(1+b)-heatLo)/((heatHi-heatLo)||1)));
  const stops=[[0,169,114],[224,168,0],[211,79,55]];
  const lo=t<0.5?stops[0]:stops[1], hi=t<0.5?stops[1]:stops[2], f=t<0.5?t*2:(t-0.5)*2;
  const mix=i=>Math.round(lo[i]+(hi[i]-lo[i])*f);
  return 'rgb('+mix(0)+','+mix(1)+','+mix(2)+')';
}
function toggleHeat(){
  HEAT=!HEAT;
  document.getElementById('heatbtn').textContent='Heatmap: '+(HEAT?'on':'off');
  document.getElementById('heatleg').classList.toggle('on',HEAT);
  if(HEAT && heatHi<=heatLo){
    let lo=Infinity, hi=-Infinity;
    (function scan(n){ if((n.type==='Table'||n.type==='View') && n.bytes){ const l=Math.log10(1+n.bytes); if(l<lo)lo=l; if(l>hi)hi=l; } (n.children||[]).forEach(scan); })(DATA);
    if(lo!==Infinity){ heatLo=lo; heatHi=hi; }
  }
  (function paint(n){ if(n.__badge){
      if(HEAT && (n.type==='Table'||n.type==='View')) n.__badge.style.background = n.bytes?heatColor(n.bytes):'#cfd8e3';
      else if(n._color) n.__badge.style.background=n._color;
    } (n.children||[]).forEach(paint); })(DATA);
}
function select(n){
  if(selected) selected.__box.classList.remove('sel');
  selected=n; n.__box.classList.add('sel');
  document.getElementById('p-title').textContent=n.name;
  document.getElementById('p-body').innerHTML=panelBody(n);
  document.getElementById('panel').classList.add('open');
}
function hidePanel(){ document.getElementById('panel').classList.remove('open'); if(selected){ selected.__box.classList.remove('sel'); selected=null; } }
document.addEventListener('keydown', e=>{ if(e.key==='Escape') hidePanel(); });
apply();
</script>
</body>
</html>
""".replace("__LEGEND__", _LEGEND).replace("__PANEL_CSS__", _PANEL_CSS).replace("__DETAIL_JS__", _DETAIL_JS)


_STAR_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --bg:#f6f8fb; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:var(--bg); color:#1b2733; }
  header { background:#fff; padding:16px 24px; border-bottom:1px solid #e3e9f1; position:sticky; top:0; z-index:6;
           display:flex; align-items:center; gap:18px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  .stats { font-size:13px; color:#5a6b7d; }
  .stats b { color:#1b2733; }
  .legend { display:flex; gap:14px; font-size:12px; color:#5a6b7d; align-items:center; margin-left:auto; flex-wrap:wrap; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .dot { width:12px; height:12px; border-radius:3px; }
  .controls { background:#fff; border-bottom:1px solid #e3e9f1; padding:10px 24px; position:sticky; top:58px; z-index:5;
              display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .controls input[type=search] { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; width:240px; }
  .controls select, .controls button { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; background:#fff; cursor:pointer; }
  .controls button:hover { background:#eef3fb; }
  .hint { font-size:12px; color:#8a97a6; }
  .heat-legend { display:none; align-items:center; gap:6px; font-size:11px; color:#5a6b7d; }
  .heat-legend.on { display:inline-flex; }
  .heat-legend .ramp { width:110px; height:10px; border-radius:5px; background:linear-gradient(90deg,#00a972,#e0a800,#d34f37); }
  .status { font-size:12px; color:#5a6b7d; margin-left:auto; }
  #wrap { display:flex; justify-content:center; padding:18px; }
  svg { max-width:100%; height:auto; }
  .seg { stroke:#fff; stroke-width:1; cursor:pointer; transition:filter .12s, opacity .12s; }
  .seg:hover { filter:brightness(1.10); }
  .seg.dim { opacity:.10; }
  .seg.hit { stroke:#13315b; stroke-width:1.6; }
  .lbl { font-size:10.5px; fill:#13315b; pointer-events:none; }
  .lbl.ondark { fill:#fff; }
  .center-name { font-size:14px; font-weight:700; fill:#fff; pointer-events:none; }
  .center-sub { font-size:11px; fill:#cfe0f6; pointer-events:none; }
__PANEL_CSS__
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="stats">__STATS__</div>
  __LEGEND__
</header>
__NAV__
<div class="controls">
  <input type="search" id="q" placeholder="Search & highlight…" oninput="onSearch(this.value)">
  <select id="weight" onchange="WEIGHT=this.value; render()">
    <option value="count">Ring size: equal</option>
    <option value="bytes">Ring size: data volume</option>
    <option value="rows">Ring size: row count</option>
  </select>
  <button id="heatbtn" onclick="toggleHeat()">Heatmap: off</button>
  <span class="heat-legend" id="heatleg"><span>less</span><span class="ramp"></span><span>more data</span></span>
  <span class="hint">Click a ring to zoom in · centre to zoom out</span>
  <span class="status" id="status"></span>
</div>
<div id="wrap"><svg id="star"></svg></div>
__PANEL_HTML__
<script>
const DATA = __DATA__;
const SVGNS = "http://www.w3.org/2000/svg";
const R0 = 58, RING = 64, TWO = Math.PI * 2;
(function setParents(n, par){ n.__parent = par || null; (n.children || []).forEach(c => setParents(c, n)); })(DATA, null);
let focus = DATA, q = '', allPaths = [], nameT, subT;
let WEIGHT='count', HEAT=false, heatLo=0, heatHi=0;
const svg = document.getElementById('star');
function esc(s){ return (s+'').replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
__DETAIL_JS__
function showDetail(n){ document.getElementById('p-title').textContent=n.name;
  document.getElementById('p-body').innerHTML=panelBody(n); document.getElementById('panel').classList.add('open'); }
function hidePanel(){ document.getElementById('panel').classList.remove('open'); }
document.addEventListener('keydown', e=>{ if(e.key==='Escape') hidePanel(); });
function leafWeight(n){
  if(WEIGHT==='bytes') return Math.max(1, Math.log10(1 + (+n.bytes||0)) * 1000);
  if(WEIGHT==='rows')  return Math.max(1, +n.rows||0);
  return 1;
}
function heatColor(b){
  if(!b) return '#cfd8e3';
  const t=Math.max(0,Math.min(1,(Math.log10(1+b)-heatLo)/((heatHi-heatLo)||1)));
  const stops=[[0,169,114],[224,168,0],[211,79,55]];
  const lo=t<0.5?stops[0]:stops[1], hi=t<0.5?stops[1]:stops[2], f=t<0.5?t*2:(t-0.5)*2;
  const mix=i=>Math.round(lo[i]+(hi[i]-lo[i])*f);
  return 'rgb('+mix(0)+','+mix(1)+','+mix(2)+')';
}
function toggleHeat(){
  HEAT=!HEAT;
  document.getElementById('heatbtn').textContent='Heatmap: '+(HEAT?'on':'off');
  document.getElementById('heatleg').classList.toggle('on',HEAT);
  if(HEAT && heatHi<=heatLo){
    let lo=Infinity, hi=-Infinity;
    (function scan(n){ if((n.type==='Table'||n.type==='View') && n.bytes){ const l=Math.log10(1+n.bytes); if(l<lo)lo=l; if(l>hi)hi=l; } (n.children||[]).forEach(scan); })(DATA);
    if(lo!==Infinity){ heatLo=lo; heatHi=hi; }
  }
  render();
}
function leaves(n){ if(!n.children || !n.children.length){ return (n._lv = leafWeight(n)); } let s = 0; n.children.forEach(c => s += leaves(c)); return (n._lv = s); }
function depthOf(n){ let m = 0; (n.children || []).forEach(c => m = Math.max(m, 1 + depthOf(c))); return m; }
function assign(n, a0, a1, d){ n._a0 = a0; n._a1 = a1; n._depth = d; let a = a0;
  (n.children || []).forEach(c => { const span = (a1 - a0) * c._lv / n._lv; assign(c, a, a + span, d + 1); a += span; }); }
const px = (r, a, C) => C + r * Math.sin(a);
const py = (r, a, C) => C - r * Math.cos(a);
function seg(ri, ro, a0, a1, C){ const big = (a1 - a0) > Math.PI ? 1 : 0;
  return `M${px(ro,a0,C)} ${py(ro,a0,C)} A${ro} ${ro} 0 ${big} 1 ${px(ro,a1,C)} ${py(ro,a1,C)}`
       + ` L${px(ri,a1,C)} ${py(ri,a1,C)} A${ri} ${ri} 0 ${big} 0 ${px(ri,a0,C)} ${py(ri,a0,C)} Z`; }
function ringPath(ri, ro, a0, a1, C){
  if(a1 - a0 >= TWO - 1e-6){ const m = a0 + Math.PI; return seg(ri, ro, a0, m, C) + ' ' + seg(ri, ro, m, a0 + TWO, C); }
  return seg(ri, ro, a0, a1, C); }
function fill(n){ if(HEAT && (n.type==='Table'||n.type==='View')) return n.bytes?heatColor(n.bytes):'#cfd8e3'; return n._color || '#cfe0f6'; }
function shortName(n){ const p = n.__parent; return (p && n.name.indexOf(p.name + '.') === 0) ? n.name.slice(p.name.length + 1) : n.name; }
function mkText(x, y, cls){ const t = document.createElementNS(SVGNS, 'text'); t.setAttribute('x', x); t.setAttribute('y', y);
  t.setAttribute('text-anchor', 'middle'); t.setAttribute('class', cls); svg.appendChild(t); return t; }
function setCenter(n){
  nameT.textContent = n.name.length > 24 ? n.name.slice(0, 23) + '…' : n.name;
  const lead = n.__parent ? '↩ ' : '';
  const kids = (n.children || []).length;
  subT.textContent = lead + (kids ? (kids + ' ' + childNoun(n) + (kids > 1 ? 's' : '')) : n.type);
}
function childNoun(n){ const k = (n.children || [])[0]; if(!k) return n.type;
  return ({'Catalog':'schema','Schema':'object','Metastore':'catalog'}[n.type]) || 'item'; }
function render(){
  leaves(focus); const MAXD = depthOf(focus) || 1; assign(focus, 0, TWO, 0);
  const OUTER = R0 + MAXD * RING, MARGIN = 150, D = 2 * (OUTER + MARGIN), C = D / 2;
  svg.setAttribute('viewBox', `0 0 ${D} ${D}`); svg.setAttribute('width', D); svg.setAttribute('height', D);
  while(svg.firstChild) svg.removeChild(svg.firstChild);
  allPaths = [];
  (function draw(n){
    if(n._depth > 0){
      const ri = R0 + (n._depth - 1) * RING, ro = R0 + n._depth * RING;
      const dark = n._dark;
      const path = document.createElementNS(SVGNS, 'path');
      path.setAttribute('d', ringPath(ri, ro, n._a0, n._a1, C));
      path.setAttribute('class', 'seg'); path.setAttribute('fill', fill(n));
      path.__node = n; allPaths.push(path);
      path.addEventListener('mouseenter', () => setCenter(n));
      path.addEventListener('mouseleave', () => setCenter(focus));
      path.addEventListener('click', () => { if(n.children && n.children.length){ focus = n; render(); } else { showDetail(n); } });
      const title = document.createElementNS(SVGNS, 'title');
      title.textContent = n.name + (n.resource_type ? '  (' + n.resource_type + ')' : '');
      path.appendChild(title); svg.appendChild(path);
      const sweep = n._a1 - n._a0, mid = (n._a0 + n._a1) / 2, full = sweep >= TWO - 1e-6, rmid = (ri + ro) / 2;
      if(full || sweep * rmid > 22){
        let txt = shortName(n); const maxlen = full ? 22 : 15; if(txt.length > maxlen) txt = txt.slice(0, maxlen - 1) + '…';
        const t = document.createElementNS(SVGNS, 'text'); t.setAttribute('class', 'lbl' + (dark ? ' ondark' : ''));
        t.setAttribute('text-anchor', 'middle'); t.setAttribute('dominant-baseline', 'middle');
        if(full){ t.setAttribute('x', C); t.setAttribute('y', C - rmid); }
        else { const x = px(rmid, mid, C), y = py(rmid, mid, C); let deg = mid * 180 / Math.PI - 90;
          let dd = ((deg % 360) + 360) % 360; if(dd > 90 && dd < 270) deg += 180;
          t.setAttribute('x', x); t.setAttribute('y', y); t.setAttribute('transform', `rotate(${deg} ${x} ${y})`); }
        t.textContent = txt; svg.appendChild(t);
      }
    }
    (n.children || []).forEach(draw);
  })(focus);
  const disc = document.createElementNS(SVGNS, 'circle');
  disc.setAttribute('cx', C); disc.setAttribute('cy', C); disc.setAttribute('r', R0);
  disc.setAttribute('fill', '#13315b'); disc.style.cursor = focus.__parent ? 'pointer' : 'default';
  disc.addEventListener('click', () => { if(focus.__parent){ focus = focus.__parent; render(); } });
  svg.appendChild(disc);
  nameT = mkText(C, C - 5, 'center-name'); subT = mkText(C, C + 13, 'center-sub');
  setCenter(focus);
  applySearch();
}
function subtreeMatch(n){ return n.name.toLowerCase().includes(q) || (n.children || []).some(subtreeMatch); }
function applySearch(){
  let hits = 0;
  allPaths.forEach(p => {
    const n = p.__node;
    if(!q){ p.classList.remove('dim', 'hit'); return; }
    if(n.name.toLowerCase().includes(q)){ p.classList.add('hit'); p.classList.remove('dim'); hits++; }
    else if(subtreeMatch(n)){ p.classList.remove('dim', 'hit'); }
    else { p.classList.add('dim'); p.classList.remove('hit'); }
  });
  document.getElementById('status').textContent = q ? (hits + ' match' + (hits === 1 ? '' : 'es')) : '';
}
function onSearch(v){ q = (v || '').trim().toLowerCase(); applySearch(); }
render();
</script>
</body>
</html>
""".replace("__LEGEND__", _LEGEND).replace("__PANEL_CSS__", _PANEL_CSS).replace("__PANEL_HTML__", _PANEL_HTML).replace("__DETAIL_JS__", _DETAIL_JS)


_HUB_RAW = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --hub:#13315b; --bg:#f6f8fb; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:var(--bg); color:#1b2733; }
  header { background:#fff; padding:16px 24px; border-bottom:1px solid #e3e9f1; position:sticky; top:0; z-index:6;
           display:flex; align-items:center; gap:18px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  .stats { font-size:13px; color:#5a6b7d; }
  .stats b { color:#1b2733; }
  .legend { display:flex; gap:14px; font-size:12px; color:#5a6b7d; align-items:center; margin-left:auto; flex-wrap:wrap; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .dot { width:12px; height:12px; border-radius:3px; }
  .controls { background:#fff; border-bottom:1px solid #e3e9f1; padding:10px 24px; position:sticky; top:58px; z-index:5;
              display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .controls input[type=search] { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; width:280px; }
  .controls button { font:inherit; font-size:13px; padding:6px 12px; border:1px solid #cdd8e6; background:#fff; border-radius:6px; cursor:pointer; }
  .controls button:hover { background:#eef3fb; }
  .controls button:disabled { opacity:.45; cursor:default; }
  .controls select { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; background:#fff; max-width:260px; }
  .crumbs { font-size:12px; color:#5a6b7d; display:flex; gap:6px; align-items:center; flex-wrap:wrap; }
  .crumbs a { color:#c41e0c; cursor:pointer; text-decoration:none; }
  .crumbs a:hover { text-decoration:underline; }
  .crumbs .sep { color:#b8c4d4; }
  .hint { font-size:12px; color:#8a97a6; }
  .status { font-size:12px; color:#5a6b7d; margin-left:auto; }
  #wrap { display:flex; justify-content:center; padding:18px; }
  svg { max-width:100%; height:auto; }
  .spoke { stroke:#8ea7c8; stroke-width:1.8; }
  .twig  { stroke:#cdd8e6; stroke-width:1; }
  .node rect { transition:filter .12s, opacity .12s; }
  .node { cursor:pointer; }
  .node.leaf { cursor:pointer; }
  .node:hover rect { filter:brightness(1.08); }
  .node text { pointer-events:none; }
  .node.dim { opacity:.16; }
  .node.hit rect { stroke:#13315b; stroke-width:2; }
  .node rect { stroke:#b9cdec; }
  .node text { fill:#13315b; font-size:10.5px; }
  .hub rect { fill:var(--hub); stroke:#0b2444; cursor:pointer; }
  .hub .h1 { fill:#fff; font-size:13px; font-weight:700; }
  .hub .h2 { fill:#9fb6d6; font-size:9px; }
  .hub .h3 { fill:#cfe0f6; font-size:10px; }
__PANEL_CSS__
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="stats">__STATS__</div>
  __LEGEND__
</header>
__NAV__
<div class="controls">
  <input type="search" id="q" placeholder="Search & highlight…" oninput="onSearch(this.value)">
  <button id="up" onclick="up()">↑ Up</button>
  <button onclick="home()">Home</button>
  <select id="focus" onchange="jump(this.value)" title="Jump to a node"></select>
  <span class="crumbs" id="crumbs"></span>
  <span class="hint">Click a node to drill in · click the centre (or Up) to go back</span>
  <span class="status" id="status"></span>
</div>
<div id="wrap"><svg id="graph"></svg></div>
__PANEL_HTML__
<script>
const DATA = __DATA__;
const SVGNS = "http://www.w3.org/2000/svg";
const TWO = Math.PI * 2;
const R1 = 300, R2 = 600, HUBW = 252, HUBH = 70, MARGIN = 130;
const D = 2 * (R2 + MARGIN), C = D / 2;
const svg = document.getElementById('graph');
svg.setAttribute('viewBox', `0 0 ${D} ${D}`); svg.setAttribute('width', D); svg.setAttribute('height', D);
const px = (r, a) => C + r * Math.sin(a);
const py = (r, a) => C - r * Math.cos(a);
function esc(s){ return (s+'').replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
__DETAIL_JS__
function showDetail(n){ document.getElementById('p-title').textContent=n.name;
  document.getElementById('p-body').innerHTML=panelBody(n); document.getElementById('panel').classList.add('open'); }
function hidePanel(){ document.getElementById('panel').classList.remove('open'); }
document.addEventListener('keydown', e=>{ if(e.key==='Escape') hidePanel(); });
(function setParents(n, p){ n.__parent = p || null; (n.children || []).forEach(c => setParents(c, n)); })(DATA, null);
let focus = DATA, q = '', pills = [];
const NODES = [];
(function idx(n){ n.__idx = NODES.length; NODES.push(n); (n.children || []).forEach(idx); })(DATA);
(function buildFocusSelect(){
  const sel = document.getElementById('focus');
  const def = document.createElement('option'); def.value = ''; def.textContent = 'Jump to…'; sel.appendChild(def);
  (__FOCUS_GROUPS__).forEach(g => {
    const og = document.createElement('optgroup'); og.label = g[1];
    NODES.filter(n => n.type === g[0] && n.children && n.children.length)
         .sort((a, b) => a.name.localeCompare(b.name))
         .forEach(n => { const o = document.createElement('option'); o.value = n.__idx; o.textContent = n.name + ' (' + n.children.length + ')'; og.appendChild(o); });
    if(og.children.length) sel.appendChild(og);
  });
})();
function jump(v){ if(v === '') return; focus = NODES[parseInt(v, 10)]; render(); }

function foldFrom(n){ let h = n, crumb = [h]; while(h.children && h.children.length === 1){ h = h.children[0]; crumb.push(h); } return { hub: h, crumb }; }
function short(name, parent){ return (parent && name.indexOf(parent + '.') === 0) ? name.slice(parent.length + 1) : name; }
function line(x1, y1, x2, y2, cls){
  const l = document.createElementNS(SVGNS, 'line');
  l.setAttribute('x1', x1); l.setAttribute('y1', y1); l.setAttribute('x2', x2); l.setAttribute('y2', y2);
  l.setAttribute('class', cls); return l;
}
function pill(x, y, text, node){
  const drillable = node.children && node.children.length;
  const g = document.createElementNS(SVGNS, 'g'); g.setAttribute('class', 'node' + (drillable ? '' : ' leaf'));
  const w = Math.max(34, text.length * 6.7 + 16), h = 22;
  const r = document.createElementNS(SVGNS, 'rect');
  r.setAttribute('x', x - w / 2); r.setAttribute('y', y - h / 2);
  r.setAttribute('width', w); r.setAttribute('height', h); r.setAttribute('rx', 11);
  if(node._color){ r.style.fill = node._color; r.style.stroke = node._stroke; }
  g.appendChild(r);
  const t = document.createElementNS(SVGNS, 'text');
  t.setAttribute('x', x); t.setAttribute('y', y);
  t.setAttribute('text-anchor', 'middle'); t.setAttribute('dominant-baseline', 'central');
  if(node._color) t.style.fill = node._dark ? '#fff' : '#13315b';
  t.textContent = text; g.appendChild(t);
  const tip = document.createElementNS(SVGNS, 'title');
  tip.textContent = node.name + (node.resource_type ? '  (' + node.resource_type + ')' : '') + (drillable ? '  — click to drill in' : '');
  g.appendChild(tip);
  g.addEventListener('click', () => { if(drillable){ focus = node; render(); } else { showDetail(node); } });
  g.__node = node; pills.push(g);
  return g;
}
function childLabel(n){
  const k = n.children || []; if(!k.length) return '';
  const noun = (__CHILD_NOUNS__)[k[0].type] || 'item';
  return k.length + ' ' + noun + (k.length > 1 ? 's' : '');
}
function render(){
  while(svg.firstChild) svg.removeChild(svg.firstChild);
  pills = [];
  const { hub } = foldFrom(focus);
  const workloads = hub.children || [];
  const lines = document.createElementNS(SVGNS, 'g'), nodes = document.createElementNS(SVGNS, 'g');
  const weight = w => Math.max(1, (w.children || []).length);
  let total = 0; workloads.forEach(w => total += weight(w));
  let acc = 0;
  workloads.forEach(w => {
    const frac = weight(w) / total, a0 = acc * TWO, a1 = (acc + frac) * TWO; acc += frac;
    const aMid = (a0 + a1) / 2, wx = px(R1, aMid), wy = py(R1, aMid);
    lines.appendChild(line(C, C, wx, wy, 'spoke'));
    const subs = w.children || [];
    subs.forEach((s, i) => {
      const sa = a0 + (i + 0.5) / subs.length * (a1 - a0), sx = px(R2, sa), sy = py(R2, sa);
      lines.appendChild(line(wx, wy, sx, sy, 'twig'));
      const cc = (s.children || []).length;
      nodes.appendChild(pill(sx, sy, short(s.name, w.name) + (cc ? ' (' + cc + ')' : ''), s));
    });
    const wc = (w.children || []).length;
    nodes.appendChild(pill(wx, wy, short(w.name, hub.name) + (wc ? ' (' + wc + ')' : ''), w));
  });
  svg.appendChild(lines); svg.appendChild(nodes);
  const hg = document.createElementNS(SVGNS, 'g'); hg.setAttribute('class', 'hub');
  const hr = document.createElementNS(SVGNS, 'rect');
  hr.setAttribute('x', C - HUBW / 2); hr.setAttribute('y', C - HUBH / 2);
  hr.setAttribute('width', HUBW); hr.setAttribute('height', HUBH); hr.setAttribute('rx', 14);
  if(hub.__parent) hr.addEventListener('click', () => { focus = hub.__parent; render(); });
  hg.appendChild(hr);
  const parentName = hub.__parent ? hub.__parent.name : '';
  function hubText(dy, cls, txt){
    const t = document.createElementNS(SVGNS, 'text');
    t.setAttribute('x', C); t.setAttribute('y', C + dy);
    t.setAttribute('text-anchor', 'middle'); t.setAttribute('class', cls);
    t.textContent = txt; hg.appendChild(t);
  }
  if(parentName) hubText(-18, 'h2', '↑ ' + parentName);
  hubText(2, 'h1', hub.name.length > 30 ? hub.name.slice(0, 29) + '…' : hub.name);
  hubText(20, 'h3', childLabel(hub) || hub.type);
  svg.appendChild(hg);
  document.getElementById('up').disabled = !hub.__parent;
  const fsel = document.getElementById('focus'); if(fsel) fsel.value = hub.__idx;
  renderCrumbs(hub);
  applySearch();
}
function renderCrumbs(hub){
  const path = []; let p = hub; while(p){ path.unshift(p); p = p.__parent; }
  const box = document.getElementById('crumbs'); box.innerHTML = '';
  path.forEach((n, i) => {
    if(i) { const s = document.createElement('span'); s.className = 'sep'; s.textContent = '›'; box.appendChild(s); }
    if(n === hub){ const b = document.createElement('b'); b.textContent = n.name; box.appendChild(b); }
    else { const a = document.createElement('a'); a.textContent = n.name; a.onclick = () => { focus = n; render(); }; box.appendChild(a); }
  });
}
function up(){ const { hub } = foldFrom(focus); if(hub.__parent){ focus = hub.__parent; render(); } }
function home(){ focus = DATA; render(); }
function onSearch(v){ q = (v || '').trim().toLowerCase(); applySearch(); }
function applySearch(){
  let hits = 0;
  pills.forEach(g => {
    const n = g.__node;
    if(!q){ g.classList.remove('dim', 'hit'); return; }
    if(n.name.toLowerCase().includes(q)){ g.classList.add('hit'); g.classList.remove('dim'); hits++; }
    else { g.classList.add('dim'); g.classList.remove('hit'); }
  });
  document.getElementById('status').textContent = q ? (hits + ' match' + (hits === 1 ? '' : 'es') + ' on this level') : '';
}
render();
</script>
</body>
</html>
""".replace("__PANEL_CSS__", _PANEL_CSS).replace("__PANEL_HTML__", _PANEL_HTML).replace("__DETAIL_JS__", _DETAIL_JS)


# UC catalog hub: focus jump-groups + child-type nouns.
_HUB_CHILD_NOUNS = ("{'Catalog':'catalog','Schema':'schema','Table':'object','View':'object',"
                    "'Volume':'object','Function':'object','Model':'object'}")
_HUB_TEMPLATE = (_HUB_RAW
                 .replace("__LEGEND__", _LEGEND)
                 .replace("__FOCUS_GROUPS__", "[['Catalog','Catalogs'],['Schema','Schemas']]")
                 .replace("__CHILD_NOUNS__", _HUB_CHILD_NOUNS))


# ─────────────────────────────────────────────────────────────────────────────
# 4. Infrastructure topology  (Metastore → Storage account → External location;
#    + Storage credentials & Connections)  — reuses the hub renderer.
# ─────────────────────────────────────────────────────────────────────────────

_TOPO_LEGEND = (
    '<div class="legend">'
    '<span><i class="dot" style="background:#5a6b7d"></i> Category</span>'
    '<span><i class="dot" style="background:#2f6fb0"></i> Storage account</span>'
    '<span><i class="dot" style="background:#0a9396"></i> External location</span>'
    '<span><i class="dot" style="background:#8a63d2"></i> Storage credential</span>'
    '<span><i class="dot" style="background:#e0a800"></i> Connection</span>'
    '</div>'
)

_TOPO_CHILD_NOUNS = ("{'Group':'category','StorageAccount':'storage account',"
                     "'ExternalLocation':'external location','StorageCredential':'credential',"
                     "'Connection':'connection'}")
_TOPO_TEMPLATE = (_HUB_RAW
                  .replace("__LEGEND__", _TOPO_LEGEND)
                  .replace("__FOCUS_GROUPS__", "[['Group','Categories'],['StorageAccount','Storage accounts']]")
                  .replace("__CHILD_NOUNS__", _TOPO_CHILD_NOUNS))


_UNMAPPED = "Unmapped / non-Azure"


def _ext_loc_node(e: dict) -> dict:
    az = e.get("azure") or {}
    return {
        "name": e.get("name", ""),
        "type": "ExternalLocation",
        "full_name": e.get("name", ""),
        "url": _short(e.get("url", "")),
        "credential_name": e.get("credential_name", ""),
        "read_only": e.get("read_only"),
        "owner": e.get("owner", ""),
        "container": az.get("container", ""),
        "resource_type": "External location",
        "location": e.get("owner", ""),
    }


def build_uc_topology(inv: Any) -> dict:
    """Build the metastore-level securable topology (storage accounts ← external
    locations, storage credentials, connections) as a shallow hub tree."""
    d = _as_dict(inv)
    az = d.get("azure") or {}
    accounts = az.get("storage_accounts") or []

    # Group external locations by their resolved Azure storage account.
    locs_by_account: dict[str, list] = defaultdict(list)
    for e in d.get("external_locations") or []:
        acct = ((e.get("azure") or {}).get("storage_account") or "").strip()
        locs_by_account[acct or _UNMAPPED].append(_ext_loc_node(e))

    account_nodes = []
    for a in accounts:
        name = a.get("name", "")
        account_nodes.append({
            "name": name,
            "type": "StorageAccount",
            "full_name": a.get("resource_id", "") or name,
            "resource_group": a.get("resource_group", ""),
            "subscription_id": a.get("subscription_id", ""),
            "region": a.get("location", ""),
            "sku": a.get("sku", ""),
            "kind": a.get("kind", ""),
            "hns_enabled": a.get("hns_enabled"),
            "public_network_access": a.get("public_network_access", ""),
            "network_default_action": a.get("network_default_action", ""),
            "resource_type": "Storage account",
            "location": a.get("location", ""),
            "children": locs_by_account.pop(name, []),
        })
    # External locations whose account wasn't in the Azure inventory (or non-Azure).
    leftover = [loc for locs in locs_by_account.values() for loc in locs]
    if leftover:
        account_nodes.append({
            "name": _UNMAPPED, "type": "StorageAccount", "resource_type": "Storage account",
            "children": leftover,
        })

    cred_nodes = [{
        "name": c.get("name", ""), "type": "StorageCredential",
        "full_name": c.get("name", ""), "owner": c.get("owner", ""),
        "comment": _short(c.get("comment", "")),
        "resource_type": "Storage credential", "location": c.get("owner", ""),
    } for c in (d.get("storage_credentials") or [])]

    conn_nodes = [{
        "name": c.get("name", ""), "type": "Connection",
        "full_name": c.get("name", ""), "owner": c.get("owner", ""),
        "connection_type": c.get("connection_type", "") or c.get("type", ""),
        "comment": _short(c.get("comment", "")),
        "resource_type": "Connection", "location": c.get("owner", ""),
    } for c in (d.get("connections") or [])]

    groups = []
    if account_nodes:
        groups.append({"name": "Storage Accounts", "type": "Group",
                       "resource_type": "Category", "children": account_nodes})
    if cred_nodes:
        groups.append({"name": "Storage Credentials", "type": "Group",
                       "resource_type": "Category", "children": cred_nodes})
    if conn_nodes:
        groups.append({"name": "Connections", "type": "Group",
                       "resource_type": "Category", "children": conn_nodes})

    root = {
        "name": _metastore_name(d),
        "type": METASTORE,
        "workspace": d.get("workspace_name", ""),
        "resource_type": "Metastore",
        "children": groups,
    }
    assign_colours(root)
    return root


def build_fleet_topology(inventories: list[Any]) -> dict:
    """Union the metastore-level securable topology across every workspace in a
    fleet (storage accounts, external locations, storage credentials, connections)
    — deduped by name — into one infrastructure diagram."""
    sa_by_name: dict[str, dict] = {}
    el_by_name: dict[str, dict] = {}
    sc_by_name: dict[str, dict] = {}
    cn_by_name: dict[str, dict] = {}
    for inv in inventories:
        d = _as_dict(inv)
        for a in ((d.get("azure") or {}).get("storage_accounts") or []):
            sa_by_name.setdefault(a.get("name", ""), a)
        for e in (d.get("external_locations") or []):
            name = e.get("name", "")
            cur = el_by_name.get(name)
            if cur is None:
                el_by_name[name] = dict(e)            # copy — never mutate the source
            elif not (cur.get("azure") or {}).get("storage_account") and (e.get("azure") or {}).get("storage_account"):
                cur["azure"] = e.get("azure")         # fill a missing Azure mapping
        for c in (d.get("storage_credentials") or []):
            sc_by_name.setdefault(c.get("name", ""), c)
        for c in (d.get("connections") or []):
            cn_by_name.setdefault(c.get("name", ""), c)

    combined = {
        "workspace_name": "Fleet",
        "metastore": {},
        "azure": {"storage_accounts": list(sa_by_name.values())},
        "external_locations": list(el_by_name.values()),
        "storage_credentials": list(sc_by_name.values()),
        "connections": list(cn_by_name.values()),
    }
    root = build_uc_topology(combined)
    root["name"] = "Fleet Infrastructure"
    return root


def _topo_stats_line(root: dict) -> str:
    c = count_by_type(root)
    parts = [
        (c.get("StorageAccount", 0), "storage accounts"),
        (c.get("ExternalLocation", 0), "external locations"),
        (c.get("StorageCredential", 0), "storage credentials"),
        (c.get("Connection", 0), "connections"),
    ]
    return " &nbsp;·&nbsp; ".join(f"<b>{n}</b> {label}" for n, label in parts)


def render_topology_html(root: dict, title: str = "Unity Catalog Inventory — Infrastructure",
                         nav: str = "") -> str:
    return (_TOPO_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _topo_stats_line(root))
            .replace("__NAV__", nav)
            .replace("__DATA__", _json_inline(root)))


# ─────────────────────────────────────────────────────────────────────────────
# 5. Overview dashboard  (static, pure-CSS bar charts — landing page)
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_overview(catalog_nodes: list[dict], *, metastore: str = "",
                        workspace: str = "", scanned_at: str = "") -> dict:
    """Aggregate a list of catalog nodes (``Catalog → Schema → object``) into the
    overview dashboard payload. Shared by per-workspace and fleet overviews."""
    by_type = count_by_type({"type": "", "children": catalog_nodes})
    fmt: Counter = Counter()
    ttype: Counter = Counter()
    cat_tables, cat_bytes, sch_tables, sch_bytes, largest = [], [], [], [], []
    total_bytes = 0
    for cat in catalog_nodes:
        c_tbl = c_bytes = 0
        for sch in cat.get("children", []):
            s_tbl = s_bytes = 0
            for obj in sch.get("children", []):
                if obj.get("type") in (TABLE, VIEW):
                    s_tbl += 1
                    fmt[obj.get("data_source_format") or "—"] += 1
                    ttype[obj.get("table_type") or "—"] += 1
                    b = obj.get("bytes") or 0
                    s_bytes += b
                    if b:
                        largest.append((obj.get("full_name", ""), b,
                                        obj.get("bytes_h", ""), obj.get("rows_h", ""),
                                        obj.get("table_type", "")))
            sname = sch.get("full_name") or sch.get("name", "")
            sch_tables.append((sname, s_tbl))
            sch_bytes.append((sname, s_bytes))
            c_tbl += s_tbl
            c_bytes += s_bytes
        cat_tables.append((cat.get("name", ""), c_tbl))
        cat_bytes.append((cat.get("name", ""), c_bytes))
        total_bytes += c_bytes
    largest.sort(key=lambda x: x[1], reverse=True)

    def _topn(pairs, n=10):
        return sorted((p for p in pairs if p[1]), key=lambda x: x[1], reverse=True)[:n]

    def _bytes_top(pairs):
        return [(name, b, _humanize_bytes(b)) for name, b in _topn(pairs)]

    return {
        "metastore": metastore,
        "workspace": workspace,
        "scanned_at": scanned_at,
        "totals": {t: by_type.get(t, 0) for t in
                   (CATALOG, SCHEMA, TABLE, VIEW, VOLUME, FUNCTION, MODEL)},
        "total_bytes_h": _humanize_bytes(total_bytes),
        "by_type": [(t, by_type.get(t, 0)) for t in
                    (CATALOG, SCHEMA, TABLE, VIEW, VOLUME, FUNCTION, MODEL) if by_type.get(t, 0)],
        "by_format": fmt.most_common(),
        "by_table_type": ttype.most_common(),
        "top_catalogs_tables": _topn(cat_tables),
        "top_schemas_tables": _topn(sch_tables),
        "top_catalogs_bytes": _bytes_top(cat_bytes),
        "top_schemas_bytes": _bytes_top(sch_bytes),
        "largest_tables": [{"full_name": fn, "bytes_h": bh, "rows_h": rh, "table_type": tt}
                           for fn, _b, bh, rh, tt in largest[:20]],
    }


def build_overview(inv: Any) -> dict:
    """Aggregate one workspace's UC tree into the overview dashboard payload."""
    tree = build_uc_tree(inv)
    return _aggregate_overview(tree.get("children", []),
                               metastore=tree.get("name", ""),
                               workspace=tree.get("workspace", ""),
                               scanned_at=_as_dict(inv).get("scanned_at", ""))


def build_fleet_overview(inventories: list[Any]) -> dict:
    """Aggregate an entire fleet (grouped by metastore, deduped) into the overview
    dashboard payload — totals match the fleet tree's deduped union."""
    fleet = build_fleet_tree(inventories)
    if fleet.get("type") == "Fleet":
        catalog_nodes = [cat for ms in fleet["children"] for cat in ms.get("children", [])]
        n_ms = len(fleet["children"])
    else:                                  # single metastore: fleet IS the Metastore node
        catalog_nodes = fleet.get("children", [])
        n_ms = 1
    n_ws = len({(_as_dict(inv).get("workspace_name", "") or i)
                for i, inv in enumerate(inventories)})
    summary = f"{n_ws} workspace{'s' if n_ws != 1 else ''} · {n_ms} metastore{'s' if n_ms != 1 else ''}"
    return _aggregate_overview(catalog_nodes, metastore=summary, workspace=summary)


def _bar_rows(rows: list, color: str = "#2f6fb0") -> str:
    """Render ``(label, value, display)`` triples as horizontal CSS bars."""
    if not rows:
        return '<p class="empty">None</p>'
    mx = max((r[1] for r in rows), default=0) or 1
    out = []
    for r in rows:
        label, val = r[0], r[1]
        disp = r[2] if len(r) > 2 else str(val)
        pct = max(2, round(val / mx * 100))
        lab = html.escape(str(label))
        out.append(f'<div class="brow"><span class="blabel" title="{lab}">{lab}</span>'
                   f'<span class="btrack"><span class="bfill" style="width:{pct}%;background:{color}"></span></span>'
                   f'<span class="bval">{html.escape(str(disp))}</span></div>')
    return "".join(out)


def _count_rows(pairs: list) -> list:
    """Normalise ``(label, count)`` pairs to ``(label, count, humanized)`` triples."""
    return [(name, val, _humanize_int(val)) for name, val in pairs]


def render_overview_html(overview: dict, title: str = "Unity Catalog Inventory — Overview",
                         nav: str = "") -> str:
    esc = html.escape
    t = overview.get("totals", {})
    card_keys = [(CATALOG, "Catalogs"), (SCHEMA, "Schemas"), (TABLE, "Tables"),
                 (VIEW, "Views"), (VOLUME, "Volumes"), (FUNCTION, "Functions"), (MODEL, "Models")]
    cards = "".join(
        f'<div class="stat"><div class="stat-v">{t.get(k, 0)}</div><div class="stat-l">{esc(lbl)}</div></div>'
        for k, lbl in card_keys)
    cards += (f'<div class="stat"><div class="stat-v">{esc(overview.get("total_bytes_h", "") or "—")}</div>'
              f'<div class="stat-l">Total Size</div></div>')

    def _panel(titletext, inner):
        return f'<section class="panel"><h2>{esc(titletext)}</h2>{inner}</section>'

    body = []
    body.append(_panel("Objects by type", _bar_rows(_count_rows(overview.get("by_type", [])), "#ff3621")))
    body.append('<div class="grid2">'
                + _panel("Tables by format", _bar_rows(_count_rows(overview.get("by_format", [])), "#00a972"))
                + _panel("Tables by object type", _bar_rows(_count_rows(overview.get("by_table_type", [])), "#2272b4"))
                + '</div>')
    body.append('<div class="grid2">'
                + _panel("Top catalogs by tables", _bar_rows(_count_rows(overview.get("top_catalogs_tables", [])), "#ff3621"))
                + _panel("Top catalogs by data volume", _bar_rows(overview.get("top_catalogs_bytes", []), "#8a63d2"))
                + '</div>')
    body.append('<div class="grid2">'
                + _panel("Top schemas by tables", _bar_rows(_count_rows(overview.get("top_schemas_tables", [])), "#fb7359"))
                + _panel("Top schemas by data volume", _bar_rows(overview.get("top_schemas_bytes", []), "#0a9396"))
                + '</div>')

    lt = overview.get("largest_tables", [])
    if lt:
        trs = "".join(
            f'<tr><td>{esc(r.get("full_name", ""))}</td><td class="ct">{esc(r.get("table_type", ""))}</td>'
            f'<td class="num">{esc(r.get("rows_h", "") or "—")}</td><td class="num">{esc(r.get("bytes_h", "") or "—")}</td></tr>'
            for r in lt)
        table = ('<div class="tablewrap"><table><thead><tr><th>Table</th><th>Type</th>'
                 '<th class="num">Rows</th><th class="num">Size</th></tr></thead><tbody>'
                 + trs + '</tbody></table></div>')
    else:
        table = '<p class="empty">No table statistics available.</p>'
    body.append(_panel("Largest tables", table))

    meta = (f'Metastore <code>{esc(overview.get("metastore", ""))}</code> &nbsp;·&nbsp; '
            f'Workspace <code>{esc(overview.get("workspace", ""))}</code> &nbsp;·&nbsp; '
            f'Scanned {esc(overview.get("scanned_at", ""))}')
    return (_OVERVIEW_TEMPLATE
            .replace("__TITLE__", esc(title))
            .replace("__META__", meta)
            .replace("__NAV__", nav)
            .replace("__CARDS__", cards)
            .replace("__BODY__", "".join(body)))


_OVERVIEW_TEMPLATE = ("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  * { box-sizing:border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:#f6f8fb; color:#1b2733; }
  header { background:linear-gradient(135deg,#1b3139,#ff3621); color:#fff; padding:24px 32px; }
  header h1 { margin:0 0 6px; font-size:21px; }
  header .meta { font-size:12.5px; opacity:.92; }
  header .meta code { background:rgba(255,255,255,.18); padding:2px 6px; border-radius:4px; }
  main { max-width:1180px; margin:0 auto; padding:22px 32px 60px; }
  .stats { display:grid; grid-template-columns:repeat(auto-fill,minmax(110px,1fr)); gap:12px; margin:18px 0 6px; }
  .stat { background:#fff; border:1px solid #e3e9f1; border-radius:10px; padding:14px; text-align:center; box-shadow:0 1px 2px rgba(20,40,80,.04); }
  .stat-v { font-size:23px; font-weight:700; color:#1b3139; }
  .stat-l { font-size:11px; color:#6b7280; margin-top:4px; text-transform:uppercase; letter-spacing:.03em; }
  .panel { background:#fff; border:1px solid #e3e9f1; border-radius:12px; padding:16px 18px; margin-top:18px; }
  .panel h2 { font-size:14px; margin:0 0 12px; color:#1b3139; }
  .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:18px; }
  @media (max-width:760px){ .grid2 { grid-template-columns:1fr; } }
  .brow { display:flex; align-items:center; gap:10px; margin:5px 0; font-size:12.5px; }
  .blabel { width:200px; min-width:200px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:#33414f; }
  .btrack { flex:1; background:#eef2f7; border-radius:6px; height:14px; overflow:hidden; }
  .bfill { display:block; height:100%; border-radius:6px; }
  .bval { width:74px; text-align:right; color:#5a6b7d; font-variant-numeric:tabular-nums; }
  .tablewrap { overflow-x:auto; border:1px solid #e6ecf4; border-radius:10px; }
  table { border-collapse:collapse; width:100%; font-size:12.5px; background:#fff; }
  thead th { background:#1b3139; color:#fff; text-align:left; padding:7px 11px; font-weight:600; }
  th.num, td.num { text-align:right; font-variant-numeric:tabular-nums; }
  td { padding:6px 11px; border-top:1px solid #eef2f7; word-break:break-word; }
  td.ct { font-family:"SF Mono",Menlo,Consolas,monospace; color:#2272b4; }
  tbody tr:nth-child(even){ background:#f6f9fd; }
  .empty { color:#94a3b8; font-style:italic; }
  footer { text-align:center; color:#94a3b8; font-size:12px; padding:24px; }
__NAV_CSS__
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="meta">__META__</div>
</header>
__NAV__
<main>
  <div class="stats">__CARDS__</div>
  __BODY__
</main>
<footer>Generated by SAT Scanner — Unity Catalog Inventory</footer>
</body>
</html>
""").replace("__NAV_CSS__", _NAV_CSS)
