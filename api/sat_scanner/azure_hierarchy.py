"""SAT Scanner — Azure resource hierarchy discovery + offline report tree.

Builds the full tenant resource tree::

    Management group → Subscription → Resource group → Resource

from two data sources, combined:

  * a portal *Management Groups* CSV export for the MG→subscription tree
    (works even where the ARM Management API returns "Not Authorized", which the
    portal export plainly shows at the tenant root); and
  * live **Azure Resource Graph** for each subscription's resource groups and
    resources.

The tree is a plain nested-dict shape — compatible with the standalone
``build_hierarchy.py`` renderers — so the embedded-JSON HTML views render it
unchanged::

    {"name", "type", "id", "subs", "access", "children": [...]}

with resource leaves additionally carrying ``resource_type``, ``rtype_short``,
``kind``, ``location``, ``rg`` and ``subscription``.

Reuses the existing ARM plumbing in :mod:`azure_infra` (``_arm_get`` /
``_arm_post`` and the Resource Graph ``$skipToken`` pagination pattern) and the
``az`` CLI session wired in :mod:`azure_auth` — no ``azure-mgmt-*`` SDK
dependency.  No file contents leave the machine.
"""
from __future__ import annotations

import csv
import html
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import httpx

from .azure_infra import (
    _arm_get, _arm_post, _ARM_BASE, _API_RESOURCE_GRAPH, _scoped_subscription_ids,
)

# Node type labels (the two original strings are matched verbatim by the JS).
MGMT_GROUP = "Management group"
SUBSCRIPTION = "Subscription"
RESOURCE_GROUP = "Resource group"
RESOURCE = "Resource"

_API_MGMT_GROUPS = "2020-05-01"
_GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


# ─────────────────────────────────────────────────────────────────────────────
# 1. CSV import  (Azure portal "Management Groups" export)
# ─────────────────────────────────────────────────────────────────────────────

def load_rows(csv_path: str | Path) -> list[dict]:
    """Read a portal Management-Groups CSV export into a list of row dicts.

    The export has a title row, a blank row, then a header row beginning with
    ``id`` and the columns
    ``id,displayName,itemType,path,accessLevel,childSubscriptionCount,totalSubscriptionCount``.
    """
    path = Path(csv_path).expanduser()
    with path.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    header_idx = next(i for i, r in enumerate(rows) if r and r[0] == "id")
    header = rows[header_idx]
    data = [r for r in rows[header_idx + 1:] if any(c.strip() for c in r)]
    return [dict(zip(header, r)) for r in data]


def build_tree_from_csv(records: list[dict]) -> dict | None:
    """Build the MG→subscription tree from portal export rows (keyed by displayName)."""
    nodes: dict[str, dict] = {}
    for rec in records:
        name = rec["displayName"].strip()
        nodes[name] = {
            "name": name,
            "type": rec["itemType"].strip(),
            "subs": rec.get("totalSubscriptionCount", "").strip(),
            "access": rec.get("accessLevel", "").strip(),
            "id": rec.get("id", "").strip(),
            "children": [],
        }
    root = None
    for rec in records:
        name = rec["displayName"].strip()
        path = [p.strip() for p in rec["path"].split(",") if p.strip()]
        if not path:
            root = nodes[name]
            continue
        parent = path[-1]
        if parent in nodes:
            nodes[parent]["children"].append(nodes[name])
        else:  # orphan: attach to root defensively
            (root or nodes[name])["children"].append(nodes[name])
    if root:
        _sort_kids(root)
    return root


def _sort_kids(n: dict) -> None:
    """Stable sort: containers before leaves, alpha within (recursive)."""
    order = {MGMT_GROUP: 0, SUBSCRIPTION: 1, RESOURCE_GROUP: 2, RESOURCE: 3}
    n["children"].sort(key=lambda c: (order.get(c["type"], 9), c["name"].lower()))
    for c in n["children"]:
        _sort_kids(c)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Live discovery via ARM + Resource Graph
# ─────────────────────────────────────────────────────────────────────────────

async def discover_management_groups(client: httpx.AsyncClient, token: str, tenant_id: str,
                                     root_id: str, errors: list[str]) -> dict | None:
    """Build the MG→subscription tree live from the ARM Management Groups API.

    Returns the root node dict, or ``None`` (appending a reason to ``errors``) when
    the API is unavailable or returns *Not Authorized* — the caller then falls back
    to the CSV tree.
    """
    group_id = root_id or tenant_id
    if not group_id:
        errors.append("no tenant/root management group id available")
        return None
    url = f"{_ARM_BASE}/providers/Microsoft.Management/managementGroups/{group_id}"
    data, status, err = await _arm_get(
        client, url, token,
        {"api-version": _API_MGMT_GROUPS, "$expand": "children", "$recurse": "true"})
    if status != 200:
        errors.append(f"management groups {status}: {_short(err)} "
                      "(no Management Group Reader access — using CSV if provided)")
        return None
    props = data.get("properties", {}) or {}
    root = {
        "name": props.get("displayName") or data.get("name", group_id),
        "type": MGMT_GROUP,
        "id": data.get("name", group_id),
        "subs": "",
        "access": "",
        "children": [_convert_mg_node(c) for c in (props.get("children") or [])],
    }
    _sort_kids(root)
    return root


def _convert_mg_node(entry: dict) -> dict:
    """Convert one ARM management-group child entry (recursively) to a tree node."""
    etype = (entry.get("type") or "").lower()
    disp = entry.get("displayName") or entry.get("name") or ""
    if "subscriptions" in etype:
        return {"name": disp, "type": SUBSCRIPTION, "id": entry.get("name", ""),
                "subs": "", "access": "", "children": []}
    node = {"name": disp, "type": MGMT_GROUP, "id": entry.get("name", ""),
            "subs": "", "access": "", "children": []}
    for c in (entry.get("children") or []):
        node["children"].append(_convert_mg_node(c))
    return node


async def _graph_query(client: httpx.AsyncClient, token: str, sub_ids: list[str],
                       query: str, errors: list[str]) -> list[dict]:
    """Run an Azure Resource Graph query across ``sub_ids`` (paginated via $skipToken)."""
    url = f"{_ARM_BASE}/providers/Microsoft.ResourceGraph/resources?api-version={_API_RESOURCE_GRAPH}"
    out: list[dict] = []
    skip_token = None
    while True:
        body = {"subscriptions": sub_ids, "query": query,
                "options": {"resultFormat": "objectArray", "top": 1000}}
        if skip_token:
            body["options"]["$skipToken"] = skip_token
        data, status, err = await _arm_post(client, url, token, body)
        if status != 200:
            errors.append(f"Resource Graph {status}: {_short(err)}")
            break
        out.extend(data.get("data", []) or [])
        skip_token = data.get("$skipToken") or data.get("skipToken")
        if not skip_token:
            break
    return out


async def discover_subscriptions(client, token, sub_ids, errors) -> list[dict]:
    """List subscriptions (id + display name) visible to the session via Resource Graph."""
    q = ("ResourceContainers | where type =~ 'microsoft.resources/subscriptions' "
         "| project subscriptionId, name, "
         "displayName=tostring(properties.displayName), state=tostring(properties.state)")
    return await _graph_query(client, token, sub_ids, q, errors)


async def discover_resource_groups(client, token, sub_ids, errors) -> list[dict]:
    """List every resource group across ``sub_ids`` via Resource Graph."""
    q = ("ResourceContainers "
         "| where type =~ 'microsoft.resources/subscriptions/resourcegroups' "
         "| project id, name, subscriptionId, location")
    return await _graph_query(client, token, sub_ids, q, errors)


async def discover_resources(client, token, sub_ids, errors) -> list[dict]:
    """List every (top-level) Azure resource across ``sub_ids`` via Resource Graph."""
    q = ("Resources | project id, name, type, kind, location, "
         "resourceGroup, subscriptionId, sku=tostring(sku.name), tags")
    return await _graph_query(client, token, sub_ids, q, errors)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Tree assembly  (graft RGs + resources onto the MG/subscription tree)
# ─────────────────────────────────────────────────────────────────────────────

def _short(val: Any, limit: int = 200) -> str:
    s = val if isinstance(val, str) else json.dumps(val, default=str)
    return s if len(s) <= limit else s[:limit] + "…"


def _rtype_short(full_type: str) -> str:
    """'microsoft.network/virtualnetworks' → 'virtualnetworks'."""
    return full_type.rsplit("/", 1)[-1] if full_type else ""


def _resource_node(r: dict) -> dict:
    ftype = r.get("type", "") or ""
    return {
        "name": r.get("name", "") or "(unnamed)",
        "type": RESOURCE,
        "id": r.get("id", ""),
        "subs": "",
        "access": "",
        "resource_type": ftype,
        "rtype_short": _rtype_short(ftype),
        "kind": r.get("kind", "") or "",
        "location": r.get("location", "") or "",
        "rg": r.get("resourceGroup", "") or "",
        "subscription": r.get("subscriptionId", "") or "",
        "sku": r.get("sku", "") or "",
        "tags": r.get("tags") or {},
        "children": [],
    }


def _populate_subscription(sub_node: dict, sub_id: str,
                           rgs_by_sub: dict[str, list[dict]],
                           res_by_sub_rg: dict[str, dict[str, list[dict]]]) -> None:
    """Attach a subscription's resource groups (and their resources) under ``sub_node``."""
    res_groups = res_by_sub_rg.get(sub_id, {})
    seen: set[str] = set()
    for rg in rgs_by_sub.get(sub_id, []):
        rg_name = rg.get("name", "") or "(unnamed)"
        rg_node = {
            "name": rg_name, "type": RESOURCE_GROUP, "id": rg.get("id", ""),
            "subs": "", "access": "", "location": rg.get("location", "") or "",
            "subscription": sub_id, "children": [],
        }
        for r in res_groups.get(rg_name.lower(), []):
            rg_node["children"].append(_resource_node(r))
        sub_node["children"].append(rg_node)
        seen.add(rg_name.lower())
    # resources whose RG had no container row (deleted RG / permissions): bucket them
    for rg_lower, rows in res_groups.items():
        if rg_lower in seen:
            continue
        rg_node = {
            "name": rows[0].get("resourceGroup") or "(ungrouped)", "type": RESOURCE_GROUP,
            "id": "", "subs": "", "access": "", "location": "", "subscription": sub_id,
            "children": [_resource_node(r) for r in rows],
        }
        sub_node["children"].append(rg_node)
    _sort_kids(sub_node)


def _make_sub_node(s: dict, rgs_by_sub, res_by_sub_rg) -> dict:
    """Build a fully-populated subscription node from a Resource Graph subscription row."""
    sid = s.get("subscriptionId", "")
    node = {"name": s.get("displayName") or s.get("name") or sid,
            "type": SUBSCRIPTION, "id": sid, "subscription_id": sid,
            "subs": "", "access": "", "children": []}
    _populate_subscription(node, sid, rgs_by_sub, res_by_sub_rg)
    return node


def build_full_tree(mg_tree: dict | None, subs: list[dict], rgs: list[dict],
                    resources: list[dict]) -> dict:
    """Combine the MG/subscription tree with live RGs + resources into one root node."""
    rgs_by_sub: dict[str, list[dict]] = defaultdict(list)
    for rg in rgs:
        rgs_by_sub[rg.get("subscriptionId", "")].append(rg)
    res_by_sub_rg: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in resources:
        res_by_sub_rg[r.get("subscriptionId", "")][(r.get("resourceGroup", "") or "").lower()].append(r)

    # name → subscriptionId map (fallback join when the CSV id column is blank)
    name_to_subid: dict[str, str] = {}
    for s in subs:
        sid = s.get("subscriptionId", "")
        for key in (s.get("displayName"), s.get("name")):
            if key:
                name_to_subid[key.strip().lower()] = sid

    matched: set[str] = set()

    def attach(node: dict) -> None:
        if node.get("type") == SUBSCRIPTION:
            sid = _subid_for(node, name_to_subid)
            node["subscription_id"] = sid          # keep the resolved GUID on the node
            if sid and not node.get("id"):
                node["id"] = sid
            if sid:
                matched.add(sid)
                _populate_subscription(node, sid, rgs_by_sub, res_by_sub_rg)
        for c in node.get("children", []):
            attach(c)

    if mg_tree:
        attach(mg_tree)
        root = mg_tree
    else:  # no MG tree at all — synthesise a tenant root from the live subscriptions
        root = {"name": "Azure (tenant)", "type": MGMT_GROUP, "id": "",
                "subs": str(len(subs)), "access": "", "children": []}
        for s in subs:
            matched.add(s.get("subscriptionId", ""))
            root["children"].append(_make_sub_node(s, rgs_by_sub, res_by_sub_rg))

    # any subscriptions with resources but missing from the MG tree → bucket them
    leftover = [s for s in subs
                if s.get("subscriptionId") and s["subscriptionId"] not in matched]
    if leftover:
        root["children"].append({
            "name": "Subscriptions outside the management-group tree",
            "type": MGMT_GROUP, "id": "", "subs": str(len(leftover)), "access": "",
            "children": [_make_sub_node(s, rgs_by_sub, res_by_sub_rg) for s in leftover],
        })

    _sort_kids(root)
    return root


def _subid_for(node: dict, name_to_subid: dict[str, str]) -> str:
    """Resolve a subscription node's GUID: from its id, else by display name."""
    m = _GUID_RE.search(node.get("id", "") or "")
    if m:
        return m.group(0).lower()
    return name_to_subid.get(node.get("name", "").strip().lower(), "")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Per-node colours, counts, traversal  (ported from build_hierarchy.py)
# ─────────────────────────────────────────────────────────────────────────────

def _set_colour(node: dict, hue: int) -> None:
    """Stamp one node with colour fields for ``hue``, styled by type (lighter as you go deeper)."""
    hue %= 360
    node["_hue"] = hue
    t = node["type"]
    if t == MGMT_GROUP:
        node["_color"] = f"hsl({hue} 62% 46%)"
        node["_stroke"] = f"hsl({hue} 52% 30%)"
        node["_dark"] = True
    elif t == SUBSCRIPTION:
        node["_color"] = f"hsl({hue} 64% 52%)"
        node["_stroke"] = f"hsl({hue} 54% 34%)"
        node["_dark"] = True
    elif t == RESOURCE_GROUP:
        node["_color"] = f"hsl({hue} 58% 60%)"
        node["_stroke"] = f"hsl({hue} 48% 40%)"
        node["_dark"] = False
    else:  # RESOURCE
        node["_color"] = f"hsl({hue} 62% 72%)"
        node["_stroke"] = f"hsl({hue} 46% 50%)"
        node["_dark"] = False


def _recolour(node: dict, base: int) -> None:
    """Give each child a distinct hue (evenly spread around the wheel, rotated by ``base``), recursively."""
    kids = node.get("children", [])
    n = len(kids) or 1
    for i, c in enumerate(kids):
        hue = round(base + i * 360 / n)
        _set_colour(c, hue)
        _recolour(c, hue)


def assign_colours(root: dict) -> None:
    """Colour the tree so every sibling set has distinct, evenly-spread hues.

    Descends past any single-child spine to the first branching node, then hands each of
    its children a distinct hue and recurses — so a subscription's resource groups (and a
    resource group's resources) are each visually distinct, not shades of a single hue.
    """
    branch = root
    while len(branch.get("children", [])) == 1:
        branch = branch["children"][0]
    _recolour(branch, 0)


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
# 5. HTML renderers  (self-contained, offline — ported & extended for 4 node types)
# ─────────────────────────────────────────────────────────────────────────────

def _stats_line(root: dict) -> str:
    c = count_by_type(root)
    parts = [
        (c.get(MGMT_GROUP, 0), "management groups"),
        (c.get(SUBSCRIPTION, 0), "subscriptions"),
        (c.get(RESOURCE_GROUP, 0), "resource groups"),
        (c.get(RESOURCE, 0), "resources"),
    ]
    return " &nbsp;·&nbsp; ".join(f"<b>{n}</b> {label}" for n, label in parts)


def render_tree_html(root: dict, title: str = "Azure Resource Hierarchy — Tree") -> str:
    return (_TREE_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _stats_line(root))
            .replace("__DATA__", json.dumps(root)))


def render_star_html(root: dict, title: str = "Azure Resource Hierarchy — Sunburst") -> str:
    return (_STAR_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _stats_line(root))
            .replace("__DATA__", json.dumps(root)))


def render_hub_html(root: dict, title: str = "Azure Resource Hierarchy — Hub & Spoke") -> str:
    return (_HUB_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__STATS__", _stats_line(root))
            .replace("__DATA__", json.dumps(root)))


_LEGEND = (
    '<div class="legend">'
    '<span><i class="dot" style="background:#0078d4"></i> Mgmt group</span>'
    '<span><i class="dot" style="background:#5b8def"></i> Subscription</span>'
    '<span><i class="dot" style="background:#8ca0b8"></i> Resource group</span>'
    '<span><i class="dot" style="background:#cfe0f6"></i> Resource</span>'
    '</div>'
)

_TREE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --mg:#0078d4; --mg-d:#004578; --sub:#5b8def; --bg:#f6f8fb; --line:#c7d4e6; }
  * { box-sizing: border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:var(--bg); color:#1b2733; }
  header { background:#fff; padding:14px 24px; border-bottom:1px solid #e3e9f1; position:sticky; top:0; z-index:6;
           display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  .stats { font-size:13px; color:#5a6b7d; }
  .stats b { color:#1b2733; }
  .legend { display:flex; gap:16px; font-size:12px; color:#5a6b7d; align-items:center; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .dot { width:12px; height:12px; border-radius:3px; }
  .controls { background:#fff; border-bottom:1px solid #e3e9f1; padding:10px 24px; position:sticky; top:56px; z-index:5;
              display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .controls input[type=search], .controls select { font:inherit; font-size:13px; padding:6px 10px;
              border:1px solid #cdd8e6; border-radius:6px; background:#fff; }
  .controls input[type=search] { width:260px; }
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
  .node.sel { border-color:#0078d4; box-shadow:0 0 0 2px rgba(0,120,212,.18); }
  .node .tw { cursor:pointer; width:16px; height:16px; display:inline-flex; align-items:center; justify-content:center;
              color:#8095ab; font-size:11px; user-select:none; }
  .node .tw.empty { visibility:hidden; }
  .badge { width:20px; height:20px; border-radius:5px; display:inline-flex; align-items:center; justify-content:center;
           font-size:11px; font-weight:700; color:#fff; }
  .mg .badge { background:var(--mg); } .sub .badge { background:var(--sub); }
  .rg .badge { background:#8ca0b8; } .res .badge { background:#9fb6d6; }
  .name { font-size:13.5px; }
  .mg > .name { font-weight:600; }
  .name mark { background:#ffe08a; padding:0 1px; border-radius:2px; }
  .count { font-size:11px; color:#fff; background:#7c8da3; border-radius:10px; padding:1px 7px; }
  .mg .count { background:#0a66c2; }
  .meta { font-size:10.5px; color:#8a97a6; }
  li.collapsed > ul { display:none; }
  li.hide { display:none; }
  .panel { position:fixed; top:0; right:0; width:380px; max-width:92vw; height:100%; background:#fff; border-left:1px solid #e3e9f1;
           box-shadow:-4px 0 18px rgba(20,40,80,.08); transform:translateX(100%); transition:transform .18s; z-index:20;
           display:flex; flex-direction:column; }
  .panel.open { transform:none; }
  .panel-hd { padding:14px 18px; border-bottom:1px solid #eef2f7; display:flex; align-items:center; gap:10px; }
  .panel-hd b { font-size:14px; word-break:break-word; }
  .panel-hd .x { margin-left:auto; cursor:pointer; color:#8a97a6; font-size:16px; }
  .panel-bd { padding:14px 18px; overflow:auto; font-size:13px; }
  .panel-bd table { border-collapse:collapse; width:100%; }
  .panel-bd td { padding:5px 6px; border-bottom:1px solid #f0f3f8; vertical-align:top; word-break:break-word; }
  .panel-bd td.k { color:#5a6b7d; width:118px; }
  .panel-bd pre { margin:0; white-space:pre-wrap; font:12px/1.5 "SF Mono",Menlo,Consolas,monospace; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="stats">__STATS__</div>
  __LEGEND__
</header>
<div class="controls">
  <input type="search" id="q" placeholder="Search name, type, location, id…" oninput="onSearch(this.value)">
  <span class="chips" id="chips"></span>
  <select id="rtype" onchange="state.rtype=this.value; apply()"><option value="">All resource types</option></select>
  <select id="loc" onchange="state.loc=this.value; apply()"><option value="">All locations</option></select>
  <button onclick="setAll(false)">Expand</button>
  <button onclick="setAll(true)">Collapse</button>
  <button onclick="resetFilters()">Reset</button>
  <span class="status" id="status"></span>
</div>
<div id="tree"></div>
<div class="panel" id="panel">
  <div class="panel-hd"><b id="p-title"></b><span class="x" onclick="hidePanel()">✕</span></div>
  <div class="panel-bd" id="p-body"></div>
</div>
<script>
const DATA = __DATA__;
const TYPES = ['Management group','Subscription','Resource group','Resource'];
const TYPE_CLS = {'Management group':'mg','Subscription':'sub','Resource group':'rg','Resource':'res'};
const TYPE_BADGE = {'Management group':'M','Subscription':'S','Resource group':'G','Resource':'R'};
const TYPE_DOT = {'Management group':'#0078d4','Subscription':'#5b8def','Resource group':'#8ca0b8','Resource':'#9fb6d6'};
const state = { q:'', types:new Set(TYPES), rtype:'', loc:'' };
let selected = null;

function el(tag, cls, txt){ const e=document.createElement(tag); if(cls)e.className=cls; if(txt!=null)e.textContent=txt; return e; }
function esc(s){ return (s+'').replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }

function render(node){
  const cls = TYPE_CLS[node.type] || 'res';
  const li = el('li', cls); node.__li = li;
  const box = el('div','node'); node.__box = box;
  const tw = el('span','tw');
  const hasKids = node.children && node.children.length;
  if(hasKids){ tw.textContent='▼'; tw.onclick=(e)=>{ e.stopPropagation(); li.classList.toggle('collapsed'); }; }
  else { tw.classList.add('empty'); }
  box.appendChild(tw);
  const badge = el('span','badge', TYPE_BADGE[node.type] || 'R');
  if(node._color) badge.style.background = node._color;
  box.appendChild(badge);
  const nameEl = el('span','name', node.name); node.__name = nameEl;
  box.appendChild(nameEl);
  const n = parseInt(node.subs,10);
  if(node.type==='Management group' && n>0) box.appendChild(el('span','count', n+' sub'+(n>1?'s':'')));
  if(node.rtype_short) box.appendChild(el('span','meta', node.rtype_short));
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
(function(){ const u=el('ul'); u.appendChild(render(DATA)); document.getElementById('tree').appendChild(u); })();

(function init(){
  const rtypes=new Set(), locs=new Set();
  (function scan(n){ if(n.resource_type) rtypes.add(n.resource_type); if(n.location) locs.add(n.location); (n.children||[]).forEach(scan); })(DATA);
  const rt=document.getElementById('rtype');
  [...rtypes].sort().forEach(t=>{ const o=document.createElement('option'); o.value=t; o.textContent=t; rt.appendChild(o); });
  const lc=document.getElementById('loc');
  [...locs].sort().forEach(t=>{ const o=document.createElement('option'); o.value=t; o.textContent=t; lc.appendChild(o); });
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
  if(state.q){
    const hay=(n.name+' '+(n.type||'')+' '+(n.resource_type||'')+' '+(n.location||'')+' '+(n.id||'')).toLowerCase();
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
  const filtering = !!(state.q || state.rtype || state.loc || state.types.size<TYPES.length);
  const shown={'Management group':0,'Subscription':0,'Resource group':0,'Resource':0};
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
  state.q=''; state.rtype=''; state.loc=''; state.types=new Set(TYPES);
  document.getElementById('q').value=''; document.getElementById('rtype').value=''; document.getElementById('loc').value='';
  document.querySelectorAll('.chip').forEach(c=>c.classList.remove('off'));
  apply();
}
function select(n){
  if(selected) selected.__box.classList.remove('sel');
  selected=n; n.__box.classList.add('sel');
  document.getElementById('p-title').textContent=n.name;
  const rows=[['Type',n.type]];
  if(n.subscription_id) rows.push(['Subscription Id',n.subscription_id]);
  if(n.subscription) rows.push(['Subscription',n.subscription]);
  if(n.rg) rows.push(['Resource Group',n.rg]);
  if(n.resource_type) rows.push(['Resource Type',n.resource_type]);
  if(n.kind) rows.push(['Kind',n.kind]);
  if(n.location) rows.push(['Location',n.location]);
  if(n.sku) rows.push(['SKU',n.sku]);
  if(n.access && n.access!=='Reader') rows.push(['Access',n.access]);
  if(n.subs && n.type==='Management group') rows.push(['Subscriptions',n.subs]);
  if(n.id) rows.push(['Id',n.id]);
  let h=rows.map(r=>'<tr><td class="k">'+esc(r[0])+'</td><td>'+esc(r[1])+'</td></tr>').join('');
  if(n.tags && Object.keys(n.tags).length) h+='<tr><td class="k">Tags</td><td><pre>'+esc(JSON.stringify(n.tags,null,2))+'</pre></td></tr>';
  document.getElementById('p-body').innerHTML='<table>'+h+'</table>';
  document.getElementById('panel').classList.add('open');
}
function hidePanel(){ document.getElementById('panel').classList.remove('open'); if(selected){ selected.__box.classList.remove('sel'); selected=null; } }
document.addEventListener('keydown', e=>{ if(e.key==='Escape') hidePanel(); });
</script>
</body>
</html>
""".replace("__LEGEND__", _LEGEND)


_STAR_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --mg:#0078d4; --bg:#f6f8fb; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:var(--bg); color:#1b2733; }
  header { background:#fff; padding:16px 24px; border-bottom:1px solid #e3e9f1; position:sticky; top:0; z-index:6;
           display:flex; align-items:center; gap:18px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  .stats { font-size:13px; color:#5a6b7d; }
  .stats b { color:#1b2733; }
  .legend { display:flex; gap:16px; font-size:12px; color:#5a6b7d; align-items:center; margin-left:auto; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .dot { width:12px; height:12px; border-radius:3px; }
  .controls { background:#fff; border-bottom:1px solid #e3e9f1; padding:10px 24px; position:sticky; top:58px; z-index:5;
              display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .controls input[type=search] { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; width:260px; }
  .hint { font-size:12px; color:#8a97a6; }
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
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="stats">__STATS__</div>
  __LEGEND__
</header>
<div class="controls">
  <input type="search" id="q" placeholder="Search & highlight…" oninput="onSearch(this.value)">
  <span class="hint">Click a ring to zoom in · click the centre to zoom out</span>
  <span class="status" id="status"></span>
</div>
<div id="wrap"><svg id="star"></svg></div>
<script>
const DATA = __DATA__;
const SVGNS = "http://www.w3.org/2000/svg";
const R0 = 58, RING = 64, TWO = Math.PI * 2;
(function setParents(n, par){ n.__parent = par || null; (n.children || []).forEach(c => setParents(c, n)); })(DATA, null);
let focus = DATA, q = '', allPaths = [], nameT, subT;
const svg = document.getElementById('star');
function leaves(n){ if(!n.children || !n.children.length){ return (n._lv = 1); } let s = 0; n.children.forEach(c => s += leaves(c)); return (n._lv = s); }
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
function fill(n){ if(n._color) return n._color; if(n.type !== 'Management group') return '#cfe0f6';
  return `hsl(206 72% ${Math.min(70, 30 + n._depth * 8)}%)`; }
function shortName(n){ const p = n.__parent; return (p && n.name.indexOf(p.name + '-') === 0) ? n.name.slice(p.name.length + 1) : n.name; }
function mkText(x, y, cls){ const t = document.createElementNS(SVGNS, 'text'); t.setAttribute('x', x); t.setAttribute('y', y);
  t.setAttribute('text-anchor', 'middle'); t.setAttribute('class', cls); svg.appendChild(t); return t; }
function setCenter(n){
  nameT.textContent = n.name.length > 24 ? n.name.slice(0, 23) + '…' : n.name;
  const lead = n.__parent ? '↩ ' : '';
  subT.textContent = lead + (n.type === 'Management group' ? ((parseInt(n.subs, 10) || 0) + ' subscriptions') : n.type);
}
function render(){
  leaves(focus); const MAXD = depthOf(focus) || 1; assign(focus, 0, TWO, 0);
  const OUTER = R0 + MAXD * RING, MARGIN = 150, D = 2 * (OUTER + MARGIN), C = D / 2;
  svg.setAttribute('viewBox', `0 0 ${D} ${D}`); svg.setAttribute('width', D); svg.setAttribute('height', D);
  while(svg.firstChild) svg.removeChild(svg.firstChild);
  allPaths = [];
  (function draw(n){
    if(n._depth > 0){
      const ri = R0 + (n._depth - 1) * RING, ro = R0 + n._depth * RING;
      const dark = n._color ? n._dark : (n.type === 'Management group' && n._depth <= 3);
      const path = document.createElementNS(SVGNS, 'path');
      path.setAttribute('d', ringPath(ri, ro, n._a0, n._a1, C));
      path.setAttribute('class', 'seg'); path.setAttribute('fill', fill(n));
      path.__node = n; allPaths.push(path);
      path.addEventListener('mouseenter', () => setCenter(n));
      path.addEventListener('mouseleave', () => setCenter(focus));
      path.addEventListener('click', () => { if(n.children && n.children.length){ focus = n; render(); } });
      const title = document.createElementNS(SVGNS, 'title');
      title.textContent = n.name + (n.rtype_short ? '  (' + n.rtype_short + ')' : '');
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
""".replace("__LEGEND__", _LEGEND)


_HUB_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --mg:#0078d4; --mg-d:#004578; --hub:#13315b; --bg:#f6f8fb; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin:0; background:var(--bg); color:#1b2733; }
  header { background:#fff; padding:16px 24px; border-bottom:1px solid #e3e9f1; position:sticky; top:0; z-index:6;
           display:flex; align-items:center; gap:18px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  .stats { font-size:13px; color:#5a6b7d; }
  .stats b { color:#1b2733; }
  .legend { display:flex; gap:16px; font-size:12px; color:#5a6b7d; align-items:center; margin-left:auto; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .dot { width:12px; height:12px; border-radius:3px; }
  .controls { background:#fff; border-bottom:1px solid #e3e9f1; padding:10px 24px; position:sticky; top:58px; z-index:5;
              display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .controls input[type=search] { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; width:260px; }
  .controls button { font:inherit; font-size:13px; padding:6px 12px; border:1px solid #cdd8e6; background:#fff; border-radius:6px; cursor:pointer; }
  .controls button:hover { background:#eef3fb; }
  .controls button:disabled { opacity:.45; cursor:default; }
  .controls select { font:inherit; font-size:13px; padding:6px 10px; border:1px solid #cdd8e6; border-radius:6px; background:#fff; max-width:240px; }
  .crumbs { font-size:12px; color:#5a6b7d; display:flex; gap:6px; align-items:center; flex-wrap:wrap; }
  .crumbs a { color:#0a66c2; cursor:pointer; text-decoration:none; }
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
  .node.leaf { cursor:default; }
  .node:hover rect { filter:brightness(1.08); }
  .node text { pointer-events:none; }
  .node.dim { opacity:.16; }
  .node.hit rect { stroke:#13315b; stroke-width:2; }
  .node.mg rect  { fill:var(--mg); stroke:var(--mg-d); }
  .node.mg text  { fill:#fff; font-size:11px; font-weight:600; }
  .node.sub rect { fill:#e8f0fb; stroke:#b9cdec; }
  .node.sub text { fill:#13315b; font-size:10.5px; }
  .hub rect { fill:var(--hub); stroke:#0b2444; cursor:pointer; }
  .hub .h1 { fill:#fff; font-size:13px; font-weight:700; }
  .hub .h2 { fill:#9fb6d6; font-size:9px; }
  .hub .h3 { fill:#cfe0f6; font-size:10px; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="stats">__STATS__</div>
  __LEGEND__
</header>
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
(function setParents(n, p){ n.__parent = p || null; (n.children || []).forEach(c => setParents(c, n)); })(DATA, null);
let focus = DATA, q = '', pills = [];
const NODES = [];
(function idx(n){ n.__idx = NODES.length; NODES.push(n); (n.children || []).forEach(idx); })(DATA);
(function buildFocusSelect(){
  const sel = document.getElementById('focus');
  const def = document.createElement('option'); def.value = ''; def.textContent = 'Jump to…'; sel.appendChild(def);
  [['Management group','Management groups'],['Subscription','Subscriptions'],['Resource group','Resource groups']].forEach(g => {
    const og = document.createElement('optgroup'); og.label = g[1];
    NODES.filter(n => n.type === g[0] && n.children && n.children.length)
         .sort((a, b) => a.name.localeCompare(b.name))
         .forEach(n => { const o = document.createElement('option'); o.value = n.__idx; o.textContent = n.name + ' (' + n.children.length + ')'; og.appendChild(o); });
    if(og.children.length) sel.appendChild(og);
  });
})();
function jump(v){ if(v === '') return; focus = NODES[parseInt(v, 10)]; render(); }

function foldFrom(n){ let h = n, crumb = [h]; while(h.children && h.children.length === 1){ h = h.children[0]; crumb.push(h); } return { hub: h, crumb }; }
function short(name, parent){ return (parent && name.indexOf(parent + '-') === 0) ? name.slice(parent.length + 1) : name; }
function line(x1, y1, x2, y2, cls){
  const l = document.createElementNS(SVGNS, 'line');
  l.setAttribute('x1', x1); l.setAttribute('y1', y1); l.setAttribute('x2', x2); l.setAttribute('y2', y2);
  l.setAttribute('class', cls); return l;
}
function pill(x, y, text, cls, node){
  const drillable = node.children && node.children.length;
  const g = document.createElementNS(SVGNS, 'g'); g.setAttribute('class', 'node ' + cls + (drillable ? '' : ' leaf'));
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
  tip.textContent = node.name + (node.rtype_short ? '  (' + node.rtype_short + ')' : '') + (drillable ? '  — click to drill in' : '');
  g.appendChild(tip);
  if(drillable) g.addEventListener('click', () => { focus = node; render(); });
  g.__node = node; pills.push(g);
  return g;
}
function childLabel(n){
  const k = n.children || []; if(!k.length) return '';
  const t = k[0].type; const noun = t === 'Management group' ? 'group' : t === 'Subscription' ? 'subscription' : t === 'Resource group' ? 'resource group' : 'resource';
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
      nodes.appendChild(pill(sx, sy, short(s.name, w.name) + (cc ? ' (' + cc + ')' : ''), 'sub', s));
    });
    const wc = (w.children || []).length;
    nodes.appendChild(pill(wx, wy, short(w.name, hub.name) + (wc ? ' (' + wc + ')' : ''), 'mg', w));
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
""".replace("__LEGEND__", _LEGEND)
