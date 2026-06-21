"""Unit tests for the fleet-combined UC diagrams (root-level, grouped by metastore).

Pure/offline: synthetic ``UCInventoryResult`` trees, no network. Run:
``pytest sat_scanner/tests/test_fleet_hierarchy.py`` from the ``assessment/`` dir.
"""
from __future__ import annotations

import re

from sat_scanner import uc_hierarchy as uh
from sat_scanner.inventory_models import (
    UCCatalog, UCColumn, UCInventoryResult, UCSchema, UCTable,
)


def _catalog(name: str, nbytes: int | None = None) -> UCCatalog:
    props = {"spark.sql.statistics.totalSize": str(nbytes)} if nbytes else {}
    table = UCTable(full_name=f"{name}.s.t", catalog=name, schema="s", name="t",
                    table_type="MANAGED", properties=props,
                    columns=[UCColumn(name="a"), UCColumn(name="b")], grants=[])
    schema = UCSchema(full_name=f"{name}.s", catalog=name, name="s", tables=[table])
    return UCCatalog(name=name, catalog_type="MANAGED_CATALOG", schemas=[schema])


def _inv(ws, mid, names, *, nbytes=1000, external_locations=(), storage_accounts=()):
    r = UCInventoryResult(f"https://{ws}", ws, "2026-06-19T00:00:00")
    r.metastore = {"current_assignment": {"metastore_id": mid, "metastore_name": f"ms::{mid}"}}
    r.catalogs = [_catalog(n, nbytes) for n in names]
    r.external_locations = [dict(e) for e in external_locations]
    r.azure = {"available": bool(storage_accounts), "storage_accounts": list(storage_accounts)}
    return r


def _cat_names(node):
    return sorted(c["name"] for c in node["children"])


def _count_cat(node, name):
    n = 1 if (node.get("type") == "Catalog" and node.get("name") == name) else 0
    return n + sum(_count_cat(k, name) for k in node.get("children", []))


# wsA/wsB share ms-1 ({c1,c2}); wsC on ms-2 ({c1,d1}); c1 collides across metastores.
A = _inv("wsA", "ms-1", ["c1", "c2"], nbytes=1000,
         external_locations=[{"name": "el1", "azure": {"storage_account": "sa1"}}],
         storage_accounts=[{"name": "sa1", "resource_group": "rg", "location": "uksouth"}])
B = _inv("wsB", "ms-1", ["c1", "c2"], nbytes=1000)
C = _inv("wsC", "ms-2", ["c1", "d1"], nbytes=2000,
         external_locations=[{"name": "el2", "azure": {"storage_account": "sa2"}}],
         storage_accounts=[{"name": "sa2", "location": "useast"}])


def test_fleet_tree_multi_metastore():
    t = uh.build_fleet_tree([A, B, C])
    assert t["type"] == "Fleet"
    assert t["_color"] == "#0d2a30"                      # palette applied, not grey fallback
    assert len(t["children"]) == 2                       # ms-1, ms-2
    for ms in t["children"]:
        assert ms["type"] == "Metastore" and ms["metastore_id"] and ms["workspaces"]
    ms1 = next(m for m in t["children"] if m["metastore_id"] == "ms-1")
    ms2 = next(m for m in t["children"] if m["metastore_id"] == "ms-2")
    assert _cat_names(ms1) == ["c1", "c2"]               # wsA+wsB deduped, not 4 catalogs
    assert sorted(ms1["workspaces"]) == ["wsA", "wsB"]
    assert _cat_names(ms2) == ["c1", "d1"]
    assert _count_cat(t, "c1") == 2                      # distinct c1 per metastore


def test_fleet_tree_single_metastore_no_wrapper():
    t = uh.build_fleet_tree([A, B])
    assert t["type"] == "Metastore"                      # no Fleet wrapper
    assert _cat_names(t) == ["c1", "c2"]


def test_fleet_workspace_visibility():
    # wsA sees {c1,c2}; wsB sees {c1,c2,c3} (c3 ISOLATED to wsB); both on ms-1.
    a = _inv("wsA", "ms-1", ["c1", "c2"])
    b = _inv("wsB", "ms-1", ["c1", "c2", "c3"])
    t = uh.build_fleet_tree([a, b])
    assert t["workspaces"] == ["wsA", "wsB"]            # dropdown source (plural ⇒ fleet)
    cats = {n["name"]: n["w"] for n in _walk(t) if n.get("type") == "Catalog"}
    assert cats["c1"] == [0, 1] and cats["c2"] == [0, 1]
    assert cats["c3"] == [1]                            # visible to wsB only
    c3 = next(n for n in _walk(t) if n.get("type") == "Catalog" and n["name"] == "c3")
    assert c3["children"][0]["children"][0]["w"] == [1]  # table inherits catalog visibility
    # a per-workspace tree has no `workspaces` plural ⇒ no dropdown
    assert "workspaces" not in uh.build_uc_tree(a)


def test_fleet_topology_unions_and_dedups():
    # duplicate-named external location across two inventories; first has empty azure.
    a = _inv("wsA", "ms-1", ["c1"], external_locations=[{"name": "shared", "azure": {}}],
             storage_accounts=[{"name": "sa1"}])
    c = _inv("wsC", "ms-2", ["d1"], external_locations=[{"name": "shared", "azure": {"storage_account": "sa2"}}],
             storage_accounts=[{"name": "sa2"}])
    topo = uh.build_fleet_topology([A, C])
    assert topo["name"] == "Fleet Infrastructure"
    counts = uh.count_by_type(topo)
    assert counts.get("StorageAccount", 0) >= 2          # sa1, sa2
    assert counts.get("ExternalLocation", 0) == 2        # el1, el2

    topo2 = uh.build_fleet_topology([a, c])
    c2 = uh.count_by_type(topo2)
    assert c2.get("ExternalLocation", 0) == 1            # 'shared' deduped to one node
    # the first (empty-azure) occurrence picked up sa2 → maps under a real account, not Unmapped
    names = [n["name"] for n in _walk(topo2) if n.get("type") == "StorageAccount"]
    assert "sa2" in names

    # source inventories untouched (shallow copy)
    assert a.external_locations[0].get("azure") == {}


def _walk(n):
    yield n
    for k in n.get("children", []):
        yield from _walk(k)


def test_fleet_overview_totals_match_tree():
    ov = uh.build_fleet_overview([A, B, C])
    t = uh.build_fleet_tree([A, B, C])
    counts = uh.count_by_type(t)
    assert ov["totals"]["Catalog"] == counts["Catalog"] == 4   # 2 (ms-1) + 2 (ms-2)
    assert ov["totals"]["Table"] == counts["Table"] == 4
    assert ov["total_bytes_h"]                                  # non-empty (bytes summed)
    assert "metastore" in ov["metastore"] or "·" in ov["metastore"]


def test_build_overview_regression():
    # _aggregate_overview extraction preserves the single-workspace overview shape.
    o = uh.build_overview(A)
    assert set(o) == set(uh.build_fleet_overview([A]))
    assert o["totals"]["Catalog"] == 2 and o["totals"]["Table"] == 2


def test_all_five_fleet_renders_well_formed():
    t = uh.build_fleet_tree([A, B, C])
    nav = uh.build_nav("sat-uc-inventory-combined-2026-06-19", "Tree")
    renders = {
        "overview": uh.render_overview_html(uh.build_fleet_overview([A, B, C]), nav=nav),
        "tree": uh.render_tree_html(t, nav=nav),
        "star": uh.render_star_html(t, nav=nav),
        "hub": uh.render_hub_html(t, nav=nav),
        "topology": uh.render_topology_html(uh.build_fleet_topology([A, C]), nav=nav),
    }
    for name, h in renders.items():
        assert h.startswith("<!DOCTYPE html>"), name
        assert not re.search(r"__[A-Z]+__", h), f"{name}: leftover template token"
        assert h.rstrip().endswith("</html>"), name
    assert "Fleet" in renders["tree"]
