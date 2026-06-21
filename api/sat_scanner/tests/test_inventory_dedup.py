"""Unit tests for metastore-level dedup of multi-workspace UC inventory.

Pure/offline: synthetic ``UCInventoryResult`` trees + monkeypatched enumeration —
no live Databricks, no network. Run: ``pytest sat_scanner/tests/test_inventory_dedup.py``
(from the ``assessment/`` dir, or with ``assessment`` on ``sys.path``).
"""
from __future__ import annotations

import asyncio

import sat_scanner.inventory as inv
from sat_scanner.inventory_models import (
    UCCatalog, UCColumn, UCInventoryResult, UCSchema, UCTable,
)


# ── synthetic UC tree ────────────────────────────────────────────────────────

def _catalog(name: str) -> UCCatalog:
    """A one-schema, one-table catalog (2 columns) named ``name``."""
    table = UCTable(full_name=f"{name}.s.t", catalog=name, schema="s", name="t",
                    table_type="MANAGED",
                    columns=[UCColumn(name="a"), UCColumn(name="b")], grants=[])
    schema = UCSchema(full_name=f"{name}.s", catalog=name, name="s", tables=[table])
    return UCCatalog(name=name, catalog_type="MANAGED_CATALOG", schemas=[schema])


def _make_targets(hosts):
    return [(h, "tok", h.rsplit("/", 1)[-1]) for h in hosts]


def _names(catalogs):
    return {c.name for c in catalogs}


# ── Test 1: grouping ─────────────────────────────────────────────────────────

def test_group_by_metastore():
    targets = _make_targets(["https://a", "https://b", "https://c",
                             "https://d", "https://e", "https://f"])
    probes = {
        "https://a": {"metastore_id": "ms-1", "catalog_names": {"c1", "c2"}, "reachable": True},
        "https://b": {"metastore_id": "ms-1", "catalog_names": {"c1", "c2", "c3"}, "reachable": True},
        "https://c": {"metastore_id": "ms-1", "catalog_names": {"c1", "c2"}, "reachable": True},
        "https://d": {"metastore_id": "ms-2", "catalog_names": {"d1"}, "reachable": True},
        "https://e": {"metastore_id": "", "catalog_names": {"e1"}, "reachable": True},
        "https://f": {"metastore_id": "", "catalog_names": set(), "reachable": False},
    }
    groups = inv._group_by_metastore(targets, probes)
    by_ms = {g["metastore_id"]: g for g in groups if g["dedup"]}
    assert set(by_ms) == {"ms-1"}                      # only ms-1 has >1 member
    assert len(by_ms["ms-1"]["members"]) == 3
    assert by_ms["ms-1"]["leader_host"] == "https://a"  # deterministic (min host)

    singles = [g for g in groups if not g["dedup"]]
    assert len(singles) == 3                            # ms-2, e (unknown), f (unreachable)
    unreachable = [g for g in singles if not g["reachable"]]
    assert len(unreachable) == 1 and unreachable[0]["members"][0][0] == "https://f"


# ── shared fakes for the enumeration path ────────────────────────────────────

def _install_fakes(monkeypatch, visible_by_host):
    """Patch the network-touching enumeration with synthetic catalogs.

    Returns ``enumerated`` — the ordered list of catalog names actually built
    (each catalog object is created exactly once and shared by reference).
    """
    made: dict[str, UCCatalog] = {}
    enumerated: list[str] = []

    def get_or_make(name: str) -> UCCatalog:
        if name not in made:
            made[name] = _catalog(name)
            enumerated.append(name)
        return made[name]

    async def fake_metastore_scoped(client, host, token, result, sem, **kw):
        result.metastore = {"current_assignment": {"metastore_id": "ms-1"}, "metastores": []}
        result.external_locations = [{"name": "el1"}]
        result.catalogs = [get_or_make(n) for n in sorted(visible_by_host[host])]
        return "ms-1", {c.name: c for c in result.catalogs}

    async def fake_subset(client, host, token, names, sem, **kw):
        return {n: get_or_make(n) for n in sorted(names)}

    async def fake_azure(client, host, token, result, *, skip_azure, quiet):
        result.azure = {"available": True}
        for loc in result.external_locations:
            loc["azure"] = {"ws": result.workspace_url}   # distinct per workspace

    monkeypatch.setattr(inv, "_enumerate_metastore_scoped", fake_metastore_scoped)
    monkeypatch.setattr(inv, "_enumerate_catalogs_subset", fake_subset)
    monkeypatch.setattr(inv, "_enumerate_azure_for", fake_azure)
    return enumerated


# ── Tests 2–5: union cache, visibility, share/copy, stats ────────────────────

def test_union_cache_and_assembly(monkeypatch):
    visible = {
        "https://a": {"c1", "c2"},
        "https://b": {"c1", "c2", "c3"},   # c3 ISOLATED to b (leader 'a' can't see it)
        "https://c": {"c1", "c2"},
    }
    enumerated = _install_fakes(monkeypatch, visible)

    group = {
        "metastore_id": "ms-1", "leader_host": "https://a", "reachable": True, "dedup": True,
        "members": [
            ("https://a", "tok", "a", {"c1", "c2"}),
            ("https://b", "tok", "b", {"c1", "c2", "c3"}),
            ("https://c", "tok", "c", {"c1", "c2"}),
        ],
    }
    results = asyncio.run(inv._inventory_group(group, opts={"source": "auto", "skip_azure": False}))
    by_name = {name: r for (name, _h, _t, r) in results}

    # 2: each catalog enumerated exactly once (c3 picked up from member b)
    assert enumerated == ["c1", "c2", "c3"]

    # 3: per-workspace visibility
    assert _names(by_name["a"].catalogs) == {"c1", "c2"}
    assert _names(by_name["b"].catalogs) == {"c1", "c2", "c3"}
    assert _names(by_name["c"].catalogs) == {"c1", "c2"}

    # 4: shared catalog refs, copied external_locations dicts (distinct azure maps)
    a, c = by_name["a"], by_name["c"]
    assert a.catalogs[0] is c.catalogs[0]                       # shared UCCatalog
    assert a.external_locations[0] is not c.external_locations[0]
    assert a.external_locations[0]["azure"] != c.external_locations[0]["azure"]

    # 5: stats recomputed per visible set
    assert a.stats["catalogs"] == 2 and a.stats["tables"] == 2
    assert by_name["b"].stats["catalogs"] == 3 and by_name["b"].stats["tables"] == 3


# ── Test 6: combined-report dedup ────────────────────────────────────────────

def test_aggregate_dedup(monkeypatch):
    visible = {"https://a": {"c1", "c2"}, "https://b": {"c1", "c2", "c3"}, "https://c": {"c1", "c2"}}
    _install_fakes(monkeypatch, visible)
    group = {
        "metastore_id": "ms-1", "leader_host": "https://a", "reachable": True, "dedup": True,
        "members": [("https://a", "t", "a", {"c1", "c2"}),
                    ("https://b", "t", "b", {"c1", "c2", "c3"}),
                    ("https://c", "t", "c", {"c1", "c2"})],
    }
    results = asyncio.run(inv._inventory_group(group, opts={"source": "auto", "skip_azure": False}))

    agg = inv.aggregate_inventories(results)
    # summed totals double/triple-count shared objects: 2 + 3 + 2
    assert agg["totals"]["tables"] == 7
    assert agg["totals"]["catalogs"] == 7
    # deduped: each shared object counted once per metastore
    assert len(agg["metastores"]) == 1
    ms = agg["metastores"][0]
    assert ms["metastore_id"] == "ms-1"
    assert sorted(ms["workspaces"]) == ["a", "b", "c"]
    assert ms["deduped_totals"]["tables"] == 3        # {t1, t2, t3}
    assert ms["deduped_totals"]["catalogs"] == 3
    assert ms["deduped_totals"]["columns"] == 6       # 3 tables × 2 cols
    assert agg["deduped_totals"]["tables"] == 3


# ── Test 7: fallback / delegation ────────────────────────────────────────────

def test_fleet_delegates_when_no_dedup(monkeypatch):
    called = {"many": 0, "probe": 0}

    async def fake_many(targets, **kw):
        called["many"] += 1
        return [(n, h, t, None) for (h, t, n) in targets]

    async def fake_probe(*a, **k):
        called["probe"] += 1
        return {}

    monkeypatch.setattr(inv, "run_inventory_many", fake_many)
    monkeypatch.setattr(inv, "_probe_all", fake_probe)

    targets = _make_targets(["https://a", "https://b"])
    asyncio.run(inv.run_inventory_fleet(targets, dedup=False))
    asyncio.run(inv.run_inventory_fleet(_make_targets(["https://solo"]), dedup=True))  # single target

    assert called["many"] == 2          # both delegated
    assert called["probe"] == 0         # dedup path never entered
