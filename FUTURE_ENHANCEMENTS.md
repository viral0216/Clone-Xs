# Future Enhancements

Tracked items deferred for later prioritisation. Each entry is self-contained
with file paths, current behaviour, and proposed fix.

---

## Recently completed

### ✅ Cross-workspace clones as first-class operations across all surfaces

Both gaps shipped in the *Unreleased* changelog entry. End-to-end: the same
cross-workspace clone request now behaves identically across CLI/YAML, REST
API, and the scheduler.

- **Scheduler routing** — [src/scheduler.py](src/scheduler.py) `run_scheduled_clone`
  now branches on `config["target_workspace"]` and dispatches to
  `run_cross_workspace_clone`. Drift detection is skipped for cross-workspace
  runs because `compare_catalogs` only works within one metastore.
- **Pydantic API model** — [api/models/clone.py](api/models/clone.py) gained six
  fields:
  - On `TargetWorkspace`: `cleanup_after_clone`, `prune_share_extras`
  - On `CloneRequest`: `clone_views`, `clone_functions`, `clone_volumes`,
    `volume_max_file_mb`

  Defaults match the orchestrator's existing `config.get(field, default)`
  fallbacks, so existing callers see no behaviour change. New callers get
  Pydantic validation and the fields actually flowing through the API path
  instead of being silently dropped by `extra="ignore"`.

Documentation updated in [docs/docs/reference/configuration.md](docs/docs/reference/configuration.md),
[docs/docs/reference/api.md](docs/docs/reference/api.md), and
[docs/docs/reference/changelog.md](docs/docs/reference/changelog.md).

---

## Open

*(Add new tracked items below.)*
