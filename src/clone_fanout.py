"""Multi-target fanout orchestrator — clone one source catalog to N target
workspaces in parallel.

Each target gets its own complete cross-workspace clone (Delta Sharing +
DEEP CLONE), with its own deterministic share/recipient/shared-catalog
names derived from `(source_host, source_catalog, target_host, dest_catalog,
target_metastore_id)`. Per-target source-side state is independent — a
failure on target B leaves the share/recipient on source for target A and C
in place, so they can keep running.

Why fanout instead of "one share, N recipients":

The naive optimisation would be to create one source share and grant it to
N recipients (one per target metastore), saving N-1 shares' worth of source
metadata. We don't do that today because:

1. Recipient-uniqueness rule forces per-target recipient anyway (Databricks
   enforces ONE recipient per (source_metastore, target_metastore_sharing_id)
   from a given source). So "one share, N recipients" still means N
   recipient creates, just N-1 fewer share creates.
2. Delta Shares are cheap metadata; the real cost is the data copy.
3. Per-target isolation means a partial failure on one target can't taint
   another target's run.

If/when we hit a customer with 100+ target fanout, we'll revisit. For
typical N-region DR (3-7 targets) the simpler isolated approach wins.

Result shape mirrors `clone_catalog`'s aggregate summary so downstream
report generators can consume it identically. Per-target detail is in
`per_target` as a list keyed by target host.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def run_cross_workspace_fanout(client: WorkspaceClient, config: dict) -> dict:
    """Run a cross-workspace clone to every target in `config["target_workspaces"]`
    in parallel.

    Returns an aggregate dict with `mode: "fanout"`, per-target results, and
    rolled-up totals (bytes, files, tables). One target's failure doesn't
    fail others — `status` is `"success"` only if EVERY target succeeded;
    otherwise `"partial"` (any succeeded) or `"failed"` (none succeeded).

    `fanout_max_parallel` caps how many targets run simultaneously. Higher
    values increase source-side egress bandwidth pressure (a cap of 5 is the
    default).
    """
    from src.clone_cross_workspace import run_cross_workspace_clone

    targets: list[dict] = list(config.get("target_workspaces") or [])
    if not targets:
        # Pydantic catches this at the API boundary, but guard anyway —
        # CLI / programmatic callers can hit this path.
        raise ValueError("target_workspaces is empty — nothing to fan out")

    max_parallel = int(config.get("fanout_max_parallel", 5))
    if max_parallel < 1:
        max_parallel = 1
    # Don't spawn more workers than there are targets (saves a thread pool
    # frame on the small-N case which is the common case).
    max_parallel = min(max_parallel, len(targets))

    fanout_start = time.time()
    logger.info(
        "Fanout starting: %d target(s), max_parallel=%d",
        len(targets), max_parallel,
    )

    # Each per-target run gets its own config dict — same source side, but
    # target_workspace replaced with that single entry. We strip the plural
    # field so the inner orchestrator can't loop back into fanout.
    per_target_configs = []
    for target in targets:
        sub = dict(config)
        sub["target_workspace"] = target
        sub.pop("target_workspaces", None)
        per_target_configs.append((target.get("host", "unknown"), sub))

    results: list[dict] = []

    def _run_one(target_label: str, sub_config: dict) -> dict:
        per_start = time.time()
        try:
            inner = run_cross_workspace_clone(client, sub_config)
            inner["target_host"] = target_label
            inner["target_status"] = inner.get("status", "success")
            inner["target_duration_seconds"] = round(time.time() - per_start, 1)
            return inner
        except Exception as e:
            # Per-target hard failure (auth, connectivity, raised mid-clone).
            # Log + return a synthetic failure record — fanout aggregator marks
            # status partial/failed based on the count of these.
            logger.exception(f"Fanout: target {target_label} failed: {e}")
            return {
                "target_host": target_label,
                "target_status": "failed",
                "target_duration_seconds": round(time.time() - per_start, 1),
                "error": str(e),
            }

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(_run_one, label, sub): label
            for label, sub in per_target_configs
        }
        for fut in as_completed(futures):
            results.append(fut.result())

    # Aggregate. `success` = every target succeeded; `failed` = no target
    # succeeded; `partial` = at least one succeeded and at least one failed.
    succeeded = sum(1 for r in results if r.get("target_status") == "success")
    failed = sum(1 for r in results if r.get("target_status") != "success")

    if failed == 0:
        agg_status = "success"
    elif succeeded == 0:
        agg_status = "failed"
    else:
        agg_status = "partial"

    bytes_copied = sum(int(r.get("bytes_copied", 0) or 0) for r in results)
    files_copied = sum(int(r.get("files_copied", 0) or 0) for r in results)
    tables_total = sum(int(r.get("tables_total", 0) or 0) for r in results)
    tables_cloned = sum(int(r.get("tables_cloned", 0) or 0) for r in results)
    tables_failed = sum(int(r.get("tables_failed", 0) or 0) for r in results)

    return {
        "mode": "fanout",
        "status": agg_status,
        "target_count": len(targets),
        "succeeded_targets": succeeded,
        "failed_targets": failed,
        "duration_seconds": round(time.time() - fanout_start, 1),
        "bytes_copied": bytes_copied,
        "files_copied": files_copied,
        "tables_total": tables_total,
        "tables_cloned": tables_cloned,
        "tables_failed": tables_failed,
        "per_target": results,
        "timestamp": datetime.now().isoformat(),
        "source_catalog": config.get("source_catalog"),
        "destination_catalog": config.get("destination_catalog"),
    }
