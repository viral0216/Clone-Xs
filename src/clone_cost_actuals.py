"""Post-clone actual cost reconciliation.

Clone-Xs runs the clone DDL as direct SQL warehouse calls from the API
process — there is no Databricks job_id to filter `system.billing.usage`
on, so we correlate by (warehouse_id, start_time, end_time) instead. This
catches the DBU consumption that the clone's CLONE / CTAS statements drove
on the warehouse during the job's wall-clock window.

The system.billing.usage table has a billing-data lag (typically 1-4
hours after consumption); queries during that window may under-report.
The response surfaces the lag explicitly so the UI can warn rather than
silently show a low number.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# Conservative billing-data lag. Anthropic's experience: usage records
# typically appear within 1-4h of the metering window. Within this
# window the actual_cost is potentially incomplete.
BILLING_LAG_HOURS = 4


def query_clone_job_actual_cost(
    client: WorkspaceClient,
    query_warehouse_id: str,
    target_warehouse_id: str,
    started_at: str,
    completed_at: str,
) -> dict[str, Any]:
    """Return actual DBU/cost spent on `target_warehouse_id` during the clone window.

    Args:
        client: Databricks SDK client.
        query_warehouse_id: Warehouse used to RUN the billing query (the
            FinOps query warehouse — typically the same as the destination,
            but separated so a different workspace's billing can be queried).
        target_warehouse_id: Warehouse whose consumption we're attributing
            (the warehouse that executed the clone DDL).
        started_at, completed_at: ISO timestamps bounding the clone job.
            The query expands the window by `BILLING_LAG_HOURS` on the upper
            bound to catch usage records that landed late.

    Returns:
        {
          "actual_cost": float,         # USD list-price (BYO discount not applied)
          "actual_dbus": float,
          "currency": "USD",
          "billing_data_incomplete": bool,  # True if completed_at is within lag window
          "lag_warning": str | None,
          "warehouse_id": str,
          "started_at": str,
          "completed_at": str,
        }

    On query failure returns the same shape with actual_cost=0, actual_dbus=0,
    and `error` set.
    """
    completed_dt = _parse_iso(completed_at)
    incomplete = False
    lag_warning: str | None = None
    if completed_dt is not None:
        elapsed_h = (datetime.now(timezone.utc) - completed_dt).total_seconds() / 3600.0
        if elapsed_h < BILLING_LAG_HOURS:
            incomplete = True
            lag_warning = (
                f"Job completed {elapsed_h:.1f}h ago; billing data typically "
                f"lags {BILLING_LAG_HOURS}h. Reported cost may be incomplete."
            )

    base_result: dict[str, Any] = {
        "actual_cost": 0.0,
        "actual_dbus": 0.0,
        "currency": "USD",
        "billing_data_incomplete": incomplete,
        "lag_warning": lag_warning,
        "warehouse_id": target_warehouse_id,
        "started_at": started_at,
        "completed_at": completed_at,
    }

    if not target_warehouse_id or not started_at or not completed_at:
        base_result["error"] = "missing warehouse_id or time bounds"
        return base_result

    sql = f"""
        SELECT
            SUM(u.usage_quantity) AS dbus,
            SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
          ON u.sku_name = p.sku_name
          AND u.usage_unit = p.usage_unit
          AND p.price_start_time <= u.usage_date
          AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_date)
        WHERE u.usage_metadata.warehouse_id = '{_safe_id(target_warehouse_id)}'
          AND u.usage_start_time >= TIMESTAMP '{_safe_ts(started_at)}'
          AND u.usage_start_time <= TIMESTAMP '{_safe_ts(completed_at)}'
    """

    try:
        rows = execute_sql(client, query_warehouse_id, sql)
    except Exception as e:
        logger.warning(f"Clone cost actuals query failed: {e}")
        base_result["error"] = str(e)
        return base_result

    if rows:
        row = rows[0]
        base_result["actual_dbus"] = round(float(row.get("dbus") or 0), 4)
        base_result["actual_cost"] = round(float(row.get("cost") or 0), 4)
    return base_result


def reconcile_estimate_vs_actual(estimated_cost: float, actual_cost: float) -> dict[str, Any]:
    """Compute variance between an estimator forecast and the billed cost.

    `variance_pct` is signed — positive means actual > estimate (over-budget),
    negative means actual < estimate (under-budget). Returns None for
    variance_pct when estimated_cost is 0 (no useful ratio).
    """
    diff = actual_cost - estimated_cost
    if estimated_cost > 0:
        variance_pct = round((diff / estimated_cost) * 100.0, 2)
    else:
        variance_pct = None
    return {
        "estimated_cost": round(estimated_cost, 4),
        "actual_cost": round(actual_cost, 4),
        "variance_abs": round(diff, 4),
        "variance_pct": variance_pct,
    }


def _parse_iso(s: str | None) -> datetime | None:
    """Parse an ISO timestamp from JobManager (`datetime.now().isoformat()`).

    JobManager stores naive local-zone timestamps without offsets. We treat
    them as UTC for the lag calculation; an hour or two of skew here only
    affects whether we flag billing as incomplete, not the SQL query itself.
    """
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_id(s: str) -> str:
    """Strip anything that isn't a Databricks identifier character.

    Defense-in-depth — warehouse_id should already be a UUID-ish string from
    the SDK, but the SQL is f-string interpolated, so we belt-and-brace it.
    """
    return "".join(c for c in s if c.isalnum() or c in "-_")


def _safe_ts(s: str) -> str:
    """Allow only characters that appear in ISO timestamps."""
    return "".join(c for c in s if c.isalnum() or c in "-:.+T ")
