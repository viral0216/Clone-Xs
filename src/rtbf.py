"""RTBF (Right to Be Forgotten) engine — orchestrates GDPR Article 17 erasure requests.

Workflow: Submit → Discover → Impact Analysis → Execute Deletion → VACUUM → Verify → Certificate.
"""

import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.masking import _get_mask_expression
from src.pii_detection import COLUMN_NAME_PATTERNS
from src.rtbf_store import RTBFStore, STATUS_TRANSITIONS

logger = logging.getLogger(__name__)

# Subject type → PII column name patterns (reuses pii_detection patterns)
SUBJECT_TYPE_PATTERNS = {
    "email": [r"(?i)(email|e_mail|email.?addr)"],
    "phone": [r"(?i)(phone|mobile|cell|fax|tel)"],
    "ssn": [r"(?i)(ssn|social.?security|social_sec)"],
    "name": [r"(?i)(first.?name|last.?name|full.?name|middle.?name|given.?name|surname)"],
    "customer_id": [r"(?i)(customer.?id|cust.?id|client.?id|account.?id|user.?id|member.?id)"],
    "national_id": [r"(?i)(national.?id|national.?insurance|nino|aadhar|aadhaar)"],
    "passport": [r"(?i)(passport)"],
    "credit_card": [r"(?i)(credit.?card|card.?num|cc.?num|pan)"],
}

DEFAULT_DEADLINE_DAYS = 30
DEFAULT_STRATEGY = "delete"


class RTBFManager:
    """Orchestrates RTBF (Right to Be Forgotten) erasure requests across cloned catalogs."""

    def __init__(
        self,
        client: WorkspaceClient,
        warehouse_id: str,
        config: dict | None = None,
        state_catalog: str = "clone_audit",
        state_schema: str = "rtbf",
        plugin_manager=None,
    ):
        self.client = client
        self.warehouse_id = warehouse_id
        self.config = config or {}
        self.plugin_manager = plugin_manager

        rtbf_config = self.config.get("rtbf", {})
        catalog = self.config.get("audit_trail", {}).get("catalog", state_catalog)

        self.store = RTBFStore(client, warehouse_id, catalog, state_schema)
        self.deadline_days = rtbf_config.get("deadline_days", DEFAULT_DEADLINE_DAYS)
        self.default_strategy = rtbf_config.get("default_strategy", DEFAULT_STRATEGY)
        self.auto_vacuum = rtbf_config.get("auto_vacuum", True)
        self.vacuum_retention_hours = rtbf_config.get("vacuum_retention_hours", 0)
        self.require_approval = rtbf_config.get("require_approval", True)
        self.verification_required = rtbf_config.get("verification_required", True)
        self.certificate_auto_generate = rtbf_config.get("certificate_auto_generate", True)
        self.certificate_output_dir = rtbf_config.get("certificate_output_dir", "reports/rtbf")
        self.exclude_schemas = rtbf_config.get(
            "exclude_schemas", ["information_schema", "default"]
        )

    def init_tables(self) -> None:
        """Initialize RTBF Delta tables."""
        self.store.init_tables()

    # ── Submit ────────────────────────────────────────────────────────────

    def submit_request(
        self,
        subject_type: str,
        subject_value: str,
        requester_email: str,
        requester_name: str,
        legal_basis: str,
        strategy: str | None = None,
        scope_catalogs: list[str] | None = None,
        grace_period_days: int = 0,
        subject_column: str | None = None,
        notes: str | None = None,
    ) -> dict:
        """Submit a new RTBF erasure request.

        Returns dict with request_id, status, deadline.
        """
        request_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(days=self.deadline_days)

        grace_period_ends = None
        if grace_period_days > 0:
            grace_period_ends = (now + timedelta(days=grace_period_days)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        # Hash the subject value — never store raw PII in audit tables
        subject_value_hash = hashlib.sha256(subject_value.encode()).hexdigest()

        created_by = self._get_current_user()

        self.store.save_request(
            request_id=request_id,
            subject_type=subject_type,
            subject_value_hash=subject_value_hash,
            requester_email=requester_email,
            requester_name=requester_name,
            legal_basis=legal_basis,
            deadline=deadline.strftime("%Y-%m-%d %H:%M:%S"),
            strategy=strategy or self.default_strategy,
            scope_catalogs=scope_catalogs,
            grace_period_days=grace_period_days,
            grace_period_ends=grace_period_ends,
            subject_column=subject_column,
            created_by=created_by,
            notes=notes,
        )

        logger.info(
            f"RTBF request {request_id} submitted for {subject_type} "
            f"(deadline: {deadline.strftime('%Y-%m-%d')})"
        )

        # Fire plugin hooks
        self._run_plugin_hook("run_on_rtbf_request", request_id, subject_type, subject_value_hash)

        # Send notifications
        self._notify(
            f"RTBF Request Submitted — {subject_type}",
            f"Request `{request_id[:8]}` submitted by {requester_name}. "
            f"Strategy: {strategy or self.default_strategy}. "
            f"Deadline: {deadline.strftime('%Y-%m-%d')}.",
            "submitted",
        )

        return {
            "request_id": request_id,
            "status": "received",
            "deadline": deadline.isoformat(),
            "message": f"RTBF request submitted. Deadline: {deadline.strftime('%Y-%m-%d')}",
        }

    # ── Discover ──────────────────────────────────────────────────────────

    def discover_subject(self, request_id: str, subject_value: str) -> dict:
        """Find all occurrences of a data subject across cloned catalogs.

        Searches:
        1. PII detection results for matching column types
        2. Clone lineage for all destination catalogs
        3. Executes COUNT queries to find actual rows

        Args:
            request_id: The RTBF request ID.
            subject_value: The actual value to search for (e.g., email address).

        Returns:
            Discovery summary with list of affected tables.
        """
        self.store.update_request_status(request_id, "discovering")

        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        subject_type = request.get("subject_type", "")
        subject_column = request.get("subject_column")
        scope_catalogs_json = request.get("scope_catalogs", "[]")
        scope_catalogs = json.loads(scope_catalogs_json) if scope_catalogs_json else []

        # Step 1: Determine which catalogs to search
        catalogs = self._get_search_catalogs(scope_catalogs)
        if not catalogs:
            self.store.update_request_status(
                request_id, "failed", error_message="No catalogs found to search"
            )
            return {"affected_tables": [], "total_rows": 0, "error": "No catalogs found"}

        # Step 2: Find candidate columns
        candidate_columns = self._find_subject_columns(
            subject_type, catalogs, explicit_column=subject_column
        )

        # Step 3: Query each candidate for matching rows
        hits = []
        total_rows = 0
        for col_info in candidate_columns:
            cat = col_info["catalog"]
            sch = col_info["schema"]
            tbl = col_info["table"]
            col = col_info["column"]
            fqn = f"`{cat}`.`{sch}`.`{tbl}`"

            action_id = str(uuid.uuid4())
            try:
                count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE `{col}` = '{subject_value}'"
                rows = execute_sql(self.client, self.warehouse_id, count_sql)
                count = int(rows[0]["cnt"]) if rows else 0

                if count > 0:
                    hits.append({
                        "catalog": cat,
                        "schema": sch,
                        "table": tbl,
                        "column": col,
                        "row_count": count,
                    })
                    total_rows += count

                    self.store.save_action(
                        action_id=action_id,
                        request_id=request_id,
                        action_type="discover",
                        catalog=cat,
                        schema_name=sch,
                        table_name=tbl,
                        column_name=col,
                        rows_before=count,
                        rows_affected=count,
                        status="completed",
                        sql_executed=count_sql,
                    )
            except Exception as e:
                logger.warning(f"Failed to query {fqn}.{col}: {e}")
                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type="discover",
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    column_name=col,
                    status="failed",
                    error_message=str(e),
                )

        # Save discovery results
        discovery_json = json.dumps(hits)
        self.store.update_request_status(
            request_id,
            "analyzed",
            discovery_json=discovery_json,
            affected_tables=len(hits),
            affected_rows=total_rows,
        )

        logger.info(
            f"RTBF discovery for {request_id}: {len(hits)} tables, {total_rows} total rows"
        )
        return {
            "request_id": request_id,
            "affected_tables": hits,
            "total_tables": len(hits),
            "total_rows": total_rows,
            "catalogs_searched": catalogs,
        }

    def analyze_impact(self, request_id: str) -> dict:
        """Summarize the impact of an RTBF deletion."""
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        discovery_json = request.get("discovery_json", "[]")
        hits = json.loads(discovery_json) if discovery_json else []

        catalogs_affected = list({h["catalog"] for h in hits})
        schemas_affected = list({f"{h['catalog']}.{h['schema']}" for h in hits})

        return {
            "request_id": request_id,
            "status": request.get("status"),
            "subject_type": request.get("subject_type"),
            "strategy": request.get("strategy"),
            "deadline": request.get("deadline"),
            "total_tables": request.get("affected_tables", 0),
            "total_rows": request.get("affected_rows", 0),
            "catalogs_affected": catalogs_affected,
            "schemas_affected": schemas_affected,
            "tables": hits,
        }

    # ── Execute Deletion ──────────────────────────────────────────────────

    def execute_deletion(
        self,
        request_id: str,
        subject_value: str,
        strategy: str | None = None,
        dry_run: bool = False,
    ) -> dict:
        """Execute the RTBF deletion/anonymization across all affected tables.

        Args:
            request_id: The RTBF request ID.
            subject_value: The actual subject value for WHERE clauses.
            strategy: Override strategy (delete, anonymize, pseudonymize).
            dry_run: If True, generate SQL but don't execute.

        Returns:
            Execution summary.
        """
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        current_status = request.get("status", "")
        if current_status not in ("approved", "analyzed"):
            if self.require_approval and current_status != "approved":
                raise ValueError(
                    f"Request must be approved before execution (current: {current_status})"
                )

        strategy = strategy or request.get("strategy", self.default_strategy)
        discovery_json = request.get("discovery_json", "[]")
        hits = json.loads(discovery_json) if discovery_json else []

        if not hits:
            return {"request_id": request_id, "message": "No affected tables found", "actions": []}

        if not dry_run:
            self.store.update_request_status(request_id, "executing")
            self._run_plugin_hook("run_on_rtbf_deletion_start", request_id, hits)
            self._notify(
                f"RTBF Deletion Started — {len(hits)} tables",
                f"Request `{request_id[:8]}` executing {strategy} on {len(hits)} tables.",
                "executing",
            )

        actions = []
        total_deleted = 0
        executed_by = self._get_current_user()

        for hit in hits:
            cat = hit["catalog"]
            sch = hit["schema"]
            tbl = hit["table"]
            col = hit["column"]
            fqn = f"`{cat}`.`{sch}`.`{tbl}`"
            action_id = str(uuid.uuid4())
            start = time.time()

            try:
                # Get row count before
                rows_before = hit.get("row_count", 0)

                if strategy == "delete":
                    del_sql = f"DELETE FROM {fqn} WHERE `{col}` = '{subject_value}'"
                elif strategy == "anonymize":
                    del_sql = self._build_anonymize_sql(
                        cat, sch, tbl, col, subject_value
                    )
                elif strategy == "pseudonymize":
                    del_sql = self._build_pseudonymize_sql(fqn, col, subject_value)
                else:
                    del_sql = f"DELETE FROM {fqn} WHERE `{col}` = '{subject_value}'"

                if dry_run:
                    actions.append({
                        "action_id": action_id,
                        "table": f"{cat}.{sch}.{tbl}",
                        "column": col,
                        "strategy": strategy,
                        "sql": del_sql,
                        "dry_run": True,
                        "rows_before": rows_before,
                    })
                    continue

                execute_sql(self.client, self.warehouse_id, del_sql)
                duration = time.time() - start

                # Get row count after
                count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE `{col}` = '{subject_value}'"
                count_rows = execute_sql(self.client, self.warehouse_id, count_sql)
                rows_after = int(count_rows[0]["cnt"]) if count_rows else 0
                rows_affected = rows_before - rows_after

                total_deleted += rows_affected

                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type=strategy,
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    column_name=col,
                    rows_before=rows_before,
                    rows_affected=rows_affected,
                    rows_after=rows_after,
                    sql_executed=del_sql,
                    status="completed",
                    executed_by=executed_by,
                    duration_seconds=duration,
                )

                actions.append({
                    "action_id": action_id,
                    "table": f"{cat}.{sch}.{tbl}",
                    "column": col,
                    "strategy": strategy,
                    "rows_before": rows_before,
                    "rows_affected": rows_affected,
                    "rows_after": rows_after,
                    "duration_seconds": round(duration, 2),
                })

            except Exception as e:
                duration = time.time() - start
                logger.error(f"RTBF deletion failed for {fqn}: {e}")
                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type=strategy,
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    column_name=col,
                    status="failed",
                    error_message=str(e),
                    executed_by=executed_by,
                    duration_seconds=duration,
                )
                actions.append({
                    "action_id": action_id,
                    "table": f"{cat}.{sch}.{tbl}",
                    "error": str(e),
                })

        if not dry_run:
            failed = sum(1 for a in actions if "error" in a)
            if failed == len(actions):
                self.store.update_request_status(
                    request_id, "failed", error_message="All table deletions failed"
                )
                self._notify(
                    "RTBF Deletion FAILED",
                    f"Request `{request_id[:8]}` — all {len(actions)} table deletions failed.",
                    "failed",
                )
            else:
                self.store.update_request_status(request_id, "deleted_pending_vacuum")
                summary = {"total_rows_affected": total_deleted, "total_tables": len(actions), "failed": failed}
                self._run_plugin_hook("run_on_rtbf_deletion_complete", request_id, summary)
                self._notify(
                    f"RTBF Deletion Complete — {total_deleted} rows",
                    f"Request `{request_id[:8]}` deleted {total_deleted} rows across {len(actions) - failed} tables. "
                    f"VACUUM pending.",
                    "completed",
                )

        logger.info(
            f"RTBF execution for {request_id}: {total_deleted} rows {'would be ' if dry_run else ''}"
            f"affected across {len(actions)} tables"
        )
        return {
            "request_id": request_id,
            "strategy": strategy,
            "dry_run": dry_run,
            "total_rows_affected": total_deleted,
            "total_tables": len(actions),
            "actions": actions,
        }

    # ── VACUUM ────────────────────────────────────────────────────────────

    def execute_vacuum(self, request_id: str, retention_hours: int | None = None) -> dict:
        """VACUUM all affected tables to physically remove Delta history.

        This is critical for true GDPR compliance — without VACUUM,
        time-travel can still access the deleted data.
        """
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        retention = retention_hours if retention_hours is not None else self.vacuum_retention_hours
        discovery_json = request.get("discovery_json", "[]")
        hits = json.loads(discovery_json) if discovery_json else []

        if not hits:
            return {"request_id": request_id, "message": "No tables to vacuum"}

        self.store.update_request_status(request_id, "vacuuming")
        executed_by = self._get_current_user()
        results = []

        # Deduplicate tables (a table may appear multiple times for different columns)
        unique_tables = {}
        for h in hits:
            key = f"{h['catalog']}.{h['schema']}.{h['table']}"
            if key not in unique_tables:
                unique_tables[key] = h

        for key, hit in unique_tables.items():
            cat = hit["catalog"]
            sch = hit["schema"]
            tbl = hit["table"]
            fqn = f"`{cat}`.`{sch}`.`{tbl}`"
            action_id = str(uuid.uuid4())
            start = time.time()

            try:
                # Temporarily disable retention check for aggressive vacuum
                if retention == 0:
                    execute_sql(
                        self.client, self.warehouse_id,
                        f"ALTER TABLE {fqn} SET TBLPROPERTIES "
                        f"('delta.retentionDurationCheck.enabled' = 'false')",
                    )

                vacuum_sql = f"VACUUM {fqn} RETAIN {retention} HOURS"
                execute_sql(self.client, self.warehouse_id, vacuum_sql)

                # Restore retention check
                if retention == 0:
                    execute_sql(
                        self.client, self.warehouse_id,
                        f"ALTER TABLE {fqn} SET TBLPROPERTIES "
                        f"('delta.retentionDurationCheck.enabled' = 'true')",
                    )

                duration = time.time() - start
                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type="vacuum",
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    sql_executed=vacuum_sql,
                    status="completed",
                    executed_by=executed_by,
                    duration_seconds=duration,
                )
                results.append({"table": key, "status": "completed", "duration_seconds": round(duration, 2)})

            except Exception as e:
                duration = time.time() - start
                logger.error(f"VACUUM failed for {fqn}: {e}")
                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type="vacuum",
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    status="failed",
                    error_message=str(e),
                    executed_by=executed_by,
                    duration_seconds=duration,
                )
                results.append({"table": key, "status": "failed", "error": str(e)})

        failed = sum(1 for r in results if r["status"] == "failed")
        if failed == len(results):
            self.store.update_request_status(
                request_id, "failed", error_message="All VACUUM operations failed"
            )
        else:
            self.store.update_request_status(request_id, "vacuumed")

        return {
            "request_id": request_id,
            "tables_vacuumed": len(results) - failed,
            "tables_failed": failed,
            "retention_hours": retention,
            "results": results,
        }

    # ── Verify ────────────────────────────────────────────────────────────

    def verify_deletion(self, request_id: str, subject_value: str) -> dict:
        """Verify that all subject data has been completely removed.

        Re-runs discovery queries and confirms zero rows remain.
        """
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        self.store.update_request_status(request_id, "verifying")

        discovery_json = request.get("discovery_json", "[]")
        hits = json.loads(discovery_json) if discovery_json else []
        executed_by = self._get_current_user()

        verification_results = []
        all_clear = True

        for hit in hits:
            cat = hit["catalog"]
            sch = hit["schema"]
            tbl = hit["table"]
            col = hit["column"]
            fqn = f"`{cat}`.`{sch}`.`{tbl}`"
            action_id = str(uuid.uuid4())

            try:
                count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE `{col}` = '{subject_value}'"
                rows = execute_sql(self.client, self.warehouse_id, count_sql)
                remaining = int(rows[0]["cnt"]) if rows else 0

                passed = remaining == 0
                if not passed:
                    all_clear = False

                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type="verify",
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    column_name=col,
                    rows_after=remaining,
                    status="completed" if passed else "failed",
                    executed_by=executed_by,
                    sql_executed=count_sql,
                )
                verification_results.append({
                    "table": f"{cat}.{sch}.{tbl}",
                    "column": col,
                    "remaining_rows": remaining,
                    "passed": passed,
                })

            except Exception as e:
                all_clear = False
                logger.error(f"Verification failed for {fqn}.{col}: {e}")
                self.store.save_action(
                    action_id=action_id,
                    request_id=request_id,
                    action_type="verify",
                    catalog=cat,
                    schema_name=sch,
                    table_name=tbl,
                    column_name=col,
                    status="failed",
                    error_message=str(e),
                    executed_by=executed_by,
                )
                verification_results.append({
                    "table": f"{cat}.{sch}.{tbl}",
                    "column": col,
                    "error": str(e),
                    "passed": False,
                })

        new_status = "verified" if all_clear else "failed"
        error_msg = None if all_clear else "Verification found remaining subject data"
        self.store.update_request_status(request_id, new_status, error_message=error_msg)

        if all_clear:
            self._notify(
                "RTBF Verification PASSED",
                f"Request `{request_id[:8]}` — all subject data confirmed deleted.",
                "verified",
            )
        else:
            failures = [r for r in verification_results if not r.get("passed")]
            self._run_plugin_hook("run_on_rtbf_verification_failed", request_id, failures)
            self._notify(
                "RTBF Verification FAILED",
                f"Request `{request_id[:8]}` — {len(failures)} table(s) still contain subject data!",
                "failed",
            )

        return {
            "request_id": request_id,
            "all_clear": all_clear,
            "status": new_status,
            "results": verification_results,
        }

    # ── Certificate ───────────────────────────────────────────────────────

    def generate_certificate(self, request_id: str, output_dir: str | None = None) -> dict:
        """Generate a GDPR-compliant deletion certificate.

        Produces HTML and JSON evidence files for the DPO/legal team.
        """
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        actions = self.store.get_actions(request_id)
        generated_by = self._get_current_user()
        certificate_id = str(uuid.uuid4())

        # Build summary
        discovery_actions = [a for a in actions if a.get("action_type") == "discover"]
        deletion_actions = [
            a for a in actions
            if a.get("action_type") in ("delete", "anonymize", "pseudonymize")
        ]
        vacuum_actions = [a for a in actions if a.get("action_type") == "vacuum"]
        verify_actions = [a for a in actions if a.get("action_type") == "verify"]

        total_rows_deleted = sum(
            int(a.get("rows_affected", 0) or 0) for a in deletion_actions
        )
        tables_processed = len({
            f"{a.get('catalog')}.{a.get('schema_name')}.{a.get('table_name')}"
            for a in deletion_actions
        })
        verification_passed = all(
            a.get("status") == "completed" for a in verify_actions
        ) if verify_actions else False

        summary = {
            "certificate_id": certificate_id,
            "request_id": request_id,
            "subject_type": request.get("subject_type"),
            "subject_value_hash": request.get("subject_value_hash"),
            "legal_basis": request.get("legal_basis"),
            "strategy": request.get("strategy"),
            "request_created": request.get("created_at"),
            "request_completed": request.get("completed_at") or request.get("updated_at"),
            "deadline": request.get("deadline"),
            "tables_processed": tables_processed,
            "total_rows_deleted": total_rows_deleted,
            "verification_passed": verification_passed,
            "discovery_summary": {
                "tables_scanned": len(discovery_actions),
                "tables_with_data": request.get("affected_tables", 0),
            },
            "deletion_summary": {
                "total_actions": len(deletion_actions),
                "successful": sum(1 for a in deletion_actions if a.get("status") == "completed"),
                "failed": sum(1 for a in deletion_actions if a.get("status") == "failed"),
            },
            "vacuum_summary": {
                "tables_vacuumed": sum(1 for a in vacuum_actions if a.get("status") == "completed"),
                "failed": sum(1 for a in vacuum_actions if a.get("status") == "failed"),
            },
            "verification_summary": {
                "tables_verified": len(verify_actions),
                "all_clear": verification_passed,
            },
            "actions": [
                {
                    "action_id": a.get("action_id"),
                    "type": a.get("action_type"),
                    "table": f"{a.get('catalog')}.{a.get('schema_name')}.{a.get('table_name')}",
                    "column": a.get("column_name"),
                    "status": a.get("status"),
                    "rows_affected": a.get("rows_affected"),
                    "executed_at": str(a.get("executed_at", "")),
                }
                for a in actions
            ],
        }

        summary_json = json.dumps(summary, default=str)

        # Generate HTML report
        html_report = self._build_certificate_html(summary)

        # Save to Delta
        self.store.save_certificate(
            certificate_id=certificate_id,
            request_id=request_id,
            generated_by=generated_by,
            summary_json=summary_json,
            tables_processed=tables_processed,
            rows_deleted=total_rows_deleted,
            verification_passed=verification_passed,
            json_report=summary_json,
        )

        # Write files
        out_dir = output_dir or self.certificate_output_dir
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        paths = {}

        json_path = os.path.join(out_dir, f"rtbf_certificate_{request_id[:8]}_{ts}.json")
        with open(json_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        paths["json"] = json_path

        html_path = os.path.join(out_dir, f"rtbf_certificate_{request_id[:8]}_{ts}.html")
        with open(html_path, "w") as f:
            f.write(html_report)
        paths["html"] = html_path

        # Mark request as completed if verified
        if request.get("status") == "verified":
            self.store.update_request_status(request_id, "completed")

        logger.info(f"RTBF certificate generated: {paths}")
        return {
            "certificate_id": certificate_id,
            "request_id": request_id,
            "tables_processed": tables_processed,
            "rows_deleted": total_rows_deleted,
            "verification_passed": verification_passed,
            "paths": paths,
        }

    # ── Status management ─────────────────────────────────────────────────

    def approve_request(self, request_id: str) -> dict:
        """Approve an RTBF request for execution."""
        return self._transition_status(request_id, "approved")

    def hold_request(self, request_id: str) -> dict:
        """Place an RTBF request on hold."""
        return self._transition_status(request_id, "on_hold")

    def cancel_request(self, request_id: str, reason: str = "") -> dict:
        """Cancel an RTBF request."""
        return self._transition_status(request_id, "cancelled", error_message=reason)

    def get_request(self, request_id: str) -> dict | None:
        """Get full request details."""
        return self.store.get_request(request_id)

    def list_requests(self, **kwargs) -> list[dict]:
        """List requests with optional filters."""
        return self.store.list_requests(**kwargs)

    def get_overdue_requests(self) -> list[dict]:
        """Get requests that have passed their GDPR deadline."""
        return self.store.get_overdue_requests()

    def get_dashboard(self) -> dict:
        """Get RTBF dashboard summary."""
        stats = self.store.get_dashboard_stats()
        overdue = self.store.get_overdue_requests()
        recent = self.store.list_requests(limit=10)
        return {
            "stats": stats,
            "overdue_requests": overdue,
            "recent_requests": recent,
        }

    # ── Private helpers ───────────────────────────────────────────────────

    def _transition_status(
        self, request_id: str, new_status: str, error_message: str | None = None,
    ) -> dict:
        """Validate and execute a status transition."""
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"RTBF request {request_id} not found")

        current = request.get("status", "")
        allowed = STATUS_TRANSITIONS.get(current, [])
        if new_status not in allowed:
            raise ValueError(
                f"Cannot transition from '{current}' to '{new_status}'. "
                f"Allowed: {allowed}"
            )

        self.store.update_request_status(request_id, new_status, error_message=error_message)
        return {"request_id": request_id, "status": new_status, "previous_status": current}

    def _get_search_catalogs(self, scope_catalogs: list[str]) -> list[str]:
        """Determine which catalogs to search for subject data."""
        if scope_catalogs:
            return scope_catalogs

        # Use source + destination from config, plus lineage destinations
        catalogs = set()
        src = self.config.get("source_catalog")
        dst = self.config.get("destination_catalog")
        if src:
            catalogs.add(src)
        if dst:
            catalogs.add(dst)

        # Query lineage for additional destinations
        audit_catalog = self.config.get("audit_trail", {}).get("catalog", "clone_audit")
        lineage_fqn = f"{audit_catalog}.lineage.clone_lineage"
        try:
            sql = f"SELECT DISTINCT dest_catalog FROM {lineage_fqn}"
            rows = execute_sql(self.client, self.warehouse_id, sql)
            for r in rows:
                if r.get("dest_catalog"):
                    catalogs.add(r["dest_catalog"])
        except Exception:
            pass  # lineage table may not exist

        return list(catalogs)

    def _find_subject_columns(
        self,
        subject_type: str,
        catalogs: list[str],
        explicit_column: str | None = None,
    ) -> list[dict]:
        """Find columns that could contain subject identifier data.

        Uses PII detection patterns + information_schema to find candidate columns.
        """
        candidates = []

        # Get column name patterns for this subject type
        if subject_type == "custom" and explicit_column:
            patterns = [re.escape(explicit_column)]
        elif subject_type in SUBJECT_TYPE_PATTERNS:
            patterns = SUBJECT_TYPE_PATTERNS[subject_type]
        else:
            # Fall back to checking all PII column patterns
            patterns = list(COLUMN_NAME_PATTERNS.keys())

        for catalog in catalogs:
            try:
                # Query information_schema for all columns
                col_sql = f"""
                    SELECT table_schema, table_name, column_name, data_type
                    FROM {catalog}.information_schema.columns
                    WHERE table_schema NOT IN ({', '.join(f"'{s}'" for s in self.exclude_schemas)})
                """
                columns = execute_sql(self.client, self.warehouse_id, col_sql)

                for col in columns:
                    col_name = col.get("column_name", "")
                    # Check if column matches subject type patterns
                    if explicit_column and col_name.lower() == explicit_column.lower():
                        candidates.append({
                            "catalog": catalog,
                            "schema": col["table_schema"],
                            "table": col["table_name"],
                            "column": col_name,
                            "data_type": col.get("data_type", ""),
                        })
                    elif not explicit_column:
                        for pattern in patterns:
                            if re.search(pattern, col_name):
                                candidates.append({
                                    "catalog": catalog,
                                    "schema": col["table_schema"],
                                    "table": col["table_name"],
                                    "column": col_name,
                                    "data_type": col.get("data_type", ""),
                                })
                                break

            except Exception as e:
                logger.warning(f"Failed to query columns in catalog {catalog}: {e}")

        # Also check PII detection results for additional columns
        audit_catalog = self.config.get("audit_trail", {}).get("catalog", "clone_audit")
        pii_table = f"{audit_catalog}.pii.pii_detections"
        try:
            pii_type_map = {
                "email": "EMAIL",
                "phone": "PHONE",
                "ssn": "SSN",
                "name": "PERSON_NAME",
                "customer_id": None,  # no PII type for generic IDs
                "national_id": "NATIONAL_ID",
                "passport": "PASSPORT",
                "credit_card": "CREDIT_CARD",
            }
            pii_type = pii_type_map.get(subject_type)
            if pii_type:
                sql = f"""
                    SELECT DISTINCT catalog, schema_name, table_name, column_name
                    FROM {pii_table}
                    WHERE pii_type = '{pii_type}'
                """
                if catalogs:
                    cat_list = ", ".join(f"'{c}'" for c in catalogs)
                    sql += f" AND catalog IN ({cat_list})"

                pii_cols = execute_sql(self.client, self.warehouse_id, sql)
                existing = {
                    (c["catalog"], c["schema"], c["table"], c["column"])
                    for c in candidates
                }
                for pc in pii_cols:
                    key = (
                        pc.get("catalog", ""),
                        pc.get("schema_name", ""),
                        pc.get("table_name", ""),
                        pc.get("column_name", ""),
                    )
                    if key not in existing:
                        candidates.append({
                            "catalog": pc["catalog"],
                            "schema": pc["schema_name"],
                            "table": pc["table_name"],
                            "column": pc["column_name"],
                        })
        except Exception:
            pass  # PII table may not exist yet

        return candidates

    def _build_anonymize_sql(
        self, catalog: str, schema: str, table: str, identifier_col: str, subject_value: str,
    ) -> str:
        """Build an UPDATE statement that anonymizes all PII columns for matching rows."""
        fqn = f"`{catalog}`.`{schema}`.`{table}`"

        # Get all columns for this table
        col_sql = f"""
            SELECT column_name, data_type
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
        columns = execute_sql(self.client, self.warehouse_id, col_sql)

        # Identify PII columns to anonymize
        update_parts = []
        for col in columns:
            col_name = col["column_name"]
            if col_name == identifier_col:
                continue  # Don't anonymize the identifier itself yet

            # Check if this column is PII
            for pattern, pii_type in COLUMN_NAME_PATTERNS.items():
                if re.search(pattern, col_name):
                    from src.pii_detection import SUGGESTED_MASKING
                    mask_strategy = SUGGESTED_MASKING.get(pii_type, "redact")
                    mask_expr = _get_mask_expression(col_name, mask_strategy, col.get("data_type", "STRING"))
                    if mask_expr:
                        update_parts.append(f"`{col_name}` = {mask_expr}")
                    break

        # Also anonymize the identifier column
        id_mask = _get_mask_expression(identifier_col, "hash", "STRING")
        if id_mask:
            update_parts.append(f"`{identifier_col}` = {id_mask}")

        if not update_parts:
            # Fallback: just hash the identifier
            return f"UPDATE {fqn} SET `{identifier_col}` = SHA2(`{identifier_col}`, 256) WHERE `{identifier_col}` = '{subject_value}'"

        return f"UPDATE {fqn} SET {', '.join(update_parts)} WHERE `{identifier_col}` = '{subject_value}'"

    def _build_pseudonymize_sql(
        self, fqn: str, identifier_col: str, subject_value: str,
    ) -> str:
        """Build an UPDATE that replaces the identifier with a pseudonym."""
        pseudonym = hashlib.sha256(
            f"pseudo_{subject_value}_{uuid.uuid4()}".encode()
        ).hexdigest()[:16]
        return (
            f"UPDATE {fqn} SET `{identifier_col}` = 'PSEUDO_{pseudonym}' "
            f"WHERE `{identifier_col}` = '{subject_value}'"
        )

    def _run_plugin_hook(self, hook_name: str, *args) -> None:
        """Run a plugin hook if a plugin manager is available."""
        if self.plugin_manager is None:
            return
        try:
            fn = getattr(self.plugin_manager, hook_name, None)
            if fn:
                fn(*args)
        except Exception as e:
            logger.warning(f"Plugin hook {hook_name} failed: {e}")

    def _notify(self, title: str, message: str, event_type: str = "info") -> None:
        """Send RTBF notifications via Slack/Teams if configured."""
        slack_url = self.config.get("slack_webhook_url", "")
        teams_url = self.config.get("teams_webhook_url", "")

        if not slack_url and not teams_url:
            return

        emoji = {
            "submitted": ":clipboard:", "executing": ":rotating_light:",
            "completed": ":white_check_mark:", "verified": ":shield:",
            "failed": ":x:", "overdue": ":warning:",
        }.get(event_type, ":information_source:")

        if slack_url:
            try:
                from urllib.request import Request, urlopen
                payload = json.dumps({
                    "blocks": [
                        {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} {title}"}},
                        {"type": "section", "text": {"type": "mrkdwn", "text": message}},
                    ],
                })
                req = Request(slack_url, data=payload.encode(), headers={"Content-Type": "application/json"})
                urlopen(req, timeout=10)
            except Exception as e:
                logger.warning(f"Slack RTBF notification failed: {e}")

        if teams_url:
            try:
                from urllib.request import Request, urlopen
                payload = json.dumps({
                    "@type": "MessageCard", "summary": title,
                    "themeColor": "E8453C" if event_type == "failed" else "2196F3",
                    "title": title, "text": message,
                })
                req = Request(teams_url, data=payload.encode(), headers={"Content-Type": "application/json"})
                urlopen(req, timeout=10)
            except Exception as e:
                logger.warning(f"Teams RTBF notification failed: {e}")

    def check_approaching_deadlines(self, warn_days: int = 5) -> list[dict]:
        """Get RTBF requests approaching their GDPR deadline.

        Returns requests where deadline is within warn_days from now.
        Useful for scheduled deadline monitoring and alerts.
        """
        now = datetime.now(timezone.utc)
        warn_date = (now + timedelta(days=warn_days)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        SELECT * FROM {self.store._requests_table}
        WHERE deadline <= '{warn_date}'
          AND deadline > '{now_str}'
          AND status NOT IN ('completed', 'cancelled')
        ORDER BY deadline ASC
        """
        try:
            approaching = execute_sql(self.client, self.warehouse_id, sql)
            if approaching:
                self._notify(
                    f"RTBF Deadline Warning — {len(approaching)} request(s)",
                    f"{len(approaching)} request(s) approaching GDPR deadline within {warn_days} days.",
                    "overdue",
                )
            return approaching
        except Exception as e:
            logger.warning(f"Failed to check approaching deadlines: {e}")
            return []

    def _get_current_user(self) -> str:
        """Get the current Databricks user."""
        try:
            me = self.client.current_user.me()
            return me.user_name or me.display_name or "unknown"
        except Exception:
            return "unknown"

    def _build_certificate_html(self, summary: dict) -> str:
        """Generate HTML deletion certificate."""
        verified_badge = (
            '<span class="badge badge-success">VERIFIED</span>'
            if summary.get("verification_passed")
            else '<span class="badge badge-danger">NOT VERIFIED</span>'
        )

        actions_html = ""
        for a in summary.get("actions", []):
            status_class = "badge-success" if a.get("status") == "completed" else "badge-danger"
            actions_html += f"""
            <tr>
                <td>{a.get('type', '')}</td>
                <td>{a.get('table', '')}</td>
                <td>{a.get('column', '')}</td>
                <td><span class="badge {status_class}">{a.get('status', '')}</span></td>
                <td>{a.get('rows_affected', '')}</td>
                <td>{a.get('executed_at', '')}</td>
            </tr>"""

        return f"""<!DOCTYPE html>
<html>
<head>
<title>RTBF Deletion Certificate — {summary.get('certificate_id', '')[:8]}</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 20px; background: #f5f5f5; color: #333; }}
.container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
h1 {{ color: #1a1a1a; border-bottom: 3px solid #d32f2f; padding-bottom: 10px; }}
h2 {{ color: #c62828; margin-top: 30px; border-bottom: 1px solid #e0e0e0; padding-bottom: 8px; }}
table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
th, td {{ border: 1px solid #ddd; padding: 10px 12px; text-align: left; }}
th {{ background: #f0f0f0; font-weight: 600; }}
.badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
.badge-success {{ background: #d4edda; color: #155724; }}
.badge-danger {{ background: #f8d7da; color: #721c24; }}
.meta {{ color: #666; font-size: 14px; margin-bottom: 20px; }}
.stat {{ font-size: 24px; font-weight: 700; color: #c62828; }}
.stat-label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
.stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 15px 0; }}
.stat-card {{ background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }}
.legal {{ background: #fff3e0; border-left: 4px solid #ff9800; padding: 15px; margin: 15px 0; border-radius: 4px; }}
.footer {{ margin-top: 30px; padding-top: 15px; border-top: 2px solid #e0e0e0; color: #666; font-size: 12px; }}
</style>
</head>
<body>
<div class="container">
<h1>RTBF Deletion Certificate</h1>
<p class="meta">
    Certificate ID: {summary.get('certificate_id', '')} |
    Request ID: {summary.get('request_id', '')} |
    Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
</p>

<div class="legal">
    <strong>Legal Basis:</strong> {summary.get('legal_basis', 'Not specified')}<br>
    <strong>Subject Type:</strong> {summary.get('subject_type', '')}<br>
    <strong>Subject Identifier Hash:</strong> <code>{summary.get('subject_value_hash', '')[:16]}...</code><br>
    <strong>Strategy:</strong> {summary.get('strategy', '')}
</div>

<h2>Summary</h2>
<div class="stats-grid">
    <div class="stat-card"><div class="stat">{summary.get('tables_processed', 0)}</div><div class="stat-label">Tables Processed</div></div>
    <div class="stat-card"><div class="stat">{summary.get('total_rows_deleted', 0)}</div><div class="stat-label">Rows Deleted</div></div>
    <div class="stat-card"><div class="stat">{summary.get('vacuum_summary', {}).get('tables_vacuumed', 0)}</div><div class="stat-label">Tables Vacuumed</div></div>
    <div class="stat-card">{verified_badge}</div>
</div>

<h2>Timeline</h2>
<table>
    <tr><th>Event</th><th>Timestamp</th></tr>
    <tr><td>Request Created</td><td>{summary.get('request_created', '')}</td></tr>
    <tr><td>GDPR Deadline</td><td>{summary.get('deadline', '')}</td></tr>
    <tr><td>Request Completed</td><td>{summary.get('request_completed', '')}</td></tr>
</table>

<h2>Action Log</h2>
<table>
    <tr><th>Type</th><th>Table</th><th>Column</th><th>Status</th><th>Rows Affected</th><th>Executed At</th></tr>
    {actions_html}
</table>

<div class="footer">
    <p>This certificate was generated by Clone-Xs RTBF Module as evidence of data erasure
    in compliance with GDPR Article 17 (Right to Erasure). The subject identifier hash is
    stored for audit purposes — the original value was never persisted in audit tables.</p>
    <p>Generated by Clone-Xs v0.4.0</p>
</div>
</div>
</body>
</html>"""
