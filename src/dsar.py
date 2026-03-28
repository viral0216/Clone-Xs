"""DSAR (Data Subject Access Request) engine — GDPR Article 15 right of access.

Reuses RTBF's subject discovery to find data, then exports instead of deleting.
Workflow: Submit -> Discover -> Approve -> Export -> Generate Report -> Deliver -> Complete.
"""

import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.dsar_store import DSARStore, STATUS_TRANSITIONS
from src.rtbf import SUBJECT_TYPE_PATTERNS

logger = logging.getLogger(__name__)


class DSARManager:
    """Orchestrates DSAR (Data Subject Access Request) workflows."""

    def __init__(self, client: WorkspaceClient, warehouse_id: str, config: dict | None = None):
        self.client = client
        self.warehouse_id = warehouse_id
        self.config = config or {}

        dsar_config = self.config.get("dsar", {})
        catalog = self.config.get("audit_trail", {}).get("catalog", "clone_audit")

        self.store = DSARStore(client, warehouse_id, catalog, "dsar")
        self.deadline_days = dsar_config.get("deadline_days", 30)
        self.default_format = dsar_config.get("default_export_format", "csv")
        self.export_dir = dsar_config.get("export_output_dir", "reports/dsar")
        self.require_approval = dsar_config.get("require_approval", True)
        self.exclude_schemas = self.config.get("rtbf", {}).get(
            "exclude_schemas", ["information_schema", "default"]
        )

    def init_tables(self) -> None:
        self.store.init_tables()

    def submit_request(
        self, subject_type: str, subject_value: str, requester_email: str,
        requester_name: str, legal_basis: str = "GDPR Article 15 - Right of access",
        export_format: str | None = None, scope_catalogs: list[str] | None = None,
        subject_column: str | None = None, notes: str | None = None,
    ) -> dict:
        request_id = str(uuid.uuid4())
        deadline = datetime.now(timezone.utc) + timedelta(days=self.deadline_days)
        subject_value_hash = hashlib.sha256(subject_value.encode()).hexdigest()

        self.store.save_request(
            request_id=request_id, subject_type=subject_type,
            subject_value_hash=subject_value_hash,
            requester_email=requester_email, requester_name=requester_name,
            legal_basis=legal_basis, export_format=export_format or self.default_format,
            deadline=deadline.strftime("%Y-%m-%d %H:%M:%S"),
            scope_catalogs=scope_catalogs, subject_column=subject_column,
            created_by=self._get_user(), notes=notes,
        )
        return {"request_id": request_id, "status": "received", "deadline": deadline.isoformat()}

    def discover_subject(self, request_id: str, subject_value: str) -> dict:
        """Reuses RTBF discovery logic to find all subject data."""
        self.store.update_request_status(request_id, "discovering")
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"DSAR request {request_id} not found")

        subject_type = request.get("subject_type", "")
        subject_column = request.get("subject_column")
        scope_json = request.get("scope_catalogs", "[]")
        scope_catalogs = json.loads(scope_json) if scope_json else []

        # Import RTBF's discovery helpers
        from src.rtbf import RTBFManager
        rtbf_mgr = RTBFManager(self.client, self.warehouse_id, config=self.config)
        catalogs = rtbf_mgr._get_search_catalogs(scope_catalogs)
        candidates = rtbf_mgr._find_subject_columns(subject_type, catalogs, explicit_column=subject_column)

        hits = []
        total_rows = 0
        for col_info in candidates:
            cat, sch, tbl, col = col_info["catalog"], col_info["schema"], col_info["table"], col_info["column"]
            fqn = f"`{cat}`.`{sch}`.`{tbl}`"
            try:
                rows = execute_sql(self.client, self.warehouse_id,
                                   f"SELECT COUNT(*) AS cnt FROM {fqn} WHERE `{col}` = '{subject_value}'")
                count = int(rows[0]["cnt"]) if rows else 0
                if count > 0:
                    hits.append({"catalog": cat, "schema": sch, "table": tbl, "column": col, "row_count": count})
                    total_rows += count
                    self.store.save_action(
                        action_id=str(uuid.uuid4()), request_id=request_id, action_type="discover",
                        catalog=cat, schema_name=sch, table_name=tbl, column_name=col, rows_found=count,
                    )
            except Exception as e:
                logger.warning(f"DSAR discovery failed for {fqn}.{col}: {e}")

        self.store.update_request_status(
            request_id, "analyzed", discovery_json=json.dumps(hits),
            affected_tables=len(hits), affected_rows=total_rows,
        )
        return {"request_id": request_id, "affected_tables": hits, "total_rows": total_rows}

    def approve_request(self, request_id: str) -> dict:
        return self._transition(request_id, "approved")

    def cancel_request(self, request_id: str) -> dict:
        return self._transition(request_id, "cancelled")

    def export_data(self, request_id: str, subject_value: str, export_format: str | None = None) -> dict:
        """Export all subject data to files."""
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"DSAR request {request_id} not found")

        fmt = export_format or request.get("export_format", self.default_format)
        discovery_json = request.get("discovery_json", "[]")
        hits = json.loads(discovery_json) if discovery_json else []
        if not hits:
            return {"request_id": request_id, "message": "No data found to export"}

        self.store.update_request_status(request_id, "exporting")
        os.makedirs(self.export_dir, exist_ok=True)

        export_id = str(uuid.uuid4())
        all_data = []
        total_rows = 0

        for hit in hits:
            fqn = f"`{hit['catalog']}`.`{hit['schema']}`.`{hit['table']}`"
            col = hit["column"]
            try:
                rows = execute_sql(self.client, self.warehouse_id,
                                   f"SELECT * FROM {fqn} WHERE `{col}` = '{subject_value}' LIMIT 10000")
                if rows:
                    all_data.append({
                        "source": f"{hit['catalog']}.{hit['schema']}.{hit['table']}",
                        "column": col, "row_count": len(rows), "data": rows,
                    })
                    total_rows += len(rows)
            except Exception as e:
                logger.warning(f"DSAR export failed for {fqn}: {e}")

        # Write export file
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        if fmt == "json":
            path = os.path.join(self.export_dir, f"dsar_export_{request_id[:8]}_{ts}.json")
            with open(path, "w") as f:
                json.dump({"request_id": request_id, "tables": all_data, "total_rows": total_rows}, f, indent=2, default=str)
        else:
            import csv
            path = os.path.join(self.export_dir, f"dsar_export_{request_id[:8]}_{ts}.csv")
            with open(path, "w", newline="") as f:
                writer = None
                for tbl in all_data:
                    for row in tbl.get("data", []):
                        if writer is None:
                            row_with_source = {"_source_table": tbl["source"], **row}
                            writer = csv.DictWriter(f, fieldnames=row_with_source.keys())
                            writer.writeheader()
                        writer.writerow({"_source_table": tbl["source"], **row})

        file_size = os.path.getsize(path) if os.path.exists(path) else 0
        self.store.save_export(
            export_id=export_id, request_id=request_id, format=fmt,
            file_path=path, file_size_bytes=file_size,
            total_rows=total_rows, total_tables=len(all_data),
            generated_by=self._get_user(),
        )
        self.store.update_request_status(request_id, "exported")
        return {"request_id": request_id, "export_id": export_id, "path": path,
                "format": fmt, "total_rows": total_rows, "total_tables": len(all_data)}

    def generate_report(self, request_id: str) -> dict:
        """Generate an access report (HTML + JSON) summarizing what data was found."""
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"DSAR request {request_id} not found")

        actions = self.store.get_actions(request_id)
        exports = self.store.get_exports(request_id)

        report = {
            "request_id": request_id, "subject_type": request.get("subject_type"),
            "subject_value_hash": request.get("subject_value_hash"),
            "legal_basis": request.get("legal_basis"),
            "tables_found": request.get("affected_tables", 0),
            "rows_found": request.get("affected_rows", 0),
            "exports": [{"format": e.get("format"), "path": e.get("file_path"),
                          "rows": e.get("total_rows"), "generated_at": str(e.get("generated_at"))} for e in exports],
            "actions": [{"type": a.get("action_type"), "table": f"{a.get('catalog')}.{a.get('schema_name')}.{a.get('table_name')}",
                          "rows": a.get("rows_found"), "status": a.get("status")} for a in actions],
        }

        os.makedirs(self.export_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join(self.export_dir, f"dsar_report_{request_id[:8]}_{ts}.json")
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        return {"request_id": request_id, "report_path": json_path, "report": report}

    def deliver_report(self, request_id: str) -> dict:
        self.store.update_request_status(request_id, "delivered")
        return {"request_id": request_id, "status": "delivered"}

    def complete_request(self, request_id: str) -> dict:
        self.store.update_request_status(request_id, "completed")
        return {"request_id": request_id, "status": "completed"}

    def get_request(self, request_id: str) -> dict | None:
        return self.store.get_request(request_id)

    def list_requests(self, **kwargs) -> list[dict]:
        return self.store.list_requests(**kwargs)

    def get_overdue_requests(self) -> list[dict]:
        return self.store.get_overdue_requests()

    def get_dashboard(self) -> dict:
        stats = self.store.get_dashboard_stats()
        recent = self.store.list_requests(limit=10)
        overdue = self.store.get_overdue_requests()
        return {"stats": stats, "recent_requests": recent, "overdue_requests": overdue}

    def _transition(self, request_id: str, new_status: str) -> dict:
        request = self.store.get_request(request_id)
        if not request:
            raise ValueError(f"DSAR request {request_id} not found")
        current = request.get("status", "")
        allowed = STATUS_TRANSITIONS.get(current, [])
        if new_status not in allowed:
            raise ValueError(f"Cannot transition from '{current}' to '{new_status}'")
        self.store.update_request_status(request_id, new_status)
        return {"request_id": request_id, "status": new_status, "previous_status": current}

    def _get_user(self) -> str:
        try:
            me = self.client.current_user.me()
            return me.user_name or me.display_name or "unknown"
        except Exception:
            return "unknown"
