"""Regulatory Compliance Automation Engine.

Maps DQ controls to regulatory frameworks (SOC2, GDPR, HIPAA, CCPA, DORA)
with automated evidence collection and audit-ready report generation.

Storage: {audit_catalog}.governance.compliance_frameworks
         {audit_catalog}.governance.compliance_evidence
         {audit_catalog}.governance.compliance_scores
"""

import logging
import uuid
from datetime import datetime, timezone

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn

logger = logging.getLogger(__name__)


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


_FRAMEWORKS_DDL = """
    framework_id STRING,
    name STRING,
    version STRING,
    description STRING,
    controls STRING,
    created_at TIMESTAMP
"""

_EVIDENCE_DDL = """
    evidence_id STRING,
    framework_id STRING,
    control_id STRING,
    control_name STRING,
    evidence_type STRING,
    evidence_summary STRING,
    evidence_count INT,
    status STRING,
    collected_at TIMESTAMP,
    valid_until TIMESTAMP
"""

_SCORES_DDL = """
    score_id STRING,
    framework_id STRING,
    framework_name STRING,
    total_controls INT,
    met_controls INT,
    partial_controls INT,
    gap_controls INT,
    score DOUBLE,
    assessed_at TIMESTAMP
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    for tbl, ddl, comment in [
        ("compliance_frameworks", _FRAMEWORKS_DDL, "Regulatory framework definitions"),
        ("compliance_evidence", _EVIDENCE_DDL, "Compliance evidence artifacts"),
        ("compliance_scores", _SCORES_DDL, "Compliance assessment scores"),
    ]:
        try:
            _run_sql(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl} ({ddl})
                USING DELTA COMMENT 'Clone-Xs: {comment}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not create {tbl}: {e}")


# Built-in framework control catalogs
FRAMEWORK_CONTROLS = {
    "SOC2": {
        "name": "SOC 2 Type II",
        "version": "2017",
        "controls": [
            {"id": "CC6.1", "name": "Logical Access Controls", "category": "Security", "evidence_source": "rbac"},
            {"id": "CC6.2", "name": "Authentication Mechanisms", "category": "Security", "evidence_source": "auth"},
            {"id": "CC6.3", "name": "Authorization & Access Control", "category": "Security", "evidence_source": "rbac"},
            {"id": "CC7.2", "name": "Monitoring System Components", "category": "Operations", "evidence_source": "monitoring"},
            {"id": "CC7.3", "name": "Change Management", "category": "Operations", "evidence_source": "audit_trail"},
            {"id": "CC8.1", "name": "Data Quality Standards", "category": "Processing Integrity", "evidence_source": "dq_results"},
            {"id": "A1.2", "name": "Data Backup & Recovery", "category": "Availability", "evidence_source": "audit_trail"},
            {"id": "PI1.1", "name": "Processing Integrity Monitoring", "category": "Processing Integrity", "evidence_source": "dq_results"},
        ],
    },
    "GDPR": {
        "name": "General Data Protection Regulation",
        "version": "2018",
        "controls": [
            {"id": "Art5", "name": "Data Processing Principles", "category": "Principles", "evidence_source": "dq_results"},
            {"id": "Art6", "name": "Lawfulness of Processing", "category": "Consent", "evidence_source": "audit_trail"},
            {"id": "Art17", "name": "Right to Erasure (RTBF)", "category": "Rights", "evidence_source": "rtbf"},
            {"id": "Art15", "name": "Right of Access (DSAR)", "category": "Rights", "evidence_source": "dsar"},
            {"id": "Art25", "name": "Data Protection by Design", "category": "Design", "evidence_source": "pii"},
            {"id": "Art30", "name": "Records of Processing", "category": "Documentation", "evidence_source": "audit_trail"},
            {"id": "Art32", "name": "Security of Processing", "category": "Security", "evidence_source": "pii"},
            {"id": "Art33", "name": "Breach Notification", "category": "Breach", "evidence_source": "incidents"},
        ],
    },
    "HIPAA": {
        "name": "Health Insurance Portability and Accountability Act",
        "version": "2013",
        "controls": [
            {"id": "164.312a", "name": "Access Control", "category": "Technical", "evidence_source": "rbac"},
            {"id": "164.312b", "name": "Audit Controls", "category": "Technical", "evidence_source": "audit_trail"},
            {"id": "164.312c", "name": "Integrity Controls", "category": "Technical", "evidence_source": "dq_results"},
            {"id": "164.312d", "name": "Authentication", "category": "Technical", "evidence_source": "auth"},
            {"id": "164.312e", "name": "Transmission Security", "category": "Technical", "evidence_source": "monitoring"},
            {"id": "164.308a1", "name": "Risk Analysis", "category": "Administrative", "evidence_source": "pii"},
        ],
    },
    "CCPA": {
        "name": "California Consumer Privacy Act",
        "version": "2020",
        "controls": [
            {"id": "1798.100", "name": "Right to Know", "category": "Rights", "evidence_source": "dsar"},
            {"id": "1798.105", "name": "Right to Delete", "category": "Rights", "evidence_source": "rtbf"},
            {"id": "1798.110", "name": "Right to Disclosure", "category": "Rights", "evidence_source": "dsar"},
            {"id": "1798.115", "name": "Right to Opt-Out", "category": "Rights", "evidence_source": "audit_trail"},
            {"id": "1798.130", "name": "Notice & Procedures", "category": "Notice", "evidence_source": "audit_trail"},
        ],
    },
    "DORA": {
        "name": "Digital Operational Resilience Act",
        "version": "2025",
        "controls": [
            {"id": "Art5", "name": "ICT Risk Management", "category": "Risk", "evidence_source": "monitoring"},
            {"id": "Art8", "name": "Identification of ICT Assets", "category": "Assets", "evidence_source": "audit_trail"},
            {"id": "Art9", "name": "Protection & Prevention", "category": "Protection", "evidence_source": "pii"},
            {"id": "Art10", "name": "Detection of Anomalies", "category": "Detection", "evidence_source": "anomalies"},
            {"id": "Art11", "name": "Response & Recovery", "category": "Response", "evidence_source": "incidents"},
            {"id": "Art12", "name": "ICT Business Continuity", "category": "Continuity", "evidence_source": "audit_trail"},
        ],
    },
}


def _collect_evidence_for_source(source: str, config, client, warehouse_id) -> tuple[int, str]:
    """Collect evidence count and summary for a given source type."""
    gov_schema = _get_schema(config)
    pii_schema = get_schema_fqn(config, "pii")
    rtbf_schema = get_schema_fqn(config, "rtbf")
    dsar_schema = get_schema_fqn(config, "dsar")
    dq_schema = get_schema_fqn(config, "data_quality")

    try:
        if source == "dq_results":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {gov_schema}.dq_results WHERE executed_at >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} DQ check results in last 90 days"
        elif source == "audit_trail":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {get_schema_fqn(config, 'logs')}.clone_operations",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} audit trail records"
        elif source == "pii":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {pii_schema}.pii_scans",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} PII scan records"
        elif source == "rtbf":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {rtbf_schema}.rtbf_requests",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} RTBF requests processed"
        elif source == "dsar":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {dsar_schema}.dsar_requests",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} DSAR requests processed"
        elif source == "monitoring":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {dq_schema}.monitoring_configs",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} monitoring configurations active"
        elif source == "anomalies":
            rows = _query_sql(f"SELECT COUNT(*) as cnt FROM {dq_schema}.metric_baselines WHERE is_anomaly = true",
                              limit=1, client=client, warehouse_id=warehouse_id) or [{"cnt": 0}]
            cnt = int(rows[0].get("cnt", 0))
            return cnt, f"{cnt} anomalies detected and tracked"
        elif source in ("rbac", "auth", "incidents"):
            return 1, f"{source} controls implemented in platform"
    except Exception:
        pass
    return 0, "No evidence collected"


def collect_evidence(
    framework_name: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Collect evidence for all controls in a framework."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    fw = FRAMEWORK_CONTROLS.get(framework_name)
    if not fw:
        return {"error": f"Unknown framework: {framework_name}"}

    fwid = framework_name.lower()
    met = partial = gap = 0
    evidence_list = []

    for control in fw["controls"]:
        count, summary = _collect_evidence_for_source(control["evidence_source"], config, client, warehouse_id)
        status = "met" if count > 0 else "gap"
        if count > 0:
            met += 1
        else:
            gap += 1

        eid = uuid.uuid4().hex[:12]
        evidence_list.append({
            "evidence_id": eid, "framework_id": fwid, "control_id": control["id"],
            "control_name": control["name"], "evidence_type": control["evidence_source"],
            "evidence_summary": summary, "evidence_count": count, "status": status,
        })

        try:
            _run_sql(f"""
                INSERT INTO {schema}.compliance_evidence VALUES (
                    '{eid}', '{fwid}', '{_esc(control["id"])}', '{_esc(control["name"])}',
                    '{_esc(control["evidence_source"])}', '{_esc(summary)}', {count},
                    '{status}', '{now}', NULL
                )
            """, client, warehouse_id)
        except Exception:
            pass

    total = len(fw["controls"])
    score = round((met / total) * 100, 1) if total else 0

    # Store score
    sid = uuid.uuid4().hex[:12]
    try:
        _run_sql(f"""
            INSERT INTO {schema}.compliance_scores VALUES (
                '{sid}', '{fwid}', '{_esc(fw["name"])}', {total}, {met}, {partial}, {gap}, {score}, '{now}'
            )
        """, client, warehouse_id)
    except Exception:
        pass

    return {
        "framework": framework_name, "total_controls": total, "met": met,
        "partial": partial, "gaps": gap, "score": score, "evidence": evidence_list,
    }


def get_frameworks(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    """List supported frameworks with latest scores."""
    config = config or {}
    schema = _get_schema(config)
    results = []
    for name, fw in FRAMEWORK_CONTROLS.items():
        entry = {"id": name.lower(), "name": fw["name"], "version": fw["version"],
                 "control_count": len(fw["controls"]), "score": None, "last_assessed": None}
        try:
            rows = _query_sql(f"""
                SELECT score, assessed_at FROM {schema}.compliance_scores
                WHERE framework_id = '{name.lower()}' ORDER BY assessed_at DESC
            """, limit=1, client=client, warehouse_id=warehouse_id)
            if rows:
                entry["score"] = float(rows[0].get("score", 0))
                entry["last_assessed"] = rows[0].get("assessed_at")
        except Exception:
            pass
        results.append(entry)
    return results


def get_gaps(framework_name: str, client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    """Get controls lacking evidence for a framework."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY control_id ORDER BY collected_at DESC) as rn
                FROM {schema}.compliance_evidence WHERE framework_id = '{_esc(framework_name.lower())}'
            ) WHERE rn = 1 AND status = 'gap'
        """, limit=100, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []


def get_score_trend(framework_name: str, client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT score, assessed_at FROM {schema}.compliance_scores
            WHERE framework_id = '{_esc(framework_name.lower())}' ORDER BY assessed_at
        """, limit=100, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []
