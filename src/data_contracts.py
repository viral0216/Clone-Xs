"""ODCS Data Contracts Engine — CRUD, YAML import/export, validation, DQX integration.

Implements the Open Data Contract Standard v3.1.0.  Contracts are stored as full
JSON documents in Delta tables with indexed metadata columns for efficient querying.

Integrates with:
- Existing DQ rules engine (src/dq_rules.py) for quality rule execution
- Existing SLA monitor (src/sla_monitor.py) for freshness / SLA checks
- DQX (databricks-labs-dqx) for advanced DataFrame-level quality checks

Spec: https://bitol-io.github.io/open-data-contract-standard/v3.1.0/
"""

import json
import logging
import re
import uuid
from datetime import datetime, timezone


from src.client import execute_sql

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _get_schema(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.governance"


def _esc(s) -> str:
    if not s:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _parse_val(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v) if not isinstance(v, (int, float)) else v


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def ensure_odcs_tables(client, warehouse_id, config):
    """Create ODCS Delta tables if they don't exist."""
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass

    tables = {
        "odcs_contracts": """
            contract_id STRING,
            name STRING,
            version STRING,
            status STRING,
            domain STRING,
            data_product STRING,
            tenant STRING,
            table_fqns STRING,
            odcs_document STRING,
            created_by STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        """,
        "odcs_contract_versions": """
            contract_id STRING,
            version STRING,
            odcs_document STRING,
            created_by STRING,
            created_at TIMESTAMP
        """,
        "odcs_validation_results": """
            validation_id STRING,
            contract_id STRING,
            contract_version STRING,
            sections_checked STRING,
            violations STRING,
            compliant BOOLEAN,
            validated_at TIMESTAMP,
            validated_by STRING
        """,
    }

    for table_name, cols in tables.items():
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs ODCS: {table_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{table_name}: {e}")


# ---------------------------------------------------------------------------
# Helpers — extract metadata from ODCS document
# ---------------------------------------------------------------------------

def _extract_table_fqns(doc: dict) -> list[str]:
    """Extract table FQNs from schema objects and servers."""
    fqns = []
    servers = doc.get("servers", [])
    schema_objects = doc.get("schema", [])

    for obj in schema_objects:
        physical = obj.get("physicalName", obj.get("name", ""))
        # If physical name already has dots (catalog.schema.table), use as-is
        if physical.count(".") >= 2:
            fqns.append(physical)
        elif servers:
            # Try to build FQN from first Databricks server
            for srv in servers:
                if srv.get("type", "").lower() == "databricks":
                    cat = srv.get("catalog", "")
                    sch = srv.get("schema", "")
                    if cat and sch:
                        fqns.append(f"{cat}.{sch}.{physical}")
                        break
        if physical and physical not in fqns:
            fqns.append(physical)
    return fqns


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def create_odcs_contract(client, warehouse_id, config, contract_data: dict, user: str = "") -> dict:
    """Create a new ODCS contract."""
    from api.models.odcs import ODCSContract

    # Reject empty/missing name
    name = (contract_data.get("name") or "").strip()
    if not name:
        return {"error": "Contract name is required"}

    # Must have at least one schema object or a meaningful description
    schema_objects = contract_data.get("schema", [])
    if not schema_objects and not contract_data.get("description", {}).get("purpose"):
        return {"error": "Contract must have at least one schema object or a description"}

    # Build and validate via Pydantic
    if not contract_data.get("id"):
        contract_data["id"] = str(uuid.uuid4())
    contract = ODCSContract.model_validate(contract_data)
    doc = contract.to_dict()
    contract_id = contract.id
    now = _now_iso()

    schema = _get_schema(config)
    table_fqns = json.dumps(_extract_table_fqns(doc))
    doc_json = json.dumps(doc)

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.odcs_contracts
        VALUES ('{_esc(contract_id)}', '{_esc(contract.name)}', '{_esc(contract.version)}',
                '{_esc(contract.status)}', '{_esc(contract.domain)}',
                '{_esc(contract.dataProduct)}', '{_esc(contract.tenant)}',
                '{_esc(table_fqns)}', '{_esc(doc_json)}',
                '{_esc(user)}', '{now}', '{now}')
    """)

    # Track change
    _track_change(client, warehouse_id, config, "odcs_contract", contract_id, "created",
                  {"name": contract.name, "version": contract.version}, user)

    return {"contract_id": contract_id, "name": contract.name, "version": contract.version, "status": "created"}


def get_odcs_contract(client, warehouse_id, config, contract_id: str) -> dict | None:
    """Get a single ODCS contract with full document."""
    schema = _get_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.odcs_contracts WHERE contract_id = '{_esc(contract_id)}'")
        if not rows:
            return None
        row = rows[0]
        doc = json.loads(row.get("odcs_document", "{}"))
        doc["_meta"] = {
            "created_by": row.get("created_by", ""),
            "created_at": str(row.get("created_at", "")),
            "updated_at": str(row.get("updated_at", "")),
        }
        return doc
    except Exception as e:
        logger.warning(f"Could not get contract {contract_id}: {e}")
        return None


def list_odcs_contracts(client, warehouse_id, config, domain: str = "", status: str = "", table_fqn: str = "") -> list[dict]:
    """List ODCS contracts with optional filters."""
    schema = _get_schema(config)
    where_parts = []
    if domain:
        where_parts.append(f"domain = '{_esc(domain)}'")
    if status:
        where_parts.append(f"status = '{_esc(status)}'")
    if table_fqn:
        where_parts.append(f"table_fqns LIKE '%{_esc(table_fqn)}%'")
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT contract_id, name, version, status, domain, data_product, tenant,
                   table_fqns, created_by, created_at, updated_at
            FROM {schema}.odcs_contracts {where}
            ORDER BY name
        """)
        results = []
        for r in rows:
            item = {k: _parse_val(v) for k, v in r.items()}
            try:
                item["table_fqns"] = json.loads(item.get("table_fqns", "[]"))
            except Exception:
                item["table_fqns"] = []
            results.append(item)
        return results
    except Exception:
        return []


def update_odcs_contract(client, warehouse_id, config, contract_id: str, updates: dict, user: str = "") -> dict:
    """Update an ODCS contract. Archives previous version if version changes."""
    from api.models.odcs import ODCSContract

    schema = _get_schema(config)
    existing = get_odcs_contract(client, warehouse_id, config, contract_id)
    if not existing:
        return {"error": "Contract not found"}

    # Remove _meta before merging
    existing.pop("_meta", None)
    old_version = existing.get("version", "")

    # Merge updates into existing document
    merged = _deep_merge(existing, updates)
    contract = ODCSContract.model_validate(merged)
    doc = contract.to_dict()
    now = _now_iso()

    # Archive previous version if version changed
    new_version = contract.version
    if old_version and new_version != old_version:
        old_doc_json = json.dumps(existing)
        try:
            execute_sql(client, warehouse_id, f"""
                INSERT INTO {schema}.odcs_contract_versions
                VALUES ('{_esc(contract_id)}', '{_esc(old_version)}',
                        '{_esc(old_doc_json)}', '{_esc(user)}', '{now}')
            """)
        except Exception as e:
            logger.warning(f"Could not archive version: {e}")

    # Update main record
    table_fqns = json.dumps(_extract_table_fqns(doc))
    doc_json = json.dumps(doc)
    execute_sql(client, warehouse_id, f"""
        UPDATE {schema}.odcs_contracts
        SET name = '{_esc(contract.name)}', version = '{_esc(new_version)}',
            status = '{_esc(contract.status)}', domain = '{_esc(contract.domain)}',
            data_product = '{_esc(contract.dataProduct)}', tenant = '{_esc(contract.tenant)}',
            table_fqns = '{_esc(table_fqns)}', odcs_document = '{_esc(doc_json)}',
            updated_at = '{now}'
        WHERE contract_id = '{_esc(contract_id)}'
    """)

    _track_change(client, warehouse_id, config, "odcs_contract", contract_id, "updated",
                  {"version": new_version, "changed_fields": list(updates.keys())}, user)

    return {"contract_id": contract_id, "version": new_version, "status": "updated"}


def delete_odcs_contract(client, warehouse_id, config, contract_id: str, user: str = ""):
    """Delete an ODCS contract."""
    schema = _get_schema(config)
    execute_sql(client, warehouse_id,
        f"DELETE FROM {schema}.odcs_contracts WHERE contract_id = '{_esc(contract_id)}'")
    _track_change(client, warehouse_id, config, "odcs_contract", contract_id, "deleted", {}, user)


# ---------------------------------------------------------------------------
# YAML Import / Export
# ---------------------------------------------------------------------------

def import_odcs_yaml(client, warehouse_id, config, yaml_content: str, user: str = "") -> dict:
    """Parse ODCS YAML and store as a new contract."""
    from api.models.odcs import ODCSContract

    contract = ODCSContract.from_yaml(yaml_content)
    doc = contract.to_dict()
    return create_odcs_contract(client, warehouse_id, config, doc, user)


def export_odcs_yaml(client, warehouse_id, config, contract_id: str) -> str:
    """Export an ODCS contract as YAML."""
    from api.models.odcs import ODCSContract

    doc = get_odcs_contract(client, warehouse_id, config, contract_id)
    if not doc:
        return ""
    doc.pop("_meta", None)
    contract = ODCSContract.model_validate(doc)
    return contract.to_yaml()


# ---------------------------------------------------------------------------
# Version History
# ---------------------------------------------------------------------------

def get_contract_versions(client, warehouse_id, config, contract_id: str) -> list[dict]:
    """Get version history for a contract."""
    schema = _get_schema(config)
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT contract_id, version, created_by, created_at
            FROM {schema}.odcs_contract_versions
            WHERE contract_id = '{_esc(contract_id)}'
            ORDER BY created_at DESC
        """)
        return [{k: _parse_val(v) for k, v in r.items()} for r in rows]
    except Exception:
        return []


def get_contract_version(client, warehouse_id, config, contract_id: str, version: str) -> dict | None:
    """Get a specific version of a contract."""
    schema = _get_schema(config)
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT odcs_document FROM {schema}.odcs_contract_versions
            WHERE contract_id = '{_esc(contract_id)}' AND version = '{_esc(version)}'
        """)
        if rows:
            return json.loads(rows[0].get("odcs_document", "{}"))
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Pre-fill from config
# ---------------------------------------------------------------------------

def prefill_from_config(config: dict) -> dict:
    """Generate a partial ODCS server block from clone_config.yaml."""
    server = {
        "server": "databricks-workspace",
        "type": "databricks",
        "environment": config.get("environment", "dev"),
    }
    audit = config.get("audit_trail", {})
    if audit.get("catalog"):
        server["catalog"] = audit["catalog"]
    if audit.get("schema"):
        server["schema"] = audit["schema"]
    wid = config.get("sql_warehouse_id", "")
    if wid:
        server["customProperties"] = [{"property": "sql_warehouse_id", "value": wid}]
    return server


# ---------------------------------------------------------------------------
# DQ / SLA Mapping — bridge existing engines to ODCS format
# ---------------------------------------------------------------------------

def map_dq_rules_to_odcs(client, warehouse_id, config, contract_id: str) -> list[dict]:
    """Read existing DQ rules for the contract's tables and convert to ODCS quality format."""
    from src.dq_rules import list_rules

    doc = get_odcs_contract(client, warehouse_id, config, contract_id)
    if not doc:
        return []

    table_fqns = _extract_table_fqns(doc)
    odcs_rules = []

    # DQ rule_type → ODCS metric mapping
    type_map = {
        "not_null": ("nullValues", "completeness"),
        "unique": ("duplicateValues", "uniqueness"),
        "range": ("invalidValues", "conformity"),
        "regex": ("invalidValues", "conformity"),
        "row_count": ("rowCount", "completeness"),
        "freshness": ("timeliness", "timeliness"),
        "referential": ("invalidValues", "consistency"),
        "custom_sql": ("", ""),
    }

    for fqn in table_fqns:
        rules = list_rules(client, warehouse_id, config, table_fqn=fqn)
        for rule in rules:
            rt = rule.get("rule_type", "")
            metric, dimension = type_map.get(rt, ("", ""))
            odcs_rule = {
                "id": rule.get("rule_id", ""),
                "name": rule.get("name", ""),
                "type": "sql" if rt == "custom_sql" else "library",
                "metric": metric,
                "dimension": dimension,
                "severity": rule.get("severity", "warning"),
                "customProperties": [{"property": "dq_rule_id", "value": rule.get("rule_id", "")}],
            }
            # Map threshold to comparison operator
            float(rule.get("threshold", 0))
            if metric == "rowCount":
                try:
                    params = json.loads(rule.get("params", "{}")) if isinstance(rule.get("params"), str) else rule.get("params", {})
                    min_val = params.get("min", 0)
                    if min_val:
                        odcs_rule["mustBeGreaterOrEqualTo"] = min_val
                except Exception:
                    pass
            elif metric == "nullValues":
                odcs_rule["mustBe"] = 0
            elif metric == "duplicateValues":
                odcs_rule["mustBe"] = 0

            if rt == "custom_sql":
                odcs_rule["query"] = rule.get("expression", "")

            odcs_rules.append(odcs_rule)

    return odcs_rules


def map_sla_rules_to_odcs(client, warehouse_id, config, contract_id: str) -> list[dict]:
    """Read existing SLA rules for the contract's tables and convert to ODCS slaProperties."""
    from src.sla_monitor import list_sla_rules

    doc = get_odcs_contract(client, warehouse_id, config, contract_id)
    if not doc:
        return []

    table_fqns = set(_extract_table_fqns(doc))
    all_rules = list_sla_rules(client, warehouse_id, config)
    odcs_sla = []

    metric_map = {
        "freshness": ("latency", "h"),
        "row_count": ("completeness", "rows"),
        "schema_stability": ("integrity", ""),
    }

    for rule in all_rules:
        if rule.get("table_fqn") not in table_fqns:
            continue
        metric = rule.get("metric", "")
        prop, unit = metric_map.get(metric, (metric, ""))

        sla_prop = {
            "property": prop,
            "value": rule.get("threshold_hours") or rule.get("threshold_value", 0),
            "unit": unit,
            "element": rule.get("table_fqn", ""),
            "driver": "operational",
        }
        if rule.get("owner_team"):
            sla_prop["description"] = f"Owned by {rule['owner_team']}"
        odcs_sla.append(sla_prop)

    return odcs_sla


# ---------------------------------------------------------------------------
# DQX Integration
# ---------------------------------------------------------------------------

def _generate_dqx_checks(doc: dict) -> list[dict]:
    """Convert ODCS quality rules to DQX check format.

    Returns a list of DQX-compatible check dictionaries that can be used
    with DQEngine.apply_checks_by_metadata().
    """
    checks = []

    # Mapping from ODCS library metrics to DQX check functions
    odcs_to_dqx = {
        "nullValues": "is_not_null",
        "missingValues": "is_not_null_and_not_empty",
        "duplicateValues": "is_unique",
        "invalidValues": "regex_match",  # or is_in_list depending on arguments
        "rowCount": None,  # dataset-level, handled separately
    }

    def _process_quality_rules(rules: list[dict], column: str = "", schema_obj_name: str = ""):
        for rule in rules:
            rule_type = rule.get("type", "library")

            # If DQX-specific fields are set, use them directly
            if rule.get("dqx_function"):
                check = {
                    "criticality": rule.get("dqx_criticality", "error"),
                    "check": {
                        "function": rule["dqx_function"],
                        "arguments": rule.get("dqx_arguments", {}),
                    },
                }
                if column:
                    check["check"]["arguments"].setdefault("column", column)
                checks.append(check)
                continue

            if rule_type == "library":
                metric = rule.get("metric", "")
                dqx_func = odcs_to_dqx.get(metric)
                if not dqx_func:
                    continue

                args = rule.get("arguments", {}) or {}
                check = {
                    "criticality": "error" if rule.get("severity") in ("critical", "error") else "warn",
                    "check": {
                        "function": dqx_func,
                        "arguments": {},
                    },
                }

                if column:
                    check["check"]["arguments"]["column"] = column

                # Map specific metrics to DQX arguments
                if metric == "invalidValues":
                    if "validValues" in args:
                        check["check"]["function"] = "is_in_list"
                        check["check"]["arguments"]["allowed"] = args["validValues"]
                    elif "pattern" in args:
                        check["check"]["function"] = "regex_match"
                        check["check"]["arguments"]["pattern"] = args["pattern"]

                checks.append(check)

            elif rule_type == "sql":
                query = rule.get("query", "")
                if query:
                    checks.append({
                        "criticality": "error" if rule.get("severity") in ("critical", "error") else "warn",
                        "check": {
                            "function": "sql_expression",
                            "arguments": {"expression": query},
                        },
                    })

    # Process contract-level quality rules
    for rule in doc.get("quality", []):
        _process_quality_rules([rule])

    # Process schema-object and property-level rules
    for schema_obj in doc.get("schema", []):
        _process_quality_rules(schema_obj.get("quality", []), schema_obj_name=schema_obj.get("name", ""))
        for prop in schema_obj.get("properties", []):
            col_name = prop.get("physicalName") or prop.get("name", "")
            _process_quality_rules(prop.get("quality", []), column=col_name, schema_obj_name=schema_obj.get("name", ""))

    return checks


def run_dqx_validation(client, warehouse_id, config, contract_id: str) -> dict:
    """Run DQX-based validation for an ODCS contract.

    Requires databricks-labs-dqx to be installed.
    Returns check results or an error if DQX is not available.
    """
    doc = get_odcs_contract(client, warehouse_id, config, contract_id)
    if not doc:
        return {"error": "Contract not found"}

    doc.pop("_meta", None)
    checks = _generate_dqx_checks(doc)
    if not checks:
        return {"status": "no_dqx_checks", "message": "No DQX-compatible quality rules found"}

    try:
        from databricks.sdk import WorkspaceClient as _WC  # noqa: F401
        from databricks.labs.dqx.engine import DQEngine
    except ImportError:
        return {"status": "dqx_not_installed", "message": "databricks-labs-dqx is not installed. Install with: pip install databricks-labs-dqx"}

    try:
        ws = client  # authenticated WorkspaceClient from API
        from src.spark_session import get_spark
        spark = get_spark()
        dq_engine = DQEngine(ws, spark=spark)

        results = []
        table_fqns = _extract_table_fqns(doc)

        for fqn in table_fqns:
            try:
                # Read the table as a DataFrame
                from src.spark_session import get_spark
                spark = get_spark()
                df = spark.table(fqn)

                # Apply DQX checks
                valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
                    df=df, checks=checks
                )
                valid_count = valid_df.count()
                invalid_count = invalid_df.count()
                total = valid_count + invalid_count

                results.append({
                    "table_fqn": fqn,
                    "total_rows": total,
                    "valid_rows": valid_count,
                    "invalid_rows": invalid_count,
                    "pass_rate": round(valid_count / max(total, 1) * 100, 2),
                    "checks_applied": len(checks),
                })
            except Exception as e:
                results.append({
                    "table_fqn": fqn,
                    "error": str(e),
                })

        return {"status": "completed", "results": results, "checks_count": len(checks)}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# Comprehensive Validation Engine
# ---------------------------------------------------------------------------

def validate_odcs_contract(client, warehouse_id, config, contract_id: str, user: str = "") -> dict:
    """Validate an ODCS contract against all 11 sections.

    Checks:
    - Fundamentals: required fields, semver, status enum
    - Schema: column existence, type matching, PK/unique/required
    - Quality: execute library metrics and SQL rules
    - SLA: freshness, availability
    - Servers: catalog/schema existence
    - Team/Roles/Support/Pricing: structural validation
    """
    doc = get_odcs_contract(client, warehouse_id, config, contract_id)
    if not doc:
        return {"error": "Contract not found"}
    doc.pop("_meta", None)

    sections = {}

    # 1. Fundamentals
    sections["fundamentals"] = _validate_fundamentals(doc)

    # 2. Schema
    sections["schema"] = _validate_schema(client, warehouse_id, doc)

    # 3. Quality
    sections["quality"] = _validate_quality(client, warehouse_id, config, doc)

    # 4. SLA
    sections["sla"] = _validate_sla(client, warehouse_id, doc)

    # 5. Servers
    sections["servers"] = _validate_servers(client, warehouse_id, doc)

    # 6. Team
    sections["team"] = _validate_team(doc)

    # 7. Roles
    sections["roles"] = _validate_roles(doc)

    # 8. Support
    sections["support"] = _validate_support(doc)

    # 9. Pricing
    sections["pricing"] = _validate_pricing(doc)

    # 10. References
    sections["references"] = _validate_references(doc)

    # 11. Custom Properties
    sections["customProperties"] = {"passed": True, "violations": []}

    # DQX validation (optional)
    dqx_result = run_dqx_validation(client, warehouse_id, config, contract_id)
    if dqx_result.get("status") == "completed":
        sections["dqx"] = {
            "passed": all(r.get("pass_rate", 0) >= 95 for r in dqx_result.get("results", []) if "error" not in r),
            "violations": [r for r in dqx_result.get("results", []) if r.get("pass_rate", 100) < 95 or "error" in r],
            "results": dqx_result.get("results", []),
        }

    total_violations = sum(len(s.get("violations", [])) for s in sections.values())
    compliant = total_violations == 0

    result = {
        "contract_id": contract_id,
        "version": doc.get("version", ""),
        "compliant": compliant,
        "sections": sections,
        "total_violations": total_violations,
        "validated_at": _now_iso(),
    }

    # Store validation result
    _store_validation_result(client, warehouse_id, config, result, user)

    return result


def _validate_fundamentals(doc: dict) -> dict:
    violations = []
    required = ["apiVersion", "kind", "id", "version", "status"]
    for field in required:
        if not doc.get(field):
            violations.append({"field": field, "type": "missing_required", "message": f"Required field '{field}' is missing"})

    if doc.get("version"):
        semver_re = re.compile(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?(\+[a-zA-Z0-9.]+)?$")
        if not semver_re.match(doc["version"]):
            violations.append({"field": "version", "type": "invalid_format", "message": f"Version '{doc['version']}' is not valid semver"})

    valid_statuses = {"proposed", "draft", "active", "deprecated", "retired"}
    if doc.get("status") and doc["status"] not in valid_statuses:
        violations.append({"field": "status", "type": "invalid_value", "message": f"Status '{doc['status']}' not in {valid_statuses}"})

    if doc.get("kind") and doc["kind"] != "DataContract":
        violations.append({"field": "kind", "type": "invalid_value", "message": "kind must be 'DataContract'"})

    return {"passed": len(violations) == 0, "violations": violations}


def _validate_schema(client, warehouse_id, doc: dict) -> dict:
    violations = []
    schema_objects = doc.get("schema", [])
    if not schema_objects:
        return {"passed": True, "violations": [], "message": "No schema objects defined"}

    servers = doc.get("servers", [])

    for obj in schema_objects:
        physical_name = obj.get("physicalName", obj.get("name", ""))
        if not physical_name:
            violations.append({"object": obj.get("name", "?"), "type": "missing_name", "message": "Schema object has no name or physicalName"})
            continue

        # Build the FQN
        fqn = physical_name
        if fqn.count(".") < 2:
            for srv in servers:
                if srv.get("type", "").lower() == "databricks":
                    cat = srv.get("catalog", "")
                    sch = srv.get("schema", "")
                    if cat and sch:
                        fqn = f"{cat}.{sch}.{physical_name}"
                        break

        if fqn.count(".") < 2:
            violations.append({"object": physical_name, "type": "unresolvable_fqn", "message": f"Cannot resolve FQN for '{physical_name}'. Add a Databricks server with catalog/schema or use fully qualified names."})
            continue

        # Describe actual table columns
        try:
            actual_cols = execute_sql(client, warehouse_id, f"DESCRIBE TABLE {fqn}")
            actual_map = {}
            for c in actual_cols:
                col_name = c.get("col_name", "")
                if col_name and not col_name.startswith("#"):
                    actual_map[col_name.lower()] = c.get("data_type", "")
        except Exception as e:
            violations.append({"object": fqn, "type": "table_not_found", "message": f"Cannot describe table: {e}"})
            continue

        # Check declared properties against actual columns
        for prop in obj.get("properties", []):
            col_name = (prop.get("physicalName") or prop.get("name", "")).lower()
            if not col_name:
                continue

            if col_name not in actual_map:
                violations.append({
                    "object": fqn, "column": col_name, "type": "missing_column",
                    "message": f"Expected column '{col_name}' not found in table",
                })
            else:
                # Type comparison (basic)
                declared_type = (prop.get("physicalType") or "").lower().strip()
                actual_type = actual_map[col_name].lower().strip()
                if declared_type and actual_type and not _types_compatible(declared_type, actual_type):
                    violations.append({
                        "object": fqn, "column": col_name, "type": "type_mismatch",
                        "message": f"Type mismatch: declared '{declared_type}', actual '{actual_type}'",
                    })

        # Check required columns are NOT NULL (via a sample query)
        required_cols = [p for p in obj.get("properties", []) if p.get("required")]
        for prop in required_cols[:10]:  # Limit checks
            col_name = prop.get("physicalName") or prop.get("name", "")
            if col_name.lower() in actual_map:
                try:
                    rows = execute_sql(client, warehouse_id,
                        f"SELECT count(*) AS nulls FROM {fqn} WHERE {col_name} IS NULL LIMIT 1")
                    nulls = int(rows[0]["nulls"]) if rows else 0
                    if nulls > 0:
                        violations.append({
                            "object": fqn, "column": col_name, "type": "required_violation",
                            "message": f"Required column '{col_name}' has {nulls} NULL values",
                        })
                except Exception:
                    pass

        # Check unique columns
        unique_cols = [p for p in obj.get("properties", []) if p.get("unique")]
        for prop in unique_cols[:10]:
            col_name = prop.get("physicalName") or prop.get("name", "")
            if col_name.lower() in actual_map:
                try:
                    rows = execute_sql(client, warehouse_id,
                        f"SELECT count(*) - count(DISTINCT {col_name}) AS dups FROM {fqn}")
                    dups = int(rows[0]["dups"]) if rows else 0
                    if dups > 0:
                        violations.append({
                            "object": fqn, "column": col_name, "type": "uniqueness_violation",
                            "message": f"Unique column '{col_name}' has {dups} duplicate values",
                        })
                except Exception:
                    pass

    return {"passed": len(violations) == 0, "violations": violations}


def _validate_quality(client, warehouse_id, config, doc: dict) -> dict:
    """Validate quality rules by executing them."""
    from src.dq_rules import _execute_single_rule

    violations = []
    all_rules = []

    # Collect all quality rules from all levels
    all_rules.extend(doc.get("quality", []))
    for obj in doc.get("schema", []):
        for rule in obj.get("quality", []):
            rule["_table"] = obj.get("physicalName", obj.get("name", ""))
            all_rules.append(rule)
        for prop in obj.get("properties", []):
            for rule in prop.get("quality", []):
                rule["_table"] = obj.get("physicalName", obj.get("name", ""))
                rule["_column"] = prop.get("physicalName") or prop.get("name", "")
                all_rules.append(rule)

    # Resolve table FQNs
    servers = doc.get("servers", [])

    for rule in all_rules[:50]:  # Cap at 50 rules per validation
        rule_type = rule.get("type", "library")
        metric = rule.get("metric", "")

        if rule_type == "text":
            continue  # Text rules are informational

        # Resolve table FQN
        table = rule.get("_table", "")
        if table and table.count(".") < 2:
            for srv in servers:
                if srv.get("type", "").lower() == "databricks":
                    cat = srv.get("catalog", "")
                    sch = srv.get("schema", "")
                    if cat and sch:
                        table = f"{cat}.{sch}.{table}"
                        break

        if not table or table.count(".") < 2:
            continue  # Skip if we can't resolve the table

        if rule_type == "library" and metric:
            # Map ODCS metric to DQ engine rule format
            dq_rule = _odcs_metric_to_dq_rule(rule, table)
            if dq_rule:
                try:
                    result = _execute_single_rule(client, warehouse_id, dq_rule)
                    if not result.get("passed"):
                        violations.append({
                            "rule": rule.get("name", metric),
                            "metric": metric,
                            "type": "quality_check_failed",
                            "table": table,
                            "column": rule.get("_column", ""),
                            "details": result,
                        })
                except Exception as e:
                    violations.append({
                        "rule": rule.get("name", metric),
                        "type": "quality_check_error",
                        "message": str(e),
                    })

        elif rule_type == "sql" and rule.get("query"):
            query = rule["query"].replace("{object}", table)
            if rule.get("_column"):
                query = query.replace("{property}", rule["_column"])
            try:
                rows = execute_sql(client, warehouse_id, query)
                if rows:
                    val = _get_numeric_result(rows[0])
                    if not _check_comparison(rule, val):
                        violations.append({
                            "rule": rule.get("name", "sql_check"),
                            "type": "sql_check_failed",
                            "table": table,
                            "value": val,
                            "query": query[:200],
                        })
            except Exception as e:
                violations.append({
                    "rule": rule.get("name", "sql_check"),
                    "type": "sql_check_error",
                    "message": str(e),
                })

    return {"passed": len(violations) == 0, "violations": violations}


def _validate_sla(client, warehouse_id, doc: dict) -> dict:
    violations = []
    sla_props = doc.get("slaProperties", [])

    for sla in sla_props:
        prop = sla.get("property", "")
        element = sla.get("element", "")

        if prop in ("latency", "frequency", "ly", "fy"):
            # Check freshness for the target table
            table_fqn = element
            if not table_fqn or table_fqn.count(".") < 2:
                continue
            try:
                detail = execute_sql(client, warehouse_id, f"DESCRIBE DETAIL {table_fqn}")
                if detail and detail[0].get("lastModified"):
                    hours_rows = execute_sql(client, warehouse_id,
                        f"SELECT datediff(hour, timestamp '{detail[0]['lastModified']}', current_timestamp()) AS hours")
                    hours = int(hours_rows[0]["hours"]) if hours_rows else 9999

                    # Parse threshold
                    threshold = sla.get("value", 0)
                    unit = sla.get("unit", "d").lower()
                    if unit in ("d", "day", "days"):
                        threshold_hours = float(threshold) * 24
                    elif unit in ("h", "hour", "hours"):
                        threshold_hours = float(threshold)
                    else:
                        threshold_hours = float(threshold) * 24

                    if hours > threshold_hours:
                        violations.append({
                            "property": prop,
                            "element": element,
                            "type": "sla_breach",
                            "message": f"Data is {hours}h old, SLA requires update within {threshold_hours}h",
                            "current_hours": hours,
                            "threshold_hours": threshold_hours,
                        })
            except Exception:
                pass

        elif prop in ("availability", "av"):
            # Check table is queryable
            table_fqn = element
            if not table_fqn or table_fqn.count(".") < 2:
                continue
            try:
                execute_sql(client, warehouse_id, f"SELECT 1 FROM {table_fqn} LIMIT 1")
            except Exception as e:
                violations.append({
                    "property": prop,
                    "element": element,
                    "type": "availability_failure",
                    "message": f"Table not available: {e}",
                })

    return {"passed": len(violations) == 0, "violations": violations}


def _validate_servers(client, warehouse_id, doc: dict) -> dict:
    violations = []
    for srv in doc.get("servers", []):
        if srv.get("type", "").lower() == "databricks":
            cat = srv.get("catalog", "")
            sch = srv.get("schema", "")
            if cat:
                try:
                    rows = execute_sql(client, warehouse_id, "SHOW CATALOGS")
                    catalog_names = {r.get("catalog", "").lower() for r in rows}
                    if cat.lower() not in catalog_names:
                        violations.append({
                            "server": srv.get("server", ""),
                            "type": "catalog_not_found",
                            "message": f"Catalog '{cat}' not found",
                        })
                except Exception:
                    pass

            if cat and sch:
                try:
                    rows = execute_sql(client, warehouse_id, f"SHOW SCHEMAS IN {cat}")
                    schema_names = {r.get("databaseName", r.get("namespace", "")).lower() for r in rows}
                    if sch.lower() not in schema_names:
                        violations.append({
                            "server": srv.get("server", ""),
                            "type": "schema_not_found",
                            "message": f"Schema '{cat}.{sch}' not found",
                        })
                except Exception:
                    pass

    return {"passed": len(violations) == 0, "violations": violations}


def _validate_team(doc: dict) -> dict:
    violations = []
    team = doc.get("team")
    if team:
        if not team.get("name"):
            violations.append({"type": "missing_field", "message": "Team name is required"})
        for i, member in enumerate(team.get("members", [])):
            if not member.get("username"):
                violations.append({"type": "missing_field", "message": f"Team member {i} missing username"})
    return {"passed": len(violations) == 0, "violations": violations}


def _validate_roles(doc: dict) -> dict:
    violations = []
    for i, role in enumerate(doc.get("roles", [])):
        if not role.get("role"):
            violations.append({"type": "missing_field", "message": f"Role {i} missing role name"})
        if role.get("access") and role["access"] not in ("read", "write"):
            violations.append({"type": "invalid_value", "message": f"Role '{role.get('role', i)}' has invalid access '{role['access']}'"})
    return {"passed": len(violations) == 0, "violations": violations}


def _validate_support(doc: dict) -> dict:
    violations = []
    for i, ch in enumerate(doc.get("support", [])):
        if not ch.get("channel"):
            violations.append({"type": "missing_field", "message": f"Support channel {i} missing channel name"})
    return {"passed": len(violations) == 0, "violations": violations}


def _validate_pricing(doc: dict) -> dict:
    violations = []
    price = doc.get("price")
    if price:
        if price.get("priceAmount") is not None and price["priceAmount"] < 0:
            violations.append({"type": "invalid_value", "message": "Price amount cannot be negative"})
    return {"passed": len(violations) == 0, "violations": violations}


def _validate_references(doc: dict) -> dict:
    violations = []
    for i, ref in enumerate(doc.get("authoritativeDefinitions", [])):
        if not ref.get("url"):
            violations.append({"type": "missing_field", "message": f"Reference {i} missing URL"})
    return {"passed": len(violations) == 0, "violations": violations}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _types_compatible(declared: str, actual: str) -> bool:
    """Check if declared type is compatible with actual type (loose matching)."""
    d = declared.lower().split("(")[0].strip()
    a = actual.lower().split("(")[0].strip()
    if d == a:
        return True
    compat = {
        "string": {"string", "varchar", "char", "text"},
        "varchar": {"string", "varchar", "char", "text"},
        "int": {"int", "integer", "bigint", "smallint", "tinyint"},
        "integer": {"int", "integer", "bigint", "smallint", "tinyint"},
        "bigint": {"int", "integer", "bigint", "long"},
        "long": {"bigint", "long"},
        "double": {"double", "float", "decimal", "numeric"},
        "float": {"double", "float", "decimal", "numeric"},
        "decimal": {"double", "float", "decimal", "numeric"},
        "boolean": {"boolean", "bool"},
        "date": {"date"},
        "timestamp": {"timestamp", "datetime"},
    }
    return a in compat.get(d, {d})


def _odcs_metric_to_dq_rule(odcs_rule: dict, table_fqn: str) -> dict | None:
    """Convert an ODCS quality rule to a DQ engine rule dict."""
    metric = odcs_rule.get("metric", "")
    column = odcs_rule.get("_column", "")

    mapping = {
        "nullValues": "not_null",
        "duplicateValues": "unique",
        "rowCount": "row_count",
    }

    rule_type = mapping.get(metric)
    if not rule_type:
        return None

    dq_rule = {
        "rule_id": odcs_rule.get("id", "odcs_check"),
        "name": odcs_rule.get("name", metric),
        "table_fqn": table_fqn,
        "column_name": column,
        "rule_type": rule_type,
        "expression": "",
        "params": "{}",
        "threshold": 0.0,
        "severity": odcs_rule.get("severity", "error"),
    }

    # Set threshold from comparison operators
    if metric == "rowCount":
        params = {}
        if odcs_rule.get("mustBeGreaterThan") is not None:
            params["min"] = odcs_rule["mustBeGreaterThan"]
        if odcs_rule.get("mustBeGreaterOrEqualTo") is not None:
            params["min"] = odcs_rule["mustBeGreaterOrEqualTo"]
        if odcs_rule.get("mustBeLessThan") is not None:
            params["max"] = odcs_rule["mustBeLessThan"]
        if odcs_rule.get("mustBeLessOrEqualTo") is not None:
            params["max"] = odcs_rule["mustBeLessOrEqualTo"]
        if odcs_rule.get("mustBeBetween"):
            params["min"] = odcs_rule["mustBeBetween"][0]
            params["max"] = odcs_rule["mustBeBetween"][1]
        dq_rule["params"] = json.dumps(params)

    return dq_rule


def _get_numeric_result(row: dict) -> float:
    """Extract the first numeric value from a SQL result row."""
    for v in row.values():
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _check_comparison(rule: dict, value: float) -> bool:
    """Check if a value satisfies the ODCS comparison operators."""
    if rule.get("mustBe") is not None:
        return value == rule["mustBe"]
    if rule.get("mustNotBe") is not None:
        return value != rule["mustNotBe"]
    if rule.get("mustBeGreaterThan") is not None:
        return value > rule["mustBeGreaterThan"]
    if rule.get("mustBeGreaterOrEqualTo") is not None:
        return value >= rule["mustBeGreaterOrEqualTo"]
    if rule.get("mustBeLessThan") is not None:
        return value < rule["mustBeLessThan"]
    if rule.get("mustBeLessOrEqualTo") is not None:
        return value <= rule["mustBeLessOrEqualTo"]
    if rule.get("mustBeBetween"):
        return rule["mustBeBetween"][0] <= value <= rule["mustBeBetween"][1]
    if rule.get("mustNotBeBetween"):
        return not (rule["mustNotBeBetween"][0] <= value <= rule["mustNotBeBetween"][1])
    return True  # No comparison operator = pass


def _store_validation_result(client, warehouse_id, config, result: dict, user: str = ""):
    """Store a validation result in Delta."""
    schema = _get_schema(config)
    vid = str(uuid.uuid4())[:8]
    try:
        execute_sql(client, warehouse_id, f"""
            INSERT INTO {schema}.odcs_validation_results
            VALUES ('{vid}', '{_esc(result["contract_id"])}', '{_esc(result["version"])}',
                    '{_esc(json.dumps(list(result["sections"].keys())))}',
                    '{_esc(json.dumps(result["sections"]))}',
                    {str(result["compliant"]).lower()},
                    '{result["validated_at"]}', '{_esc(user)}')
        """)
    except Exception as e:
        logger.warning(f"Could not store validation result: {e}")


# ---------------------------------------------------------------------------
# Legacy migration
# ---------------------------------------------------------------------------

def migrate_legacy_contracts(client, warehouse_id, config, user: str = "") -> list[dict]:
    """Migrate legacy data_contracts to ODCS format."""
    schema = _get_schema(config)
    results = []

    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.data_contracts ORDER BY name")
    except Exception:
        return [{"error": "No legacy contracts table found"}]

    for row in rows:
        try:
            consumer_teams = json.loads(row.get("consumer_teams", "[]"))
        except Exception:
            consumer_teams = []
        try:
            expected_columns = json.loads(row.get("expected_columns", "[]"))
        except Exception:
            expected_columns = []

        table_fqn = row.get("table_fqn", "")
        parts = table_fqn.split(".")

        # Build ODCS document
        odcs_data = {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "name": row.get("name", ""),
            "version": "1.0.0",
            "status": row.get("status", "draft"),
            "domain": parts[0] if len(parts) >= 3 else "",
            "description": {
                "purpose": f"Migrated from legacy contract {row.get('contract_id', '')}",
            },
            "schema": [{
                "name": parts[-1] if parts else table_fqn,
                "physicalName": parts[-1] if parts else table_fqn,
                "physicalType": "table",
                "properties": [
                    {"name": col.get("name", ""), "physicalType": col.get("type", ""), "required": not col.get("nullable", True)}
                    for col in expected_columns
                ],
                "quality": [],
            }],
            "team": {
                "name": row.get("producer_team", ""),
                "members": [],
            },
            "roles": [{"role": team, "access": "read"} for team in consumer_teams],
            "slaProperties": [],
        }

        # Map freshness SLA
        freshness_hours = int(row.get("freshness_sla_hours", 0))
        if freshness_hours > 0:
            odcs_data["slaProperties"].append({
                "property": "latency",
                "value": freshness_hours,
                "unit": "h",
                "element": table_fqn,
                "driver": "operational",
            })

        # Map row count to quality rule
        row_min = int(row.get("row_count_min", 0))
        row_max = int(row.get("row_count_max", 0))
        if row_min > 0:
            odcs_data["schema"][0]["quality"].append({
                "metric": "rowCount",
                "type": "library",
                "dimension": "completeness",
                "mustBeGreaterOrEqualTo": row_min,
                "severity": "error",
            })
        if row_max > 0:
            odcs_data["schema"][0]["quality"].append({
                "metric": "rowCount",
                "type": "library",
                "dimension": "completeness",
                "mustBeLessOrEqualTo": row_max,
                "severity": "warning",
            })

        # Add server from config
        server = prefill_from_config(config)
        if len(parts) >= 3:
            server["catalog"] = parts[0]
            server["schema"] = parts[1]
        odcs_data["servers"] = [server]

        result = create_odcs_contract(client, warehouse_id, config, odcs_data, user)
        result["legacy_contract_id"] = row.get("contract_id", "")
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# Change tracking (reuses existing governance module)
# ---------------------------------------------------------------------------

def _track_change(client, warehouse_id, config, entity_type, entity_id, change_type, details, user=""):
    try:
        from src.governance import _track_change as _gov_track
        _gov_track(client, warehouse_id, config, entity_type, entity_id, change_type, details, user)
    except Exception:
        pass  # Governance tables may not exist yet


# ---------------------------------------------------------------------------
# Deep merge
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, updates: dict) -> dict:
    """Deep merge updates into base dict. Updates take precedence."""
    result = base.copy()
    for key, val in updates.items():
        if val is None:
            continue
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


# ---------------------------------------------------------------------------
# Contract Generation from Unity Catalog
# ---------------------------------------------------------------------------

_SPARK_TO_LOGICAL = {
    "string": "string", "varchar": "string", "char": "string", "text": "string",
    "int": "integer", "integer": "integer", "smallint": "integer", "tinyint": "integer",
    "bigint": "integer", "long": "integer",
    "float": "number", "double": "number", "decimal": "number", "numeric": "number",
    "boolean": "boolean", "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp", "timestamp_ntz": "timestamp", "datetime": "timestamp",
    "time": "time",
    "binary": "string",
}


def _map_spark_to_logical(spark_type: str) -> str:
    """Map a Spark/Databricks data type to an ODCS logical type."""
    base = spark_type.lower().split("(")[0].split("<")[0].strip()
    if base.startswith("array"):
        return "array"
    if base.startswith("struct") or base.startswith("map"):
        return "object"
    return _SPARK_TO_LOGICAL.get(base, "string")


def _get_classification_from_tags(col_tags: list[dict], col_name: str) -> str:
    """Derive ODCS classification from UC column tags."""
    for tag in col_tags:
        if tag.get("column_name", "").lower() != col_name.lower():
            continue
        tn = tag.get("tag_name", "").lower()
        tv = tag.get("tag_value", "").lower()
        if tn in ("classification", "data_classification", "sensitivity"):
            if tv in ("confidential", "secret", "highly_sensitive"):
                return "confidential"
            if tv in ("restricted", "sensitive", "pii"):
                return "restricted"
            if tv in ("internal",):
                return "internal"
            if tv in ("public", "open"):
                return "public"
            return tv  # pass through unknown values
        if tn in ("pii_type", "pii", "sensitive"):
            return "restricted"
    return ""


def _is_cde(col_tags: list[dict], col_name: str) -> bool:
    """Check if a column is tagged as a Critical Data Element."""
    for tag in col_tags:
        if tag.get("column_name", "").lower() != col_name.lower():
            continue
        tn = tag.get("tag_name", "").lower()
        if tn in ("cde", "critical_data_element", "critical"):
            return True
    return False


def _get_upstream_lineage(client, warehouse_id, table_fqn: str) -> list[dict]:
    """Query system.access.table_lineage for upstream sources → ODCS relationships."""
    relationships = []
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT DISTINCT source_table_full_name, source_type, target_type
            FROM system.access.table_lineage
            WHERE target_table_full_name = '{_esc(table_fqn)}'
              AND source_table_full_name IS NOT NULL
            ORDER BY source_table_full_name
            LIMIT 50
        """)
        for r in rows:
            src = r.get("source_table_full_name", "")
            if src:
                relationships.append({
                    "type": "lineage",
                    "from": [src],
                    "to": [table_fqn],
                    "customProperties": [
                        {"property": "source_type", "value": r.get("source_type", "")},
                        {"property": "target_type", "value": r.get("target_type", "")},
                    ],
                })
    except Exception as e:
        logger.debug(f"system.access.table_lineage not available: {e}")
    return relationships


def _get_column_lineage_relationships(client, warehouse_id, table_fqn: str) -> list[dict]:
    """Query system.access.column_lineage for column-level mappings."""
    relationships = []
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT source_table_full_name, source_column_name,
                   target_table_full_name, target_column_name
            FROM system.access.column_lineage
            WHERE target_table_full_name = '{_esc(table_fqn)}'
              AND source_column_name IS NOT NULL
              AND target_column_name IS NOT NULL
            LIMIT 100
        """)
        for r in rows:
            src_tbl = r.get("source_table_full_name", "")
            src_col = r.get("source_column_name", "")
            tgt_col = r.get("target_column_name", "")
            if src_tbl and src_col and tgt_col:
                relationships.append({
                    "type": "lineage",
                    "from": [f"{src_tbl}.{src_col}"],
                    "to": [f"{table_fqn}.{tgt_col}"],
                })
    except Exception as e:
        logger.debug(f"system.access.column_lineage not available: {e}")
    return relationships


def _estimate_update_frequency(history_rows: list[dict]) -> dict | None:
    """Estimate update frequency SLA from table history timestamps."""
    if len(history_rows) < 2:
        return None
    timestamps = []
    for h in history_rows:
        ts = h.get("timestamp", "")
        if ts:
            timestamps.append(str(ts))
    if len(timestamps) < 2:
        return None
    # Rough estimate: average gap between last few updates
    try:
        from datetime import datetime as dt
        parsed = sorted([dt.fromisoformat(t.replace("Z", "+00:00").split("+")[0]) for t in timestamps], reverse=True)
        gaps_hours = []
        for i in range(min(len(parsed) - 1, 5)):
            gap = (parsed[i] - parsed[i + 1]).total_seconds() / 3600
            if gap > 0:
                gaps_hours.append(gap)
        if gaps_hours:
            avg_hours = sum(gaps_hours) / len(gaps_hours)
            if avg_hours < 2:
                return {"property": "frequency", "value": 1, "unit": "h"}
            elif avg_hours < 48:
                return {"property": "frequency", "value": round(avg_hours), "unit": "h"}
            else:
                return {"property": "frequency", "value": round(avg_hours / 24), "unit": "d"}
    except Exception:
        pass
    return None


def _roles_from_row_filters(row_filters: list[dict]) -> list[dict]:
    """Convert row filter policies to ODCS roles."""
    roles = []
    for rf in row_filters:
        roles.append({
            "role": rf.get("filter_name", "filtered_access"),
            "access": "read",
            "firstLevelApprovers": "",
            "secondLevelApprovers": "",
        })
    return roles


def generate_contract_from_uc(client, warehouse_id, config, table_fqn: str, options: dict | None = None) -> dict:
    """Auto-generate an ODCS v3.1.0 contract from Unity Catalog metadata.

    Introspects: columns, tags, properties, lineage, freshness, row count,
    column masks, row filters, and optionally DQX profiling.
    """
    from src.client import get_table_info_sdk

    opts = {
        "include_quality_rules": True,
        "include_dqx_profiling": False,
        "include_lineage": True,
        "include_sla": True,
        "include_tags": True,
        "include_properties": True,
        "include_masks": True,
        "include_row_filters": True,
        "include_history": True,
        "row_count_threshold_pct": 80,
        "freshness_sla_multiplier": 2,
        "dqx_sample_fraction": 0.3,
    }
    if options:
        opts.update(options)

    parts = table_fqn.split(".")
    if len(parts) != 3:
        return {"error": f"Invalid FQN: expected catalog.schema.table, got '{table_fqn}'"}
    catalog, schema_name, table_name = parts

    # 1. Table info via SDK
    table_info = get_table_info_sdk(client, table_fqn) or {}
    columns = table_info.get("columns", [])
    owner = table_info.get("owner", "")
    comment = table_info.get("comment", "")
    table_type = table_info.get("table_type", "TABLE").lower()
    physical_type = "view" if "view" in table_type else "table"

    # 2. Richer column metadata from information_schema
    try:
        from src.schema_drift import get_columns_info
        rich_cols = get_columns_info(client, warehouse_id, catalog, schema_name, table_name)
        rich_map = {r.get("column_name", "").lower(): r for r in rich_cols}
    except Exception:
        rich_map = {}

    # 3. Tags
    table_tags = []
    col_tags = []
    if opts["include_tags"]:
        try:
            from src.clone_tags import get_table_tags, get_column_tags, get_catalog_tags
            table_tags = get_table_tags(client, warehouse_id, catalog, schema_name, table_name)
            col_tags = get_column_tags(client, warehouse_id, catalog, schema_name, table_name)
        except Exception:
            pass

    # Domain: from catalog tags or catalog name
    domain = catalog
    if opts["include_tags"]:
        try:
            from src.clone_tags import get_catalog_tags
            cat_tags = get_catalog_tags(client, warehouse_id, catalog)
            for t in cat_tags:
                if t.get("tag_name", "").lower() == "domain":
                    domain = t.get("tag_value", catalog)
                    break
        except Exception:
            pass

    # 4. Table properties
    custom_props = []
    if opts["include_properties"]:
        try:
            from src.clone_tags import get_table_properties
            props = get_table_properties(client, warehouse_id, catalog, schema_name, table_name)
            custom_props = [{"property": p.get("key", ""), "value": str(p.get("value", ""))} for p in props if p.get("key")]
        except Exception:
            pass

    # 5. Column masks
    mask_map = {}
    if opts["include_masks"]:
        try:
            from src.security import get_column_masks
            masks = get_column_masks(client, warehouse_id, catalog, schema_name, table_name)
            mask_map = {m.get("column_name", "").lower(): m.get("mask_function_name", "") for m in masks}
        except Exception:
            pass

    # 6. Row filters → roles
    roles = []
    if opts["include_row_filters"]:
        try:
            from src.security import get_row_filters
            filters = get_row_filters(client, warehouse_id, catalog, schema_name, table_name)
            roles = _roles_from_row_filters(filters)
        except Exception:
            pass

    # 7. Lineage
    table_relationships = []
    col_relationships = []
    if opts["include_lineage"]:
        table_relationships = _get_upstream_lineage(client, warehouse_id, table_fqn)
        col_relationships = _get_column_lineage_relationships(client, warehouse_id, table_fqn)

    # 8. Freshness + size
    sla_properties = []
    freshness_hours = None
    if opts["include_sla"]:
        try:
            detail = execute_sql(client, warehouse_id, f"DESCRIBE DETAIL {table_fqn}")
            if detail and detail[0].get("lastModified"):
                hours_rows = execute_sql(client, warehouse_id,
                    f"SELECT datediff(hour, timestamp '{detail[0]['lastModified']}', current_timestamp()) AS hours")
                freshness_hours = int(hours_rows[0]["hours"]) if hours_rows else None
                if freshness_hours is not None:
                    sla_value = max(freshness_hours * opts["freshness_sla_multiplier"], 1)
                    sla_properties.append({
                        "property": "latency",
                        "value": sla_value,
                        "unit": "h",
                        "element": table_fqn,
                        "driver": "operational",
                        "description": f"Auto-detected: table was {freshness_hours}h old at generation time",
                    })
        except Exception:
            pass

    # 9. History → frequency + retention estimate
    if opts["include_history"]:
        try:
            history = execute_sql(client, warehouse_id, f"DESCRIBE HISTORY {table_fqn} LIMIT 10")
            freq = _estimate_update_frequency(history)
            if freq:
                freq["element"] = table_fqn
                freq["driver"] = "operational"
                sla_properties.append(freq)
        except Exception:
            pass

    # 10. Row count
    row_count = 0
    try:
        cnt_rows = execute_sql(client, warehouse_id, f"SELECT count(*) AS cnt FROM {table_fqn}")
        row_count = int(cnt_rows[0]["cnt"]) if cnt_rows else 0
    except Exception:
        pass

    # 11. View definition for transformLogic
    view_definition = ""
    if physical_type == "view":
        try:
            create_rows = execute_sql(client, warehouse_id, f"SHOW CREATE TABLE {table_fqn}")
            if create_rows:
                view_definition = create_rows[0].get("createtab_stmt", "")
        except Exception:
            pass

    # 12. Partition keys from DESCRIBE TABLE EXTENDED
    partition_cols = set()
    pk_cols = set()
    try:
        desc_rows = execute_sql(client, warehouse_id, f"DESCRIBE TABLE EXTENDED {table_fqn}")
        in_partition = False
        for r in desc_rows:
            col = r.get("col_name", "").strip()
            if col == "# Partition Information":
                in_partition = True
                continue
            if in_partition and col and not col.startswith("#"):
                partition_cols.add(col.lower())
            if col.startswith("#"):
                in_partition = False
    except Exception:
        pass

    # --- Build ODCS properties (columns) ---
    odcs_properties = []
    for col in columns:
        col_name = col.get("column_name", "")
        col_lower = col_name.lower()
        rich = rich_map.get(col_lower, {})
        data_type = col.get("data_type", "")
        is_nullable = col.get("nullable", True)
        if isinstance(is_nullable, str):
            is_nullable = is_nullable.lower() != "no"

        prop = {
            "name": col_name,
            "physicalName": col_name,
            "logicalType": _map_spark_to_logical(data_type),
            "physicalType": data_type,
            "required": not is_nullable,
            "description": col.get("comment", "") or rich.get("comment", "") or "",
            "primaryKey": col_lower in pk_cols,
            "partitioned": col_lower in partition_cols,
            "classification": _get_classification_from_tags(col_tags, col_name),
            "criticalDataElement": _is_cde(col_tags, col_name),
        }

        # Column mask → encryptedName
        if col_lower in mask_map:
            prop["encryptedName"] = f"{col_name}_masked"
            if not prop["classification"]:
                prop["classification"] = "restricted"

        # Column-level quality rules
        col_quality = []
        if opts["include_quality_rules"]:
            if not is_nullable:
                col_quality.append({
                    "metric": "nullValues", "type": "library",
                    "dimension": "completeness", "mustBe": 0, "severity": "error",
                })

        if col_quality:
            prop["quality"] = col_quality

        odcs_properties.append(prop)

    # --- Object-level quality rules ---
    object_quality = []
    if opts["include_quality_rules"] and row_count > 0:
        min_rows = int(row_count * opts["row_count_threshold_pct"] / 100)
        object_quality.append({
            "metric": "rowCount", "type": "library", "dimension": "completeness",
            "mustBeGreaterOrEqualTo": min_rows, "severity": "error",
            "description": f"Auto-detected: table had {row_count} rows at generation time",
        })

    # 13. DQX profiling (optional)
    if opts["include_dqx_profiling"]:
        dqx_rules = run_dqx_profiling(client, warehouse_id, table_fqn, opts)
        if isinstance(dqx_rules, list):
            object_quality.extend(dqx_rules)

    # --- Assemble ODCS document ---
    all_relationships = table_relationships + col_relationships
    server = prefill_from_config(config)
    server["catalog"] = catalog
    server["schema"] = schema_name

    odcs = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "name": f"{table_name}-contract",
        "version": "1.0.0",
        "status": "draft",
        "domain": domain,
        "dataProduct": "",
        "tenant": "",
        "tags": [t.get("tag_name", "") for t in table_tags if t.get("tag_name")],
        "description": {
            "purpose": comment or f"Auto-generated contract for {table_fqn}",
            "limitations": "",
            "usage": "",
        },
        "schema": [{
            "name": table_name,
            "physicalName": table_name,
            "physicalType": physical_type,
            "businessName": comment or table_name,
            "description": comment or "",
            "tags": [t.get("tag_name", "") for t in table_tags],
            "properties": odcs_properties,
            "relationships": all_relationships,
            "quality": object_quality,
        }],
        "slaProperties": sla_properties,
        "team": {"name": owner or "unknown", "members": [{"username": owner, "role": "Owner"}] if owner else []},
        "roles": roles,
        "servers": [server],
        "customProperties": custom_props,
    }

    # Add view definition as transformLogic
    if view_definition:
        odcs["schema"][0]["properties"] = odcs_properties  # already set
        odcs["schema"][0]["customProperties"] = [{"property": "viewDefinition", "value": view_definition[:2000]}]

    return odcs


def run_dqx_profiling(client, warehouse_id, table_fqn: str, options: dict | None = None) -> list[dict]:
    """Run DQX Profiler on a table and return ODCS-formatted quality rules.

    Requires databricks-labs-dqx to be installed.
    """
    try:
        from databricks.sdk import WorkspaceClient as _WC2  # noqa: F401
        from databricks.labs.dqx.profiler.profiler import DQProfiler
        from databricks.labs.dqx.profiler.generator import DQGenerator
    except ImportError:
        logger.debug("DQX not installed — skipping profiling")
        return []

    try:
        ws = client  # authenticated WorkspaceClient from API
        from src.spark_session import get_spark
        spark = get_spark()
        profiler = DQProfiler(ws, spark=spark)
        generator = DQGenerator(ws, spark=spark)

        # Profile the table — requires InputConfig, not raw string
        from databricks.labs.dqx.config import InputConfig
        sample = (options or {}).get("dqx_sample_fraction", 0.3)
        input_config = InputConfig(location=table_fqn)
        _stats, profiles = profiler.profile_table(input_config, options={"sample_fraction": sample})
        if not profiles:
            return []

        # Generate DQX rules from profiles
        dq_rules = generator.generate_dq_rules(profiles)
        if not dq_rules:
            return []

        # Map DQX rules → ODCS quality rules
        odcs_rules = []
        for rule in dq_rules:
            func_name = ""
            col_name = ""
            args = {}
            criticality = "error"

            # Extract from DQRule object
            if hasattr(rule, "check_func"):
                func_name = getattr(rule.check_func, "__name__", str(rule.check_func))
            if hasattr(rule, "column"):
                col_name = rule.column
            if hasattr(rule, "check_func_kwargs"):
                args = rule.check_func_kwargs or {}
            if hasattr(rule, "criticality"):
                criticality = rule.criticality

            odcs_rule = {
                "type": "custom",
                "engine": "dqx",
                "severity": "error" if criticality == "error" else "warning",
                "dqx_function": func_name,
                "dqx_arguments": args,
                "dqx_criticality": criticality,
            }

            # Map known DQX functions to ODCS library metrics
            if "is_not_null" in func_name:
                odcs_rule.update({"type": "library", "metric": "nullValues", "dimension": "completeness", "mustBe": 0})
            elif "is_in_list" in func_name:
                valid_values = args.get("allowed", args.get("values", []))
                odcs_rule.update({"type": "library", "metric": "invalidValues", "dimension": "conformity", "arguments": {"validValues": valid_values}})
            elif "is_in_range" in func_name:
                min_val = args.get("min_value", args.get("min", None))
                max_val = args.get("max_value", args.get("max", None))
                if min_val is not None and max_val is not None:
                    odcs_rule.update({"type": "library", "metric": "invalidValues", "dimension": "conformity", "mustBeBetween": [min_val, max_val]})
            elif "is_unique" in func_name:
                odcs_rule.update({"type": "library", "metric": "duplicateValues", "dimension": "uniqueness", "mustBe": 0})
            elif "is_not_null_and_not_empty" in func_name:
                odcs_rule.update({"type": "library", "metric": "missingValues", "dimension": "completeness", "mustBe": 0})

            if col_name:
                odcs_rule["name"] = f"{func_name}_{col_name}"
                odcs_rule["description"] = f"DQX profiled: {func_name} on {col_name}"

            odcs_rules.append(odcs_rule)

        logger.info(f"DQX profiling generated {len(odcs_rules)} quality rules for {table_fqn}")
        return odcs_rules

    except Exception as e:
        logger.warning(f"DQX profiling failed for {table_fqn}: {e}")
        return []


def generate_contracts_for_schema(client, warehouse_id, config, catalog: str, schema_name: str, options: dict | None = None) -> list[dict]:
    """Generate ODCS contracts for all tables in a schema."""
    from src.client import list_tables_sdk

    tables = list_tables_sdk(client, catalog, schema_name)
    results = []
    for t in tables:
        tbl_name = t.get("table_name", "") if isinstance(t, dict) else t
        if not tbl_name:
            continue
        fqn = f"{catalog}.{schema_name}.{tbl_name}"
        try:
            doc = generate_contract_from_uc(client, warehouse_id, config, fqn, options)
            if doc and "error" not in doc:
                results.append(doc)
            else:
                results.append({"table_fqn": fqn, "error": doc.get("error", "Unknown error")})
        except Exception as e:
            results.append({"table_fqn": fqn, "error": str(e)})
    return results


def generate_contracts_for_catalog(client, warehouse_id, config, catalog: str, options: dict | None = None, exclude_schemas: list[str] | None = None) -> list[dict]:
    """Generate ODCS contracts for all tables in a catalog."""
    from src.client import list_schemas_sdk

    exclude = exclude_schemas or ["information_schema"]
    schemas = list_schemas_sdk(client, catalog, exclude=exclude)
    results = []
    for schema_name in schemas:
        schema_results = generate_contracts_for_schema(client, warehouse_id, config, catalog, schema_name, options)
        results.extend(schema_results)
    return results
