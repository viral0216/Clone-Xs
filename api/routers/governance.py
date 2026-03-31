"""Governance API endpoints — Data Dictionary, DQ Rules, Certifications, SLA, ODCS Contracts."""

import json
import queue
import threading

from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response, StreamingResponse

from api.dependencies import get_db_client, get_app_config, get_credentials
from api.models.governance import (
    GlossaryTermCreate, GlossaryLinkRequest, MetadataSearchRequest,
    DQRuleCreate, DQRunRequest,
    CertificationCreate, CertificationApproval,
    SLARuleCreate, DQXCheckCreate, DQXRunRequest, DQXProfileRequest,
)
from api.models.odcs import (
    ODCSContractCreate, ODCSContractUpdate, ODCSImportRequest,
    ODCSGenerateRequest, ODCSGenerateSchemaRequest, ODCSGenerateCatalogRequest,
)

router = APIRouter()


def _ensure_spark(host: str | None = None, token: str | None = None, client=None):
    """Ensure Spark session is configured with the correct workspace credentials.

    Called by DQX endpoints before any Spark operation. Uses host/token from
    request headers, or extracts from the authenticated WorkspaceClient.
    Respects the global serverless preference from X-Use-Serverless header.
    """
    from src.spark_session import configure_spark, _spark_config
    from api.dependencies import get_serverless_preference

    # Try to get host/token from client.config if not provided via headers
    if not host and client is not None:
        try:
            host = getattr(client.config, "host", None)
            token = token or getattr(client.config, "token", None)
        except Exception:
            pass

    # Respect global serverless preference from Settings UI
    serverless_pref = get_serverless_preference()
    use_serverless = serverless_pref if serverless_pref is not None else _spark_config.get("serverless", True)

    current_host = _spark_config.get("host", "")

    # If we have credentials and they differ from current config → reconfigure
    if host and host != current_host:
        import logging
        logging.getLogger(__name__).info(f"Spark: configuring from request (host={host[:40]})")
        configure_spark(
            host=host,
            token=token or "",
            serverless=use_serverless,
            cluster_id=_spark_config.get("cluster_id", ""),
        )
    elif not current_host and not host:
        # No config at all — try env as last resort
        import os
        env_host = os.environ.get("DATABRICKS_HOST", "")
        if env_host:
            configure_spark(
                host=env_host,
                token=os.environ.get("DATABRICKS_TOKEN", ""),
                serverless=use_serverless,
                cluster_id=_spark_config.get("cluster_id", ""),
            )


# ---------------------------------------------------------------------------
# Init / Setup
# ---------------------------------------------------------------------------

@router.post("/init")
async def init_governance_tables(client=Depends(get_db_client)):
    """Initialize all governance Delta tables."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    if not wid:
        return {"error": "No SQL warehouse configured"}

    from src.governance import ensure_governance_tables
    from src.dq_rules import ensure_dq_tables
    from src.sla_monitor import ensure_sla_tables
    from src.data_contracts import ensure_odcs_tables
    from src.dqx_engine import ensure_dqx_tables
    from src.reconciliation_store import ensure_reconciliation_tables
    from src.reconciliation_alerts import ensure_alert_tables

    ensure_governance_tables(client, wid, config)
    ensure_dq_tables(client, wid, config)
    ensure_sla_tables(client, wid, config)
    ensure_odcs_tables(client, wid, config)
    ensure_dqx_tables(client, wid, config)
    ensure_reconciliation_tables(client, wid, config)
    ensure_alert_tables(client, wid, config)
    return {"status": "ok", "message": "All governance and reconciliation tables initialized"}


# ---------------------------------------------------------------------------
# Data Dictionary / Glossary
# ---------------------------------------------------------------------------

@router.post("/glossary")
async def create_term(req: GlossaryTermCreate, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import create_glossary_term
    return create_glossary_term(client, wid, config, req.model_dump())


@router.get("/glossary")
async def list_terms(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import list_glossary_terms
    return list_glossary_terms(client, wid, config)


@router.get("/glossary/{term_id}")
async def get_term(term_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import get_glossary_term
    return get_glossary_term(client, wid, config, term_id) or {"error": "Term not found"}


@router.delete("/glossary/{term_id}")
async def delete_term(term_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import delete_glossary_term
    delete_glossary_term(client, wid, config, term_id)
    return {"status": "deleted"}


@router.post("/glossary/link")
async def link_term(req: GlossaryLinkRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import link_term_to_columns
    link_term_to_columns(client, wid, config, req.term_id, req.column_fqns)
    return {"status": "linked", "columns": len(req.column_fqns)}


# ---------------------------------------------------------------------------
# Global Metadata Search
# ---------------------------------------------------------------------------

@router.post("/search")
async def search(req: MetadataSearchRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import search_metadata
    return search_metadata(client, wid, config, req.query, req.catalogs, req.search_type, req.limit)


# ---------------------------------------------------------------------------
# Data Quality Rules
# ---------------------------------------------------------------------------

@router.post("/dq/rules")
async def create_dq_rule(req: DQRuleCreate, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import create_rule
    return create_rule(client, wid, config, req.model_dump())


@router.get("/dq/rules")
async def list_dq_rules(table_fqn: str = "", severity: str = "", client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import list_rules
    return list_rules(client, wid, config, table_fqn, severity)


@router.put("/dq/rules/{rule_id}")
async def update_dq_rule(rule_id: str, req: dict, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import update_rule
    update_rule(client, wid, config, rule_id, req)
    return {"status": "updated"}


@router.delete("/dq/rules/{rule_id}")
async def delete_dq_rule(rule_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import delete_rule
    delete_rule(client, wid, config, rule_id)
    return {"status": "deleted"}


@router.post("/dq/run")
async def run_dq(req: DQRunRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import run_rules
    return run_rules(client, wid, config, req.rule_ids, req.catalog, req.table_fqn)


@router.get("/dq/results")
async def get_dq_results(table_fqn: str = "", client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import get_latest_results
    return get_latest_results(client, wid, config, table_fqn)


@router.get("/dq/history")
async def get_dq_history(rule_id: str = "", days: int = 30, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dq_rules import get_dq_history as _get_history
    return _get_history(client, wid, config, rule_id, days)


# ---------------------------------------------------------------------------
# Certifications
# ---------------------------------------------------------------------------

@router.post("/certifications")
async def create_certification(req: CertificationCreate, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import certify_table
    return certify_table(client, wid, config, req.model_dump())


@router.get("/certifications")
async def list_certifications_endpoint(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import list_certifications
    return list_certifications(client, wid, config)


@router.post("/certifications/approve")
async def approve_cert(req: CertificationApproval, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import approve_certification
    approve_certification(client, wid, config, req.cert_id, req.action, req.reviewer_notes)
    return {"status": req.action + "d"}


# ---------------------------------------------------------------------------
# SLA Rules
# ---------------------------------------------------------------------------

@router.post("/sla/rules")
async def create_sla(req: SLARuleCreate, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import create_sla_rule
    return create_sla_rule(client, wid, config, req.model_dump())


@router.get("/sla/rules")
async def list_sla(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import list_sla_rules
    return list_sla_rules(client, wid, config)


@router.post("/sla/check")
async def check_sla_endpoint(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import check_sla
    return check_sla(client, wid, config)


@router.get("/sla/status")
async def sla_status(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import get_sla_status
    return get_sla_status(client, wid, config)


@router.get("/sla/compliance-trend")
async def sla_compliance_trend(days: int = 30, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import get_sla_compliance_trend
    return get_sla_compliance_trend(client, wid, config, days)


@router.delete("/sla/rules/{sla_id}")
async def delete_sla(sla_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import delete_sla_rule
    delete_sla_rule(client, wid, config, sla_id)
    return {"status": "deleted", "sla_id": sla_id}


# ---------------------------------------------------------------------------
# ODCS Data Contracts (v3.1.0)
# ---------------------------------------------------------------------------

@router.post("/odcs/contracts")
async def create_odcs_contract_endpoint(req: ODCSContractCreate, client=Depends(get_db_client)):
    """Create a new ODCS v3.1.0 data contract."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import create_odcs_contract
    return create_odcs_contract(client, wid, config, req.model_dump(by_alias=True))


@router.get("/odcs/contracts")
async def list_odcs_contracts_endpoint(domain: str = "", status: str = "", table_fqn: str = "", client=Depends(get_db_client)):
    """List ODCS contracts with optional filters."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import list_odcs_contracts
    return list_odcs_contracts(client, wid, config, domain, status, table_fqn)


@router.get("/odcs/contracts/{contract_id}")
async def get_odcs_contract_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Get a single ODCS contract with full document."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import get_odcs_contract
    return get_odcs_contract(client, wid, config, contract_id) or {"error": "Contract not found"}


@router.put("/odcs/contracts/{contract_id}")
async def update_odcs_contract_endpoint(contract_id: str, req: ODCSContractUpdate, client=Depends(get_db_client)):
    """Update an ODCS contract."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import update_odcs_contract
    updates = req.model_dump(by_alias=True, exclude_none=True)
    return update_odcs_contract(client, wid, config, contract_id, updates)


@router.delete("/odcs/contracts/{contract_id}")
async def delete_odcs_contract_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Delete an ODCS contract."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import delete_odcs_contract
    try:
        delete_odcs_contract(client, wid, config, contract_id)
        return {"status": "deleted", "contract_id": contract_id}
    except Exception as e:
        return {"error": f"Failed to delete contract: {e}"}


@router.post("/odcs/contracts/{contract_id}/validate")
async def validate_odcs_contract_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Run full ODCS validation against all 11 sections."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import validate_odcs_contract
    return validate_odcs_contract(client, wid, config, contract_id)


@router.get("/odcs/contracts/{contract_id}/versions")
async def get_odcs_versions_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Get version history for an ODCS contract."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import get_contract_versions
    return get_contract_versions(client, wid, config, contract_id)


@router.get("/odcs/contracts/{contract_id}/versions/{version}")
async def get_odcs_version_endpoint(contract_id: str, version: str, client=Depends(get_db_client)):
    """Get a specific version of an ODCS contract."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import get_contract_version
    return get_contract_version(client, wid, config, contract_id, version) or {"error": "Version not found"}


@router.post("/odcs/import")
async def import_odcs_yaml_endpoint(req: ODCSImportRequest, client=Depends(get_db_client)):
    """Import a contract from ODCS YAML."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import import_odcs_yaml
    return import_odcs_yaml(client, wid, config, req.yaml_content)


@router.get("/odcs/contracts/{contract_id}/export")
async def export_odcs_yaml_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Export an ODCS contract as YAML."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import export_odcs_yaml
    yaml_content = export_odcs_yaml(client, wid, config, contract_id)
    if not yaml_content:
        return {"error": "Contract not found"}
    return Response(content=yaml_content, media_type="text/yaml",
                    headers={"Content-Disposition": f"attachment; filename={contract_id}.odcs.yaml"})


@router.get("/odcs/prefill")
async def prefill_odcs_endpoint(client=Depends(get_db_client)):
    """Get pre-filled server config from clone_config.yaml."""
    config = await get_app_config()
    from src.data_contracts import prefill_from_config
    return prefill_from_config(config)


@router.post("/odcs/contracts/{contract_id}/map-dq")
async def map_dq_to_odcs_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Map existing DQ rules to ODCS quality format for this contract's tables."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import map_dq_rules_to_odcs
    return map_dq_rules_to_odcs(client, wid, config, contract_id)


@router.post("/odcs/contracts/{contract_id}/map-sla")
async def map_sla_to_odcs_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Map existing SLA rules to ODCS slaProperties format."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import map_sla_rules_to_odcs
    return map_sla_rules_to_odcs(client, wid, config, contract_id)


@router.post("/odcs/migrate")
async def migrate_legacy_contracts_endpoint(client=Depends(get_db_client)):
    """Migrate legacy data contracts to ODCS v3.1.0 format."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import migrate_legacy_contracts
    return migrate_legacy_contracts(client, wid, config)


@router.post("/odcs/contracts/{contract_id}/dqx-validate")
async def dqx_validate_endpoint(contract_id: str, client=Depends(get_db_client)):
    """Run DQX-based DataFrame validation for this contract."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import run_dqx_validation
    return run_dqx_validation(client, wid, config, contract_id)


# ---------------------------------------------------------------------------
# ODCS Contract Generation from Unity Catalog
# ---------------------------------------------------------------------------

@router.post("/odcs/generate")
async def generate_from_uc_endpoint(req: ODCSGenerateRequest, client=Depends(get_db_client)):
    """Generate an ODCS contract by introspecting a Unity Catalog table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import generate_contract_from_uc, create_odcs_contract
    opts = req.model_dump(exclude={"table_fqn", "auto_save"})
    doc = generate_contract_from_uc(client, wid, config, req.table_fqn, opts)
    if doc.get("error"):
        return doc
    if req.auto_save:
        result = create_odcs_contract(client, wid, config, doc)
        doc["_saved"] = result
    return doc


@router.post("/odcs/generate-schema")
async def generate_schema_endpoint(req: ODCSGenerateSchemaRequest, client=Depends(get_db_client)):
    """Generate ODCS contracts for all tables in a schema."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import generate_contracts_for_schema, create_odcs_contract
    opts = req.model_dump(exclude={"catalog", "schema_name", "auto_save"})
    docs = generate_contracts_for_schema(client, wid, config, req.catalog, req.schema_name, opts)
    if req.auto_save:
        for doc in docs:
            if isinstance(doc, dict) and "error" not in doc:
                try:
                    doc["_saved"] = create_odcs_contract(client, wid, config, doc)
                except Exception as e:
                    doc["_saved"] = {"error": str(e)}
    return {"contracts": docs, "count": len(docs)}


@router.post("/odcs/generate-catalog")
async def generate_catalog_endpoint(req: ODCSGenerateCatalogRequest, client=Depends(get_db_client)):
    """Generate ODCS contracts for all tables in a catalog."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.data_contracts import generate_contracts_for_catalog, create_odcs_contract
    opts = req.model_dump(exclude={"catalog", "exclude_schemas", "auto_save"})
    docs = generate_contracts_for_catalog(client, wid, config, req.catalog, opts, req.exclude_schemas)
    if req.auto_save:
        for doc in docs:
            if isinstance(doc, dict) and "error" not in doc:
                try:
                    doc["_saved"] = create_odcs_contract(client, wid, config, doc)
                except Exception as e:
                    doc["_saved"] = {"error": str(e)}
    return {"contracts": docs, "count": len(docs)}


# ---------------------------------------------------------------------------
# DQX — Data Quality with databricks-labs-dqx
# ---------------------------------------------------------------------------

@router.get("/dqx/spark-status")
async def dqx_spark_status_endpoint(client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Check Spark session availability and connection status."""
    _ensure_spark(creds[0], creds[1], client)
    from src.spark_session import get_spark_status
    return get_spark_status()


@router.post("/dqx/spark-configure")
async def dqx_spark_configure_endpoint(req: dict, creds: tuple = Depends(get_credentials)):
    """Configure Spark session (cluster_id or serverless mode).

    Uses the request's authentication credentials so the Spark session
    connects to the same workspace the user is authenticated with.
    """
    import os
    from src.spark_session import configure_spark
    # Use request header credentials first, fall back to env vars
    host = creds[0] or os.environ.get("DATABRICKS_HOST", "")
    token = creds[1] or os.environ.get("DATABRICKS_TOKEN", "")
    configure_spark(
        cluster_id=req.get("cluster_id", ""),
        serverless=req.get("serverless", False),
        host=host,
        token=token,
    )
    from src.spark_session import get_spark_status
    return get_spark_status()


@router.get("/dqx/dashboard")
async def dqx_dashboard_endpoint(client=Depends(get_db_client)):
    """Get DQX dashboard summary: checks, pass rates, latest runs."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import get_dqx_dashboard
    return get_dqx_dashboard(client, wid, config)


@router.get("/dqx/functions")
async def dqx_functions_endpoint():
    """List all available DQX check functions."""
    from src.dqx_engine import list_check_functions
    return list_check_functions()


@router.post("/dqx/profile")
async def dqx_profile_endpoint(req: DQXProfileRequest, client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Profile a table using DQX Profiler and optionally auto-generate checks."""
    _ensure_spark(creds[0], creds[1], client)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    opts = req.model_dump(exclude={"table_fqn", "auto_generate_checks"})
    from src.dqx_engine import profile_table, generate_checks_from_profiles
    if req.auto_generate_checks:
        return generate_checks_from_profiles(client, wid, config, req.table_fqn, options=opts)
    return profile_table(client, wid, config, req.table_fqn, opts)


@router.post("/dqx/profile-schema")
async def dqx_profile_schema_endpoint(req: dict, client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Profile all tables in a schema and auto-generate DQX checks."""
    _ensure_spark(creds[0], creds[1], client)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import generate_checks_for_schema
    try:
        opts = {k: v for k, v in req.items() if k not in ("catalog", "schema_name")}
        return generate_checks_for_schema(client, wid, config, req.get("catalog", ""), req.get("schema_name", ""), options=opts)
    except Exception as e:
        return {"error": str(e)}


@router.post("/dqx/profile-catalog")
async def dqx_profile_catalog_endpoint(req: dict, client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Profile all tables in a catalog and auto-generate DQX checks."""
    _ensure_spark(creds[0], creds[1], client)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import generate_checks_for_catalog
    try:
        opts = {k: v for k, v in req.items() if k not in ("catalog", "exclude_schemas")}
        return generate_checks_for_catalog(client, wid, config, req.get("catalog", ""), req.get("exclude_schemas", ["information_schema"]), options=opts)
    except Exception as e:
        return {"error": str(e)}


@router.post("/dqx/profile-stream")
async def dqx_profile_stream_endpoint(req: dict, request: Request, client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """SSE endpoint that streams live profiling progress as tables are processed."""
    _ensure_spark(creds[0], creds[1], client)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    scope = req.get("scope", "schema")
    catalog = req.get("catalog", "")
    schema_name = req.get("schema_name", "")
    opts = {k: v for k, v in req.items() if k not in ("catalog", "schema_name", "scope", "exclude_schemas")}

    log_queue: queue.Queue = queue.Queue()

    def on_progress(event):
        log_queue.put(event)

    def run_profiling():
        try:
            if scope == "catalog":
                from src.dqx_engine import generate_checks_for_catalog
                result = generate_checks_for_catalog(client, wid, config, catalog,
                    req.get("exclude_schemas", ["information_schema"]), options=opts, on_progress=on_progress)
            else:
                from src.dqx_engine import generate_checks_for_schema
                result = generate_checks_for_schema(client, wid, config, catalog, schema_name, options=opts, on_progress=on_progress)
            log_queue.put({"type": "complete", "result": result})
        except Exception as e:
            log_queue.put({"type": "error", "error": str(e)})

    worker = threading.Thread(target=run_profiling, daemon=True)
    worker.start()

    async def event_generator():
        import asyncio
        loop = asyncio.get_event_loop()
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: log_queue.get(timeout=1.0)),
                    timeout=1.5,
                )
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("complete", "error"):
                    break
            except (asyncio.TimeoutError, queue.Empty):
                # Send keepalive to prevent timeout
                yield ": keepalive\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                                      "Connection": "keep-alive", "Content-Type": "text/event-stream"})


@router.post("/dqx/checks")
async def dqx_create_check_endpoint(req: DQXCheckCreate, client=Depends(get_db_client)):
    """Create a DQX check manually."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import create_check
    return create_check(client, wid, config, req.model_dump())


@router.get("/dqx/checks")
async def dqx_list_checks_endpoint(table_fqn: str = "", client=Depends(get_db_client)):
    """List DQX checks, optionally filtered by table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import list_checks
    return list_checks(client, wid, config, table_fqn)


@router.delete("/dqx/checks/{check_id}")
async def dqx_delete_check_endpoint(check_id: str, client=Depends(get_db_client)):
    """Delete a DQX check."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import delete_check
    delete_check(client, wid, config, check_id)
    return {"status": "deleted"}


@router.post("/dqx/checks/delete-bulk")
async def dqx_delete_bulk_endpoint(req: dict, client=Depends(get_db_client)):
    """Delete multiple DQX checks by IDs, or all checks for a table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import delete_checks_bulk
    check_ids = req.get("check_ids", [])
    table_fqn = req.get("table_fqn", "")
    delete_all = req.get("delete_all", False)
    return delete_checks_bulk(client, wid, config, check_ids, table_fqn, delete_all)


@router.post("/dqx/clear-all")
async def dqx_clear_all_endpoint(client=Depends(get_db_client)):
    """Clear ALL DQX data — checks, profiles, run results, definitions."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import clear_all_dqx_data
    return clear_all_dqx_data(client, wid, config)


@router.post("/dqx/checks/{check_id}/toggle")
async def dqx_toggle_check_endpoint(check_id: str, req: dict, client=Depends(get_db_client)):
    """Enable or disable a DQX check."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import toggle_check
    toggle_check(client, wid, config, check_id, req.get("enabled", True))
    return {"status": "toggled", "enabled": req.get("enabled", True)}


@router.put("/dqx/checks/{check_id}")
async def dqx_update_check_endpoint(check_id: str, req: dict, client=Depends(get_db_client)):
    """Update a DQX check (name, criticality, arguments, filter)."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import update_check
    return update_check(client, wid, config, check_id, req)


@router.post("/dqx/run")
async def dqx_run_endpoint(req: DQXRunRequest, client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Execute DQX checks on a table."""
    _ensure_spark(creds[0], creds[1], client)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import run_checks
    return run_checks(client, wid, config, req.table_fqn, req.check_ids or None)


@router.get("/dqx/results")
async def dqx_results_endpoint(table_fqn: str = "", limit: int = 50, client=Depends(get_db_client)):
    """List DQX run results."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import list_run_results
    return list_run_results(client, wid, config, table_fqn, limit)


@router.post("/dqx/run-all")
async def dqx_run_all_endpoint(client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Run DQX checks for all monitored tables."""
    _ensure_spark(creds[0], creds[1], client)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import run_all_checks
    try:
        return run_all_checks(client, wid, config)
    except Exception as e:
        return {"error": str(e)}


@router.get("/dqx/checks/export")
async def dqx_export_checks_endpoint(table_fqn: str = "", client=Depends(get_db_client)):
    """Export DQX checks as YAML."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import export_checks_yaml
    yaml_content = export_checks_yaml(client, wid, config, table_fqn)
    return Response(content=yaml_content, media_type="text/yaml",
                    headers={"Content-Disposition": "attachment; filename=dqx-checks.yaml"})


@router.post("/dqx/checks/import")
async def dqx_import_checks_endpoint(req: dict, client=Depends(get_db_client)):
    """Import DQX checks from YAML."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import import_checks_yaml
    return import_checks_yaml(client, wid, config, req.get("table_fqn", ""), req.get("yaml_content", ""))


@router.post("/dqx/checks/save-to-delta")
async def dqx_save_checks_to_delta_endpoint(req: dict, client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Save DQX checks to a user-specified Delta table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import save_checks_to_delta
    return save_checks_to_delta(
        client, wid, config,
        target_table=req.get("target_table", ""),
        table_fqn=req.get("table_fqn", ""),
        user=creds[0] if creds else "",
    )


@router.get("/dqx/profiles")
async def dqx_profiles_endpoint(table_fqn: str = "", client=Depends(get_db_client)):
    """List DQX profiles."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.dqx_engine import list_profiles
    return list_profiles(client, wid, config, table_fqn)


# ---------------------------------------------------------------------------
# Change History
# ---------------------------------------------------------------------------

@router.get("/changes")
async def get_changes(entity_type: str = "", limit: int = 100, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.governance import get_change_history
    return get_change_history(client, wid, config, entity_type, limit)
