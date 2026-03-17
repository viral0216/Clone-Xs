"""Config management endpoints."""

import yaml
from fastapi import APIRouter, HTTPException

from api.models.config import ConfigDiffRequest, ConfigUpdateRequest
from src.config import load_config

router = APIRouter()


@router.get("")
async def get_config(path: str = "config/clone_config.yaml", profile: str | None = None):
    """Load and return current config."""
    try:
        config = load_config(path, profile=profile)
        return config
    except (FileNotFoundError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("")
async def update_config(req: ConfigUpdateRequest):
    """Save config YAML to disk."""
    try:
        # Validate YAML
        yaml.safe_load(req.yaml_content)
        with open(req.path, "w") as f:
            f.write(req.yaml_content)
        return {"status": "saved", "path": req.path}
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}")


@router.post("/diff")
async def config_diff(req: ConfigDiffRequest):
    """Compare two configs (dicts or YAML/JSON strings)."""
    from src.config_diff import diff_config_dicts

    def _resolve(val):
        if isinstance(val, dict):
            return val
        try:
            return yaml.safe_load(val) or {}
        except Exception:
            return {}

    dict_a = _resolve(req.config_a)
    dict_b = _resolve(req.config_b)
    raw = diff_config_dicts(dict_a, dict_b)

    # Transform {added, removed, changed} → flat array for the UI
    differences = []
    for key, vals in raw.get("changed", {}).items():
        differences.append({"key": key, "value_a": vals["old"], "value_b": vals["new"], "changed": True})
    for key, val in raw.get("added", {}).items():
        differences.append({"key": key, "value_a": None, "value_b": val, "changed": True})
    for key, val in raw.get("removed", {}).items():
        differences.append({"key": key, "value_a": val, "value_b": None, "changed": True})
    differences.sort(key=lambda x: x["key"])
    return {"differences": differences}


@router.post("/audit")
async def save_audit_settings(req: dict):
    """Save audit trail settings to config YAML."""
    config_path = "config/clone_config.yaml"
    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}
        raw["audit_trail"] = {
            "catalog": req.get("catalog", "clone_audit"),
            "schema": req.get("schema", "logs"),
            "table": raw.get("audit_trail", {}).get("table", "clone_operations"),
        }
        raw["metrics_table"] = f"{req.get('catalog', 'clone_audit')}.metrics.clone_metrics"
        with open(config_path, "w") as f:
            yaml.dump(raw, f, default_flow_style=False, sort_keys=False)
        return {"status": "saved"}
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profiles")
async def list_profiles(path: str = "config/clone_config.yaml"):
    """List available config profiles."""
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
        profiles = raw.get("profiles", {})
        return {"profiles": list(profiles.keys())}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
