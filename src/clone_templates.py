"""Clone templates — predefined recipes for common clone scenarios."""

import json
import logging
import os

import yaml

logger = logging.getLogger(__name__)

# Built-in templates
TEMPLATES = {
    "dev-copy": {
        "name": "Development Copy",
        "description": "Shallow clone with PII masking — fast, safe dev/test environment",
        "config": {
            "clone_type": "SHALLOW",
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": True,
            "copy_security": False,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "show_progress": True,
            "dry_run": False,
            "masking_rules": [
                {"column": "email", "method": "email_mask"},
                {"column": "phone", "method": "partial"},
                {"column": "ssn", "method": "hash"},
                {"column": "first_name", "method": "redact"},
                {"column": "last_name", "method": "redact"},
                {"column": "address", "method": "redact"},
                {"column": "credit_card", "method": "hash"},
            ],
        },
    },
    "dr-backup": {
        "name": "Disaster Recovery Backup",
        "description": "Full deep clone with validation and checksums — complete backup",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": True,
            "copy_ownership": True,
            "copy_tags": True,
            "copy_security": True,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "validate_checksum": True,
            "show_progress": True,
            "max_workers": 8,
        },
    },
    "test-refresh": {
        "name": "Test Environment Refresh",
        "description": "Deep clone with data sampling — representative test data, smaller footprint",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": True,
            "copy_security": False,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "show_progress": True,
        },
    },
    "staging-promote": {
        "name": "Staging to Production Promote",
        "description": "Deep clone from staging to production with full validation",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": True,
            "copy_ownership": True,
            "copy_tags": True,
            "copy_security": True,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "validate_checksum": True,
            "show_progress": True,
            "max_workers": 4,
            "clone_policies": [
                {"name": "require_validation", "value": True, "severity": "error"},
                {"name": "require_rollback", "value": True, "severity": "error"},
            ],
        },
    },
    "incremental-sync": {
        "name": "Incremental Sync",
        "description": "Sync only changed data since last clone using Delta CDC",
        "config": {
            "clone_type": "DEEP",
            "load_type": "INCREMENTAL",
            "copy_permissions": False,
            "copy_tags": False,
            "copy_security": False,
            "enable_rollback": False,
            "validate_after_clone": True,
            "show_progress": True,
        },
    },
    "schema-only": {
        "name": "Schema Only (No Data)",
        "description": "Clone structure only — schemas, tables (empty), views, functions",
        "config": {
            "clone_type": "SHALLOW",
            "copy_permissions": True,
            "copy_ownership": True,
            "copy_tags": True,
            "copy_security": True,
            "copy_constraints": True,
            "copy_comments": True,
            "show_progress": True,
        },
    },
    "cross-workspace": {
        "name": "Cross-Workspace Clone",
        "description": "Clone to a different Databricks workspace",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": True,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "show_progress": True,
        },
        "requires": ["dest_host", "dest_token", "dest_warehouse_id"],
    },
    "dev-refresh": {
        "name": "Dev Refresh",
        "description": "Shallow clone, no permissions/ownership — fast dev environment refresh",
        "config": {
            "clone_type": "SHALLOW",
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": False,
            "copy_security": False,
            "copy_constraints": False,
            "copy_comments": False,
            "enable_rollback": False,
            "validate_after_clone": False,
            "show_progress": True,
            "max_workers": 8,
        },
    },
    "dr-replica": {
        "name": "Disaster Recovery Replica",
        "description": "Deep clone with checksums, full permissions, rollback enabled",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": True,
            "copy_ownership": True,
            "copy_tags": True,
            "copy_security": True,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "validate_checksum": True,
            "show_progress": True,
            "max_workers": 8,
            "auto_rollback_on_failure": True,
            "rollback_threshold": 1.0,
        },
    },
    "audit-copy": {
        "name": "Audit Copy",
        "description": "Deep clone with audit trail enabled — read-only focus",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": True,
            "copy_ownership": False,
            "copy_tags": True,
            "copy_security": True,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "show_progress": True,
        },
    },
    "pii-safe": {
        "name": "PII-Safe Clone",
        "description": "Deep clone with comprehensive PII masking rules",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": True,
            "copy_security": False,
            "copy_constraints": True,
            "copy_comments": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "show_progress": True,
            "masking_rules": [
                {"column": "email", "method": "email_mask"},
                {"column": "phone", "method": "partial"},
                {"column": "ssn", "method": "hash"},
                {"column": "first_name", "method": "redact"},
                {"column": "last_name", "method": "redact"},
                {"column": "address", "method": "redact"},
                {"column": "credit_card", "method": "hash"},
                {"column": "date_of_birth", "method": "redact"},
                {"column": "passport", "method": "hash"},
                {"column": "driver_license", "method": "hash"},
                {"column": "bank_account", "method": "hash"},
                {"column": "ip_address", "method": "hash"},
            ],
        },
    },
    "minimal": {
        "name": "Minimal Clone",
        "description": "Shallow clone, minimal settings — fastest possible clone",
        "config": {
            "clone_type": "SHALLOW",
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": False,
            "copy_security": False,
            "copy_constraints": False,
            "copy_comments": False,
            "enable_rollback": False,
            "validate_after_clone": False,
            "show_progress": False,
            "max_workers": 4,
        },
    },
    "full-mirror": {
        "name": "Full Mirror",
        "description": "Deep clone with EVERYTHING enabled — complete mirror of source",
        "config": {
            "clone_type": "DEEP",
            "copy_permissions": True,
            "copy_ownership": True,
            "copy_tags": True,
            "copy_security": True,
            "copy_constraints": True,
            "copy_comments": True,
            "copy_properties": True,
            "enable_rollback": True,
            "validate_after_clone": True,
            "validate_checksum": True,
            "show_progress": True,
            "max_workers": 8,
            "auto_rollback_on_failure": True,
            "rollback_threshold": 2.0,
        },
    },
}


def list_templates() -> list[dict]:
    """List all available clone templates."""
    templates = []
    for key, tmpl in TEMPLATES.items():
        templates.append({
            "key": key,
            "name": tmpl["name"],
            "description": tmpl["description"],
            "clone_type": tmpl["config"].get("clone_type", "DEEP"),
        })

    logger.info("Available clone templates:")
    logger.info("-" * 60)
    for t in templates:
        logger.info(f"  {t['key']:20s} — {t['name']}")
        logger.info(f"  {'':20s}   {t['description']}")
        logger.info(f"  {'':20s}   Clone type: {t['clone_type']}")
        logger.info("")

    return templates


def get_template(template_name: str) -> dict | None:
    """Get a template by name."""
    return TEMPLATES.get(template_name)


def apply_template(
    base_config: dict,
    template_name: str,
    overrides: dict | None = None,
) -> dict:
    """Apply a template to a base config, with optional overrides.

    Template values override base config.
    Override values override template values.

    Returns:
        Merged configuration dict.
    """
    template = TEMPLATES.get(template_name)
    if not template:
        raise ValueError(f"Unknown template: {template_name}. Available: {', '.join(TEMPLATES.keys())}")

    # Start with base config
    merged = {**base_config}

    # Apply template config
    merged.update(template["config"])

    # Apply overrides
    if overrides:
        merged.update(overrides)

    logger.info(f"Applied template: {template['name']} ({template_name})")
    return merged


def export_template(template_name: str, output_path: str | None = None) -> str:
    """Export a template as a standalone YAML config file.

    Returns:
        Path to the generated config file.
    """
    template = TEMPLATES.get(template_name)
    if not template:
        raise ValueError(f"Unknown template: {template_name}")

    config = {
        "# Template": f"{template['name']} — {template['description']}",
        "source_catalog": "<your-source-catalog>",
        "destination_catalog": "<your-dest-catalog>",
        "sql_warehouse_id": "<your-warehouse-id>",
        **template["config"],
        "exclude_schemas": [],
        "max_workers": template["config"].get("max_workers", 4),
    }

    output = output_path or f"config/template_{template_name}.yaml"
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)

    with open(output, "w") as f:
        f.write(f"# Clone Template: {template['name']}\n")
        f.write(f"# {template['description']}\n")
        f.write(f"# Generated by clone-catalog template export\n\n")
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Template exported to: {output}")
    return output


def load_user_templates(path: str) -> dict:
    """Load user-defined templates from a YAML file.

    The YAML should have the same structure as TEMPLATES dict:
    template_key:
      name: "..."
      description: "..."
      config:
        clone_type: DEEP
        ...
    """
    if not os.path.exists(path):
        logger.debug(f"User templates file not found: {path}")
        return {}

    with open(path) as f:
        user_templates = yaml.safe_load(f) or {}

    logger.info(f"Loaded {len(user_templates)} user templates from {path}")
    return user_templates


def get_all_templates(user_templates_path: str | None = None) -> dict:
    """Return merged built-in + user templates. User templates override built-in ones."""
    all_templates = dict(TEMPLATES)
    if user_templates_path:
        user = load_user_templates(user_templates_path)
        all_templates.update(user)
    return all_templates
