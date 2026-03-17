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
        "long_description": "Creates a lightweight development copy using shallow clone for speed. Strips permissions and ownership so developers can freely modify data without affecting production access controls. Includes PII masking rules for email, phone, SSN, names, addresses, and credit cards — making the copy safe for development without exposing sensitive data. Tags and constraints are preserved so schema behavior matches production.",
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
        "long_description": "Creates a full deep clone with every safety feature enabled. Copies all permissions, ownership, tags, security policies, constraints, and comments. Runs checksum validation after clone to verify data integrity. Uses 8 parallel workers for maximum throughput. Ideal for creating a complete backup that can serve as a failover in case of data loss or corruption.",
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
        "long_description": "Creates a deep clone suited for QA and testing teams. Strips permissions and ownership so testers can work independently. Preserves tags, constraints, and comments to maintain schema documentation. Enables rollback and post-clone validation so the test environment can be quickly reverted if needed. Best for refreshing test environments on a regular schedule.",
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
        "long_description": "Production-grade promotion from staging to production. Copies everything including permissions, ownership, security policies, and tags. Enforces checksum validation and mandatory rollback via clone policies — the operation will fail if validation or rollback are disabled. Uses 4 workers for controlled throughput. Designed for CI/CD pipelines where staging data has been tested and approved for production release.",
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
        "long_description": "Syncs only tables that have changed since the last clone operation using Delta Change Data Capture (CDC). Skips permissions, tags, and security to focus purely on data synchronization. Rollback is disabled since this is an additive operation. Post-clone validation confirms data consistency. Best for keeping a destination catalog up-to-date with minimal compute and network cost.",
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
        "long_description": "Clones the catalog structure without any data — creates empty tables, views, and functions with the same schema definitions. Copies all metadata including permissions, ownership, tags, security policies, constraints, and comments. Useful for setting up new environments, creating sandbox catalogs for schema testing, or bootstrapping a destination before running incremental data loads.",
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
        "long_description": "Clones a catalog to a different Databricks workspace using deep clone. Permissions and ownership are stripped since they don't transfer across workspaces — you'll need to set up access controls in the destination workspace separately. Tags, constraints, and comments are preserved. Requires destination workspace host, token, and warehouse ID. Enables rollback and validation for safety across the network boundary.",
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
        "long_description": "The fastest way to refresh a development environment. Uses shallow clone (metadata-only reference to source data files) so there's no data copy — just pointer updates. Strips all metadata (permissions, ownership, tags, security, constraints, comments) for maximum speed. Disables rollback and validation since dev environments are ephemeral. Uses 8 parallel workers. Ideal for daily dev refreshes where speed matters more than completeness.",
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
        "long_description": "Creates a production-grade disaster recovery replica with the strictest safety settings. Deep clone with full data copy, all permissions, ownership, security policies, and checksums. Auto-rollback is enabled with a 1% failure threshold — if more than 1% of tables fail, the entire operation is automatically reverted. Uses 8 parallel workers for fast replication. Designed for critical DR scenarios where data integrity is non-negotiable.",
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
        "long_description": "Creates a read-only audit copy for compliance and regulatory purposes. Deep clone preserves all data, permissions, tags, security policies, constraints, and comments. Ownership is not copied — the audit copy is owned by the service account running the clone. Rollback and validation are enabled to ensure the audit copy is complete and accurate. Ideal for creating point-in-time snapshots for auditors or compliance teams.",
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
        "long_description": "Creates a deep clone with 12 PII masking rules applied to sensitive columns — emails are masked, phone numbers partially redacted, SSNs/passports/driver licenses/bank accounts/credit cards are hashed, and names/addresses/dates of birth are fully redacted. Permissions and ownership are stripped so the masked copy can be shared with analytics teams. Tags and constraints are preserved. Best for creating GDPR/CCPA-compliant copies for data science and analytics.",
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
        "long_description": "The absolute minimum clone configuration. Shallow clone with all metadata copying disabled — no permissions, ownership, tags, security, constraints, or comments. Rollback and validation are off. Progress reporting is disabled for zero overhead. Uses 4 workers. Completes in seconds for most catalogs. Use this when you just need the table structure and data references as fast as possible, and plan to configure metadata separately.",
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
        "long_description": "The most comprehensive clone template — enables every feature. Deep clone with full data copy, all permissions, ownership, tags, security policies, properties, constraints, and comments. Checksum validation ensures byte-level accuracy. Auto-rollback triggers if more than 2% of tables fail. Uses 8 parallel workers. This is the gold standard for creating an exact replica of a production catalog where nothing should differ between source and destination.",
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
            "config": tmpl.get("config", {}),
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
        f.write(f"# Generated by clxs template export\n\n")
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
