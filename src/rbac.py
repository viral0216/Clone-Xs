"""Role-based access control for clone operations."""

import logging
import os
import re
from dataclasses import dataclass, field

import yaml

logger = logging.getLogger(__name__)


@dataclass
class RbacRule:
    """A single RBAC rule."""
    principals: list[str]
    allowed_sources: list[str] = field(default_factory=lambda: [".*"])
    allowed_destinations: list[str] = field(default_factory=lambda: [".*"])
    allowed_schemas: list[str] = field(default_factory=lambda: [".*"])
    deny: bool = False


def load_rbac_policy(policy_path: str) -> list[RbacRule]:
    """Load RBAC policy from YAML file.

    YAML format:
    rules:
      - principals: ["admin@company.com"]
        allowed_sources: [".*"]
        allowed_destinations: [".*"]
      - principals: ["*"]
        allowed_destinations: ["prod_.*"]
        deny: true
    """
    path = os.path.expanduser(policy_path)
    if not os.path.exists(path):
        logger.debug(f"RBAC policy file not found: {path}")
        return []

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    rules = []
    for rule_data in data.get("rules", []):
        rules.append(RbacRule(
            principals=rule_data.get("principals", []),
            allowed_sources=rule_data.get("allowed_sources", [".*"]),
            allowed_destinations=rule_data.get("allowed_destinations", [".*"]),
            allowed_schemas=rule_data.get("allowed_schemas", [".*"]),
            deny=rule_data.get("deny", False),
        ))

    logger.info(f"Loaded {len(rules)} RBAC rules from {path}")
    return rules


def get_current_user(client) -> str:
    """Get current user identity via Databricks SDK."""
    try:
        me = client.current_user.me()
        return me.user_name or me.display_name or "unknown"
    except Exception as e:
        logger.warning(f"Could not determine current user: {e}")
        return "unknown"


def _matches_any(value: str, patterns: list[str]) -> bool:
    """Check if value matches any of the regex patterns."""
    for pattern in patterns:
        if re.fullmatch(pattern, value):
            return True
    return False


def _principal_matches(user: str, principals: list[str]) -> bool:
    """Check if user matches any principal pattern."""
    for principal in principals:
        if principal == "*":
            return True
        if principal.lower() == user.lower():
            return True
        if re.fullmatch(principal, user, re.IGNORECASE):
            return True
    return False


def check_permission(
    policy: list[RbacRule],
    user: str,
    source_catalog: str,
    dest_catalog: str,
    schemas: list[str] | None = None,
) -> dict:
    """Evaluate policy rules against the requested operation.

    Returns dict with:
      - allowed: bool
      - reason: str
      - matched_rule: int | None (index of the matching rule)
    """
    if not policy:
        return {"allowed": True, "reason": "No RBAC policy configured", "matched_rule": None}

    # Process deny rules first, then allow rules
    for i, rule in enumerate(policy):
        if not _principal_matches(user, rule.principals):
            continue

        if rule.deny:
            # Check if operation matches deny pattern
            source_match = _matches_any(source_catalog, rule.allowed_sources)
            dest_match = _matches_any(dest_catalog, rule.allowed_destinations)
            if source_match or dest_match:
                return {
                    "allowed": False,
                    "reason": f"Denied by rule {i + 1}: "
                              f"user '{user}' is denied access to "
                              f"source='{source_catalog}' or dest='{dest_catalog}'",
                    "matched_rule": i,
                }

    # Check allow rules
    for i, rule in enumerate(policy):
        if rule.deny:
            continue
        if not _principal_matches(user, rule.principals):
            continue

        source_match = _matches_any(source_catalog, rule.allowed_sources)
        dest_match = _matches_any(dest_catalog, rule.allowed_destinations)

        if source_match and dest_match:
            # Check schema restrictions if applicable
            if schemas and rule.allowed_schemas != [".*"]:
                for schema in schemas:
                    if not _matches_any(schema, rule.allowed_schemas):
                        return {
                            "allowed": False,
                            "reason": f"Schema '{schema}' not allowed by rule {i + 1}",
                            "matched_rule": i,
                        }
            return {
                "allowed": True,
                "reason": f"Allowed by rule {i + 1}",
                "matched_rule": i,
            }

    return {
        "allowed": False,
        "reason": f"No matching allow rule for user '{user}'",
        "matched_rule": None,
    }


def enforce_rbac(client, config: dict) -> None:
    """Load policy, get user, check permission. Raises PermissionError if denied."""
    policy_path = config.get("rbac_policy_path", "~/.clone-xs/rbac_policy.yaml")
    policy = load_rbac_policy(policy_path)

    if not policy:
        logger.debug("No RBAC policy found, allowing operation")
        return

    user = get_current_user(client)
    source = config.get("source_catalog", "")
    dest = config.get("destination_catalog", "")

    result = check_permission(policy, user, source, dest)

    if not result["allowed"]:
        raise PermissionError(f"RBAC denied: {result['reason']}")

    logger.info(f"RBAC check passed for user '{user}': {result['reason']}")


def print_policy(policy: list[RbacRule]) -> None:
    """Pretty-print the loaded RBAC policy."""
    if not policy:
        print("No RBAC policy configured.")
        return

    print("=" * 60)
    print("RBAC POLICY")
    print("=" * 60)
    for i, rule in enumerate(policy):
        rule_type = "DENY" if rule.deny else "ALLOW"
        print(f"\n  Rule {i + 1} ({rule_type}):")
        print(f"    Principals:    {', '.join(rule.principals)}")
        print(f"    Sources:       {', '.join(rule.allowed_sources)}")
        print(f"    Destinations:  {', '.join(rule.allowed_destinations)}")
        if rule.allowed_schemas != [".*"]:
            print(f"    Schemas:       {', '.join(rule.allowed_schemas)}")
    print("=" * 60)


def print_user_permissions(policy: list[RbacRule], user: str) -> None:
    """Show what the given user is allowed to do."""
    print(f"\nPermissions for user: {user}")
    print("-" * 40)

    matching_rules = []
    for i, rule in enumerate(policy):
        if _principal_matches(user, rule.principals):
            matching_rules.append((i, rule))

    if not matching_rules:
        print("  No matching rules — access denied by default")
        return

    for i, rule in matching_rules:
        rule_type = "DENY" if rule.deny else "ALLOW"
        print(f"  Rule {i + 1} ({rule_type}):")
        print(f"    Sources:      {', '.join(rule.allowed_sources)}")
        print(f"    Destinations: {', '.join(rule.allowed_destinations)}")
