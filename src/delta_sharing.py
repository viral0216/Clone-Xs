"""Delta Sharing: manage shares, recipients, and grants."""

import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def list_shares(client: WorkspaceClient) -> list[dict]:
    """List all Delta Sharing shares."""
    results = []
    try:
        shares = client.shares.list()
        for s in shares:
            results.append({
                "name": s.name,
                "comment": s.comment,
                "owner": s.owner,
                "created_at": str(s.created_at) if s.created_at else None,
                "updated_at": str(s.updated_at) if s.updated_at else None,
            })
    except Exception as e:
        logger.error(f"Failed to list shares: {e}")
    return results


def get_share_details(client: WorkspaceClient, share_name: str) -> dict | None:
    """Get share details including shared objects."""
    try:
        share = client.shares.get(name=share_name, include_shared_data=True)
        objects = []
        for obj in (share.objects or []):
            objects.append({
                "name": obj.name,
                "data_object_type": str(obj.data_object_type) if obj.data_object_type else None,
                "comment": obj.comment,
                "status": str(obj.status) if obj.status else None,
                "added_at": str(obj.added_at) if obj.added_at else None,
                "added_by": obj.added_by,
                "shared_as": obj.shared_as,
            })
        return {
            "name": share.name,
            "comment": share.comment,
            "owner": share.owner,
            "objects": objects,
            "created_at": str(share.created_at) if share.created_at else None,
        }
    except Exception as e:
        logger.error(f"Failed to get share {share_name}: {e}")
        return None


def create_share(
    client: WorkspaceClient, name: str, comment: str = "",
) -> dict:
    """Create a new Delta Sharing share."""
    result = {"name": name, "success": False}
    try:
        client.shares.create(name=name, comment=comment)
        result["success"] = True
        logger.info(f"Created share: {name}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create share {name}: {e}")
    return result


def grant_table_to_share(
    client: WorkspaceClient, share_name: str, table_fqn: str,
    shared_as: str | None = None,
) -> dict:
    """Grant a table to a share."""
    result = {"share": share_name, "table": table_fqn, "success": False}
    try:
        from databricks.sdk.service.sharing import SharedDataObject, SharedDataObjectDataObjectType

        updates = [
            {
                "action": "ADD",
                "data_object": SharedDataObject(
                    name=table_fqn,
                    data_object_type=SharedDataObjectDataObjectType.TABLE,
                    shared_as=shared_as,
                ),
            }
        ]
        client.shares.update(name=share_name, updates=updates)
        result["success"] = True
        logger.info(f"Granted {table_fqn} to share {share_name}")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to grant {table_fqn} to share {share_name}: {e}")
    return result


def revoke_table_from_share(
    client: WorkspaceClient, share_name: str, table_fqn: str,
) -> dict:
    """Revoke a table from a share."""
    result = {"share": share_name, "table": table_fqn, "success": False}
    try:
        from databricks.sdk.service.sharing import SharedDataObject, SharedDataObjectDataObjectType

        updates = [
            {
                "action": "REMOVE",
                "data_object": SharedDataObject(
                    name=table_fqn,
                    data_object_type=SharedDataObjectDataObjectType.TABLE,
                ),
            }
        ]
        client.shares.update(name=share_name, updates=updates)
        result["success"] = True
        logger.info(f"Revoked {table_fqn} from share {share_name}")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to revoke {table_fqn} from share {share_name}: {e}")
    return result


def list_recipients(client: WorkspaceClient) -> list[dict]:
    """List all Delta Sharing recipients."""
    results = []
    try:
        recipients = client.recipients.list()
        for r in recipients:
            results.append({
                "name": r.name,
                "comment": r.comment,
                "owner": r.owner,
                "authentication_type": str(r.authentication_type) if r.authentication_type else None,
                "sharing_code": getattr(r, "sharing_code", None),
                "created_at": str(r.created_at) if r.created_at else None,
                "updated_at": str(r.updated_at) if r.updated_at else None,
                "activated": getattr(r, "activated", None),
            })
    except Exception as e:
        logger.error(f"Failed to list recipients: {e}")
    return results


def create_recipient(
    client: WorkspaceClient, name: str,
    comment: str = "",
    authentication_type: str = "TOKEN",
    sharing_code: str | None = None,
) -> dict:
    """Create a new Delta Sharing recipient."""
    result = {"name": name, "success": False}
    try:
        from databricks.sdk.service.sharing import AuthenticationType

        auth_type = AuthenticationType(authentication_type) if authentication_type else AuthenticationType.TOKEN
        recipient = client.recipients.create(
            name=name,
            comment=comment,
            authentication_type=auth_type,
            sharing_code=sharing_code,
        )

        result["success"] = True
        result["activation_url"] = getattr(recipient, "activation_url", None)
        logger.info(f"Created recipient: {name}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create recipient {name}: {e}")
    return result


def grant_share_to_recipient(
    client: WorkspaceClient, share_name: str, recipient_name: str,
) -> dict:
    """Grant a share to a recipient."""
    result = {"share": share_name, "recipient": recipient_name, "success": False}
    try:
        from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType

        client.grants.update(
            securable_type=SecurableType.SHARE,
            full_name=share_name,
            changes=[
                PermissionsChange(
                    add=[Privilege.SELECT],
                    principal=recipient_name,
                )
            ],
        )
        result["success"] = True
        logger.info(f"Granted share {share_name} to recipient {recipient_name}")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Failed to grant share {share_name} to {recipient_name}: {e}")
    return result


def validate_share(
    client: WorkspaceClient, share_name: str,
) -> dict:
    """Validate that all shared objects in a share are accessible."""
    details = get_share_details(client, share_name)
    if not details:
        return {"share": share_name, "valid": False, "error": "Share not found"}

    valid = True
    issues = []
    for obj in details.get("objects", []):
        status = str(obj.get("status", "")).upper()
        if status not in ("ACTIVE", ""):
            valid = False
            issues.append({
                "object": obj["name"],
                "status": status,
                "issue": "Object is not in ACTIVE state",
            })

    return {
        "share": share_name,
        "valid": valid,
        "total_objects": len(details.get("objects", [])),
        "issues": issues,
    }
