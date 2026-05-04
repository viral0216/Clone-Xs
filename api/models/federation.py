"""Federation request/response models."""

import re

from pydantic import BaseModel, Field, field_validator


# UC identifier — letters, digits, underscores; must start with a
# letter or underscore. Mirrors Databricks' identifier rules so the
# operator catches a bad name before it round-trips to the warehouse.
_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Secret reference: <scope>.<key>. Both segments follow Databricks
# secret naming rules (alphanumerics + hyphens / underscores).
_SECRET_REF_RE = re.compile(r"^[\w-]+\.[\w-]+$")


class ForeignTablesRequest(BaseModel):
    """Request to list tables in a foreign catalog."""

    catalog: str
    warehouse_id: str | None = None
    schema_filter: str | None = None


class ConnectionCloneRequest(BaseModel):
    """Request to clone a connection."""

    connection_name: str
    new_name: str | None = None
    credentials: dict | None = None
    dry_run: bool = False


class MigrateRequest(BaseModel):
    """Request to migrate a foreign table to managed Delta."""

    foreign_fqn: str
    dest_fqn: str
    warehouse_id: str | None = None
    dry_run: bool = False


class RegisterIcebergRestCatalogRequest(BaseModel):
    """Register an external Apache Iceberg REST catalog as a UC Foreign Catalog.

    Once registered, the catalog appears in the standard UC catalog
    browser and the convert-format dispatch reads its tables via the
    existing ``CONVERT TO DELTA`` strategy with no new code paths.
    The actual OAuth token / credential lives in Databricks Secrets;
    only the secret reference (``<scope>.<key>``) crosses this boundary.
    """

    name: str = Field(..., description="Single-segment UC catalog name (no dots).")
    uri: str = Field(..., description="HTTPS endpoint of the Iceberg REST catalog.")
    warehouse: str = Field(
        ..., description="Iceberg `warehouse` identifier the REST catalog uses to namespace tables."
    )
    credential: str = Field(..., description="Databricks secret reference, format `<scope>.<key>`.")
    comment: str | None = Field(
        default=None, description="Optional human-readable comment surfaced in DESCRIBE CATALOG."
    )
    warehouse_id: str | None = Field(
        default=None,
        description="SQL warehouse to execute CREATE FOREIGN CATALOG. Falls back to clone_config default.",
    )

    @field_validator("name")
    @classmethod
    def _name_is_uc_identifier(cls, v: str) -> str:
        v = v.strip()
        if not _UC_IDENTIFIER_RE.match(v):
            raise ValueError(
                f"name {v!r} is not a valid UC identifier — must start with a "
                f"letter or underscore and contain only letters, digits, and "
                f"underscores. Multi-part names (with dots) are not allowed."
            )
        return v

    @field_validator("uri")
    @classmethod
    def _uri_is_https(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("https://"):
            raise ValueError(
                f"uri {v!r} must use HTTPS — the Iceberg REST catalog credential "
                f"is sent on every request and HTTP would expose it on the wire."
            )
        return v

    @field_validator("credential")
    @classmethod
    def _credential_is_secret_ref(cls, v: str) -> str:
        v = v.strip()
        if not _SECRET_REF_RE.match(v):
            raise ValueError(
                f"credential {v!r} must be a Databricks secret reference in "
                f"`<scope>.<key>` format. Store the actual OAuth token / "
                f"credential in `databricks secrets put-secret <scope> <key>` "
                f"first, then pass the reference here so the secret never "
                f"reaches Clone-Xs."
            )
        return v

    @field_validator("warehouse")
    @classmethod
    def _warehouse_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError(
                "warehouse is required — REST catalogs use it to namespace "
                "tables, and the federation binding fails opaquely without it."
            )
        return v


class RegisterIcebergRestCatalogResponse(BaseModel):
    """Outcome of a register-Iceberg-REST request."""

    name: str
    created: bool
    error: str | None = None
    next_step: str | None = Field(
        default=None,
        description="Hint pointing the operator at the next action (e.g. 'Open /convert and pick the new catalog').",
    )
