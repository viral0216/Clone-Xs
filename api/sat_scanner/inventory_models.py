"""SAT Scanner — Unity Catalog inventory data models.

Lightweight ``__slots__`` classes mirroring the ``models.py`` style (each with a
recursive ``to_dict()``).  These represent the *enumerated* UC object tree — a
full inventory with low-level detail — as opposed to the PASS/FAIL ``SATFinding``
objects produced by the scanner.

Each flat object carries its parent fully-qualified name so the tree flattens
trivially to relational Excel/Delta rows, while the nested lists give the JSON
export its hierarchy.
"""

from __future__ import annotations

from typing import Any


def _jsonable(v: Any) -> Any:
    """Recursively convert a value (model / list / dict / scalar) to JSON-able form."""
    if hasattr(v, "to_dict"):
        return v.to_dict()
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    if isinstance(v, tuple):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    return v


class _Slotted:
    """Mixin providing a recursive ``to_dict()`` over ``__slots__``."""
    __slots__ = ()

    def to_dict(self) -> dict:
        return {s: _jsonable(getattr(self, s)) for s in self.__slots__}


class UCColumn(_Slotted):
    """A single column on a UC table or view."""
    __slots__ = ("name", "type_text", "type_name", "position", "nullable", "comment",
                 "tags", "mask")

    def __init__(self, name="", type_text="", type_name="", position=None,
                 nullable=None, comment="", tags=None, mask=None):
        self.name = name
        self.type_text = type_text
        self.type_name = type_name
        self.position = position
        self.nullable = nullable
        self.comment = comment
        self.tags = tags or {}
        self.mask = mask          # UC column mask: {function_name, using_column_names} | None


class UCGrant(_Slotted):
    """A privilege assignment on a securable (one principal)."""
    __slots__ = ("securable_type", "full_name", "principal", "privileges", "inherited_from")

    def __init__(self, securable_type="", full_name="", principal="",
                 privileges=None, inherited_from=""):
        self.securable_type = securable_type
        self.full_name = full_name
        self.principal = principal
        self.privileges = privileges or []
        self.inherited_from = inherited_from


class UCTable(_Slotted):
    __slots__ = (
        "full_name", "catalog", "schema", "name", "table_type",
        "data_source_format", "storage_location", "owner", "comment",
        "created_at", "updated_at", "view_definition", "properties",
        "tags", "columns", "grants", "constraints", "row_filter", "monitor",
    )

    def __init__(self, full_name="", catalog="", schema="", name="", table_type="",
                 data_source_format="", storage_location="", owner="", comment="",
                 created_at=None, updated_at=None, view_definition="",
                 properties=None, tags=None, columns=None, grants=None,
                 constraints=None, row_filter=None, monitor=None):
        self.full_name = full_name
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.table_type = table_type
        self.data_source_format = data_source_format
        self.storage_location = storage_location
        self.owner = owner
        self.comment = comment
        self.created_at = created_at
        self.updated_at = updated_at
        self.view_definition = view_definition
        self.properties = properties or {}
        self.tags = tags or {}
        self.columns = columns or []        # list[UCColumn]
        self.grants = grants or []          # list[UCGrant]
        self.constraints = constraints or []  # list[dict] PK/FK/CHECK/NOT NULL
        self.row_filter = row_filter        # {function_name, input_column_names} | None
        self.monitor = monitor              # Lakehouse Monitoring config | None


class UCVolume(_Slotted):
    __slots__ = ("full_name", "catalog", "schema", "name", "volume_type",
                 "storage_location", "owner", "comment", "tags", "grants")

    def __init__(self, full_name="", catalog="", schema="", name="", volume_type="",
                 storage_location="", owner="", comment="", tags=None, grants=None):
        self.full_name = full_name
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.volume_type = volume_type
        self.storage_location = storage_location
        self.owner = owner
        self.comment = comment
        self.tags = tags or {}
        self.grants = grants or []


class UCFunction(_Slotted):
    __slots__ = ("full_name", "catalog", "schema", "name", "data_type",
                 "routine_body", "owner", "comment", "grants")

    def __init__(self, full_name="", catalog="", schema="", name="", data_type="",
                 routine_body="", owner="", comment="", grants=None):
        self.full_name = full_name
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.data_type = data_type
        self.routine_body = routine_body
        self.owner = owner
        self.comment = comment
        self.grants = grants or []


class UCModel(_Slotted):
    __slots__ = ("full_name", "catalog", "schema", "name", "owner", "comment",
                 "grants", "versions")

    def __init__(self, full_name="", catalog="", schema="", name="", owner="",
                 comment="", grants=None, versions=None):
        self.full_name = full_name
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.owner = owner
        self.comment = comment
        self.grants = grants or []
        self.versions = versions or []      # list[dict] registered-model versions


class UCSchema(_Slotted):
    __slots__ = ("full_name", "catalog", "name", "owner", "comment",
                 "properties", "tags", "grants",
                 "tables", "volumes", "functions", "models")

    def __init__(self, full_name="", catalog="", name="", owner="", comment="",
                 properties=None, tags=None, grants=None,
                 tables=None, volumes=None, functions=None, models=None):
        self.full_name = full_name
        self.catalog = catalog
        self.name = name
        self.owner = owner
        self.comment = comment
        self.properties = properties or {}
        self.tags = tags or {}
        self.grants = grants or []
        self.tables = tables or []          # list[UCTable]
        self.volumes = volumes or []        # list[UCVolume]
        self.functions = functions or []    # list[UCFunction]
        self.models = models or []          # list[UCModel]


class UCCatalog(_Slotted):
    __slots__ = ("name", "catalog_type", "owner", "comment", "storage_root",
                 "isolation_mode", "properties", "tags", "grants", "schemas", "bindings")

    def __init__(self, name="", catalog_type="", owner="", comment="", storage_root="",
                 isolation_mode="", properties=None, tags=None, grants=None, schemas=None,
                 bindings=None):
        self.name = name
        self.catalog_type = catalog_type
        self.owner = owner
        self.comment = comment
        self.storage_root = storage_root
        self.isolation_mode = isolation_mode
        self.properties = properties or {}
        self.tags = tags or {}
        self.grants = grants or []
        self.schemas = schemas or []        # list[UCSchema]
        self.bindings = bindings or []      # list[dict] catalog↔workspace bindings


class UCInventoryResult(_Slotted):
    """Top-level container for a workspace's Unity Catalog inventory + Azure infra."""
    __slots__ = (
        "workspace_url", "workspace_name", "scanned_at",
        "metastore", "metastore_grants",
        "catalogs",
        "external_locations", "storage_credentials", "service_credentials",
        "connections", "shares", "recipients", "providers",
        "errors", "stats", "azure",
    )

    def __init__(self, workspace_url="", workspace_name="", scanned_at=""):
        self.workspace_url = workspace_url
        self.workspace_name = workspace_name
        self.scanned_at = scanned_at
        self.metastore: dict = {}
        self.metastore_grants: list = []     # list[UCGrant]
        self.catalogs: list = []             # list[UCCatalog]
        self.external_locations: list = []   # list[dict] (raw UC payload + optional azure map)
        self.storage_credentials: list = []  # list[dict]
        self.service_credentials: list = []  # list[dict] (non-storage SERVICE credentials)
        self.connections: list = []          # list[dict]
        self.shares: list = []               # list[dict] (outbound Delta Sharing, with objects)
        self.recipients: list = []           # list[dict]
        self.providers: list = []            # list[dict] (inbound Delta Sharing providers)
        self.errors: list = []               # list[dict] {level, full_name, http_status, error}
        self.stats: dict = {}
        self.azure: dict = {}                # AzureInventory.to_dict() or {}

    def record_error(self, level: str, full_name: str, http_status: int, error: Any) -> None:
        self.errors.append({
            "level": level,
            "full_name": full_name,
            "http_status": http_status,
            "error": str(error) if error is not None else "",
        })
