"""Tests for src/federation.py:register_iceberg_rest_catalog.

Covers the SQL emission, idempotency, and SQL-injection escaping so
operator-supplied URIs / catalog names can't smuggle DDL through the
single-quote boundary.
"""

from unittest.mock import MagicMock, patch


def _client_with_existing(catalogs: list[str]) -> MagicMock:
    """Fake WorkspaceClient whose `catalogs.list()` returns the named
    catalogs (each as an SDK-shaped object with a `.name` attribute).

    Note: ``MagicMock(name=...)`` sets the mock's repr, not an attribute.
    To stub the SDK's ``CatalogInfo.name`` we have to assign the
    attribute after construction.
    """
    client = MagicMock()
    items = [MagicMock() for _ in catalogs]
    for mock_cat, n in zip(items, catalogs):
        mock_cat.name = n
    client.catalogs.list.return_value = items
    return client


def test_register_iceberg_rest_emits_create_foreign_catalog_sql():
    """Happy path — when the catalog doesn't exist yet, the helper
    emits a single ``CREATE FOREIGN CATALOG ... USING ICEBERG OPTIONS
    (...)`` statement with the operator's URI / warehouse / credential
    embedded as quoted option values."""
    from src.federation import register_iceberg_rest_catalog

    client = _client_with_existing([])
    with patch("src.federation.execute_sql") as mock_sql:
        result = register_iceberg_rest_catalog(
            client,
            "wh-1",
            "polaris_main",
            uri="https://polaris.example.com/v1",
            warehouse="my_wh",
            credential="scope.key",
        )
    assert result == {"name": "polaris_main", "created": True}
    assert mock_sql.call_count == 1
    sql = mock_sql.call_args.args[2]
    assert "CREATE FOREIGN CATALOG `polaris_main`" in sql
    assert "USING ICEBERG" in sql
    assert "'uri' = 'https://polaris.example.com/v1'" in sql
    assert "'warehouse' = 'my_wh'" in sql
    assert "'credential' = 'scope.key'" in sql


def test_register_iceberg_rest_includes_comment_when_provided():
    """Optional `comment` becomes a `COMMENT '...'` clause appended
    after OPTIONS."""
    from src.federation import register_iceberg_rest_catalog

    client = _client_with_existing([])
    with patch("src.federation.execute_sql") as mock_sql:
        register_iceberg_rest_catalog(
            client,
            "wh-1",
            "polaris_main",
            uri="https://polaris.example.com/v1",
            warehouse="my_wh",
            credential="scope.key",
            comment="Production Polaris catalog",
        )
    sql = mock_sql.call_args.args[2]
    assert "COMMENT 'Production Polaris catalog'" in sql


def test_register_iceberg_rest_idempotent_when_catalog_exists():
    """If the catalog name is already in `client.catalogs.list()`,
    the helper returns `created=False` with an error message and
    does NOT emit any SQL (no overwrite, no rebind)."""
    from src.federation import register_iceberg_rest_catalog

    client = _client_with_existing(["polaris_main", "other_cat"])
    with patch("src.federation.execute_sql") as mock_sql:
        result = register_iceberg_rest_catalog(
            client,
            "wh-1",
            "polaris_main",
            uri="https://polaris.example.com/v1",
            warehouse="my_wh",
            credential="scope.key",
        )
    assert result["created"] is False
    assert "already exists" in result["error"]
    assert mock_sql.call_count == 0


def test_register_iceberg_rest_escapes_single_quotes_in_options():
    """Defence in depth — operator-supplied URIs shouldn't contain
    single quotes, but if one slips in, it's escaped (`''`) so it
    can't terminate the OPTION value early. The SQL test pins the
    escape pattern."""
    from src.federation import register_iceberg_rest_catalog

    client = _client_with_existing([])
    with patch("src.federation.execute_sql") as mock_sql:
        register_iceberg_rest_catalog(
            client,
            "wh-1",
            "polaris_main",
            uri="https://example.com/v1?token='injected",
            warehouse="my_wh",
            credential="scope.key",
        )
    sql = mock_sql.call_args.args[2]
    # The injected single quote is doubled to ''.
    assert "https://example.com/v1?token=''injected" in sql


def test_register_iceberg_rest_returns_error_when_create_fails():
    """If `execute_sql` raises (most commonly: missing privilege,
    unreachable endpoint), the helper catches it and returns a
    structured `{created: False, error: ...}` so the API endpoint
    can map to a 400 with the message embedded."""
    from src.federation import register_iceberg_rest_catalog

    client = _client_with_existing([])
    with patch("src.federation.execute_sql", side_effect=RuntimeError("PERMISSION_DENIED")):
        result = register_iceberg_rest_catalog(
            client,
            "wh-1",
            "polaris_main",
            uri="https://polaris.example.com/v1",
            warehouse="my_wh",
            credential="scope.key",
        )
    assert result["created"] is False
    assert "PERMISSION_DENIED" in result["error"]


def test_register_iceberg_rest_continues_when_catalogs_list_errors():
    """If the preflight `catalogs.list()` errors (transient SDK
    failure, perms), the helper still attempts the CREATE — the
    underlying CREATE will surface a clearer error than blocking
    on a transient list failure."""
    from src.federation import register_iceberg_rest_catalog

    client = MagicMock()
    client.catalogs.list.side_effect = RuntimeError("transient SDK error")
    with patch("src.federation.execute_sql") as mock_sql:
        result = register_iceberg_rest_catalog(
            client,
            "wh-1",
            "polaris_main",
            uri="https://polaris.example.com/v1",
            warehouse="my_wh",
            credential="scope.key",
        )
    # The CREATE still ran exactly once.
    assert mock_sql.call_count == 1
    assert result["created"] is True
