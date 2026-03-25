import contextlib
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from src.metadata_cache import MISSING as _MISSING, metadata_cache as _cache

load_dotenv()

logger = logging.getLogger(__name__)

# Retry settings
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds
RETRY_BACKOFF_FACTOR = 2


class RateLimiter:
    """Thread-safe rate limiter using a token bucket algorithm."""

    def __init__(self, max_rps: float = 0):
        """Initialize rate limiter.

        Args:
            max_rps: Maximum requests per second. 0 = unlimited.
        """
        self.max_rps = max_rps
        self.lock = threading.Lock()
        self._last_time = 0.0

    def acquire(self) -> None:
        """Wait until a request is allowed."""
        if self.max_rps <= 0:
            return

        with self.lock:
            now = time.time()
            min_interval = 1.0 / self.max_rps
            elapsed = now - self._last_time

            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                time.sleep(sleep_time)

            self._last_time = time.time()


# Global rate limiter (configured via set_rate_limit)
_rate_limiter = RateLimiter(0)

# Pluggable SQL executor — when set, execute_sql() routes through this
# instead of the warehouse API. Used by serverless jobs to run via spark.sql().
_sql_executor = None

# Pluggable SQL capture hook — when set, called for every SQL statement
# (including dry-run writes) before execution. Used by plan/dry-run to
# capture write statements. Must accept a single SQL string.
_sql_capture = None

# Global parallelism for SQL queries
_max_parallel_queries = 10


def set_sql_executor(executor) -> None:
    """Set a custom SQL executor function.

    The executor must accept a single SQL string and return list[dict].
    Used by serverless jobs to route through spark.sql() instead of warehouse API.

    This works because execute_sql() checks the global _sql_executor on every call.
    All modules that do `from src.client import execute_sql` get the same function
    object, which reads _sql_executor at call time (not import time).

    Example (inside Databricks Runtime):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        def spark_executor(sql):
            return [row.asDict() for row in spark.sql(sql).collect()]

        from src.client import set_sql_executor
        set_sql_executor(spark_executor)
    """
    global _sql_executor
    _sql_executor = executor
    logger.info("Custom SQL executor configured (spark.sql mode)")


# Lock for thread-safe executor swapping in spark_connect_executor()
_executor_lock = threading.Lock()


@contextlib.contextmanager
def spark_connect_executor():
    """Context manager that routes execute_sql() through Spark Connect.

    Uses databricks-connect serverless to run SQL remotely — no SQL warehouse
    or Databricks job needed. Thread-safe: holds _executor_lock for the
    duration so concurrent Spark Connect jobs are serialized.
    """
    from src.spark_session import get_spark

    spark = get_spark()

    def _exec(sql):
        return [row.asDict() for row in spark.sql(sql).collect()]

    with _executor_lock:
        global _sql_executor
        prev = _sql_executor
        _sql_executor = _exec
        logger.info("Spark Connect executor active (serverless)")
        try:
            yield
        finally:
            _sql_executor = prev
            logger.info("Spark Connect executor removed, restored previous")


def set_sql_capture(capture_fn) -> None:
    """Set a capture hook called for every SQL statement (including dry-run writes).

    Used by the plan command to record write statements that would be executed.
    Pass None to disable capture.
    """
    global _sql_capture
    _sql_capture = capture_fn


def set_rate_limit(max_rps: float) -> None:
    """Configure global rate limiting for SQL execution."""
    global _rate_limiter
    _rate_limiter = RateLimiter(max_rps)
    if max_rps > 0:
        logger.info(f"Rate limiting enabled: {max_rps} requests/second")


def get_workspace_client(
    host: str | None = None,
    token: str | None = None,
    profile: str | None = None,
) -> WorkspaceClient:
    """Return an initialized Databricks WorkspaceClient.

    Delegates to src.auth.get_client() which handles multiple auth methods
    with caching. See src/auth.py for the full auth method priority chain.

    Args:
        host: Workspace URL (optional, auto-detected from env/profile).
        token: Personal access token (optional, auto-detected from env/profile).
        profile: Databricks CLI profile name (optional).
    """
    from src.auth import get_client
    return get_client(host, token, profile)


def execute_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    max_retries: int = MAX_RETRIES,
    dry_run: bool = False,
) -> list[dict]:
    """Execute a SQL statement via SQL warehouse or serverless compute.

    If warehouse_id is "SERVERLESS", routes through the serverless job API
    (spark.sql on serverless compute — no SQL warehouse needed).

    Includes retry logic with exponential backoff for transient failures.
    In dry_run mode, logs the SQL without executing write operations.
    """
    # Capture hook — fires for every statement, even dry-run writes
    if _sql_capture is not None:
        _sql_capture(sql)

    # In dry run mode, skip write operations but allow reads (SELECT, SHOW, DESCRIBE)
    if dry_run:
        sql_upper = sql.strip().upper()
        is_read = sql_upper.startswith(("SELECT", "SHOW", "DESCRIBE"))
        if not is_read:
            logger.info(f"[DRY RUN] Would execute: {sql}")
            return []

    # Route through pluggable executor (spark.sql in serverless jobs — no warehouse needed)
    if _sql_executor is not None:
        return _sql_executor(sql)

    # Guard: fail fast if no warehouse is configured (only when using warehouse-based execution)
    if not warehouse_id or not warehouse_id.strip():
        raise ValueError(
            "No SQL warehouse selected. Go to Settings → SQL Warehouses and select a running warehouse."
        )

    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            _rate_limiter.acquire()
            return _execute_sql_once(client, warehouse_id, sql)
        except RuntimeError:
            # SQL failures (syntax error, permission denied) — don't retry
            raise
        except Exception as e:
            err_msg = str(e).lower()
            # Permanent errors — don't retry (warehouse not found, auth failures, etc.)
            if any(phrase in err_msg for phrase in [
                "was not found", "not found", "does not exist",
                "permission denied", "unauthorized", "forbidden",
                "invalid warehouse", "no warehouse",
                "not a valid endpoint", "invalid endpoint",
            ]):
                logger.error(f"SQL execution failed (non-retryable): {e}")
                raise
            last_exception = e
            if attempt < max_retries:
                delay = RETRY_BASE_DELAY * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
                logger.warning(
                    f"SQL execution attempt {attempt}/{max_retries} failed: {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"SQL execution failed after {max_retries} attempts: {e}")

    raise last_exception


def _execute_sql_once(client: WorkspaceClient, warehouse_id: str, sql: str) -> list[dict]:
    """Execute a SQL statement once (no retries)."""
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )

    if response.status and response.status.state:
        state = response.status.state.value
        if state == "FAILED":
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {error_msg}\nSQL: {sql}")

        # Poll for RUNNING/PENDING states
        while state in ("RUNNING", "PENDING"):
            time.sleep(2)
            response = client.statement_execution.get_statement(response.statement_id)
            state = response.status.state.value

        if state == "FAILED":
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {error_msg}\nSQL: {sql}")

    # Parse results
    rows = []
    if response.result and response.result.data_array:
        columns = [col.name for col in response.manifest.schema.columns]
        for row in response.result.data_array:
            rows.append(dict(zip(columns, row)))

    return rows


# ──────────────────────────────────────────────────────────────────
# SDK-based metadata helpers — no SQL warehouse required
# ──────────────────────────────────────────────────────────────────

def list_schemas_sdk(client: WorkspaceClient, catalog: str, exclude: list[str] | None = None) -> list[str]:
    """List schema names in a catalog using the SDK (no SQL warehouse needed)."""
    key = ("schemas", catalog, frozenset(exclude or []))
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        schemas = [s.name for s in client.schemas.list(catalog_name=catalog) if s.name]
        if exclude:
            schemas = [s for s in schemas if s not in exclude]
        _cache.put(key, schemas)
        return schemas
    except Exception as e:
        logger.warning(f"SDK schemas.list failed for {catalog}: {e}")
        return []


def list_tables_sdk(client: WorkspaceClient, catalog: str, schema: str) -> list[dict]:
    """List tables in a schema using the SDK (no SQL warehouse needed).

    Returns list of dicts with: table_name, table_type, data_source_format.
    """
    key = ("tables", catalog, schema)
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        result = [
            {
                "table_name": t.name,
                "table_type": t.table_type.value if t.table_type else "UNKNOWN",
                "data_source_format": getattr(t, "data_source_format", None),
            }
            for t in client.tables.list(catalog_name=catalog, schema_name=schema)
            if t.name
        ]
        _cache.put(key, result)
        return result
    except Exception as e:
        logger.warning(f"SDK tables.list failed for {catalog}.{schema}: {e}")
        return []


def list_views_sdk(client: WorkspaceClient, catalog: str, schema: str) -> list[dict]:
    """List views in a schema using the SDK (no SQL warehouse needed).

    Returns list of dicts with: table_name, view_definition.
    """
    key = ("views", catalog, schema)
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        result = [
            {
                "table_name": t.name,
                "view_definition": getattr(t, "view_text", "") or getattr(t, "view_definition", "") or "",
            }
            for t in client.tables.list(catalog_name=catalog, schema_name=schema)
            if t.name and t.table_type and t.table_type.value == "VIEW"
        ]
        _cache.put(key, result)
        return result
    except Exception as e:
        logger.warning(f"SDK views list failed for {catalog}.{schema}: {e}")
        return []


def list_functions_sdk(client: WorkspaceClient, catalog: str, schema: str) -> list[dict]:
    """List functions in a schema using the SDK (no SQL warehouse needed).

    Returns list of dicts with: function_name, full_name, data_type.
    """
    key = ("functions", catalog, schema)
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        result = [
            {
                "function_name": f.name,
                "full_name": f.full_name or f"{catalog}.{schema}.{f.name}",
                "data_type": getattr(f, "data_type", None),
            }
            for f in client.functions.list(catalog_name=catalog, schema_name=schema)
            if f.name
        ]
        _cache.put(key, result)
        return result
    except Exception as e:
        logger.warning(f"SDK functions.list failed for {catalog}.{schema}: {e}")
        return []


def list_volumes_sdk(client: WorkspaceClient, catalog: str, schema: str) -> list[dict]:
    """List volumes in a schema using the SDK (no SQL warehouse needed).

    Returns list of dicts with: volume_name, volume_type, storage_location.
    """
    key = ("volumes", catalog, schema)
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        result = [
            {
                "volume_name": v.name,
                "volume_type": getattr(v, "volume_type", None),
                "storage_location": getattr(v, "storage_location", ""),
            }
            for v in client.volumes.list(catalog_name=catalog, schema_name=schema)
            if v.name
        ]
        _cache.put(key, result)
        return result
    except Exception as e:
        logger.warning(f"SDK volumes.list failed for {catalog}.{schema}: {e}")
        return []


def get_table_info_sdk(client: WorkspaceClient, full_name: str) -> dict | None:
    """Get table metadata using the SDK (no SQL warehouse needed).

    Returns dict with: name, table_type, columns, storage_location, data_source_format, etc.
    """
    key = ("table_info", full_name)
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        t = client.tables.get(full_name)
        columns = []
        if t.columns:
            for c in t.columns:
                columns.append({
                    "column_name": c.name,
                    "data_type": c.type_text or str(c.type_name) if c.type_name else "",
                    "comment": getattr(c, "comment", ""),
                    "nullable": getattr(c, "nullable", True),
                })
        result = {
            "name": t.name,
            "full_name": t.full_name,
            "table_type": t.table_type.value if t.table_type else "UNKNOWN",
            "columns": columns,
            "storage_location": getattr(t, "storage_location", ""),
            "data_source_format": getattr(t, "data_source_format", ""),
            "owner": getattr(t, "owner", ""),
            "comment": getattr(t, "comment", ""),
            "properties": dict(t.properties) if t.properties else {},
            "created_at": str(getattr(t, "created_at", "")),
            "updated_at": str(getattr(t, "updated_at", "")),
        }
        _cache.put(key, result)
        return result
    except Exception as e:
        logger.warning(f"SDK tables.get failed for {full_name}: {e}")
        return None


def get_catalog_info_sdk(client: WorkspaceClient, catalog: str) -> dict | None:
    """Get catalog metadata using the SDK (no SQL warehouse needed)."""
    key = ("catalog_info", catalog)
    cached = _cache.get(key)
    if cached is not _MISSING:
        return cached
    try:
        c = client.catalogs.get(catalog)
        result = {
            "name": c.name,
            "owner": getattr(c, "owner", ""),
            "comment": getattr(c, "comment", ""),
            "storage_root": getattr(c, "storage_root", None) or getattr(c, "storage_location", None) or "",
            "properties": dict(c.properties) if c.properties else {},
        }
        _cache.put(key, result)
        return result
    except Exception as e:
        logger.warning(f"SDK catalogs.get failed for {catalog}: {e}")
        return None


def delete_table_sdk(client: WorkspaceClient, full_name: str) -> bool:
    """Delete a table using the SDK (no SQL warehouse needed)."""
    try:
        client.tables.delete(full_name)
        return True
    except Exception as e:
        logger.warning(f"SDK tables.delete failed for {full_name}: {e}")
        return False


def invalidate_catalog_cache(catalog: str) -> int:
    """Invalidate all cached metadata for a specific catalog."""
    return _cache.invalidate_catalog(catalog)


def clear_metadata_cache() -> None:
    """Clear all cached metadata."""
    _cache.clear()


def get_metadata_cache_stats() -> dict:
    """Return cache statistics."""
    return _cache.stats()


def set_max_parallel_queries(n: int) -> None:
    """Set the max number of parallel SQL queries.

    Controls concurrency for execute_sql_parallel() and all modules
    that use parallel execution (stats, profiling, diff, clone, etc.).

    Args:
        n: Max parallel queries (default: 10). Use 1 for sequential.
    """
    global _max_parallel_queries
    _max_parallel_queries = max(1, n)
    logger.info(f"Max parallel queries set to {_max_parallel_queries}")


def get_max_parallel_queries() -> int:
    """Get the current max parallel queries setting."""
    return _max_parallel_queries


def execute_sql_parallel(
    client: WorkspaceClient,
    warehouse_id: str,
    queries: list[str],
    max_workers: int | None = None,
    dry_run: bool = False,
) -> list[list[dict]]:
    """Execute multiple SQL queries in parallel.

    Args:
        client: WorkspaceClient instance.
        warehouse_id: SQL warehouse ID or "SERVERLESS".
        queries: List of SQL statements to execute.
        max_workers: Override max parallel queries (default: global setting).
        dry_run: If True, skip write operations.

    Returns:
        List of results in the same order as the input queries.
        Each result is a list[dict] (rows).
    """
    if not queries:
        return []

    workers = max_workers or _max_parallel_queries
    results = [None] * len(queries)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_idx = {
            executor.submit(execute_sql, client, warehouse_id, sql, dry_run=dry_run): i
            for i, sql in enumerate(queries)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                logger.warning(f"Parallel query {idx} failed: {e}")
                results[idx] = []

    return results
