import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

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
    # In dry run mode, skip write operations but allow reads (SELECT, SHOW, DESCRIBE)
    if dry_run:
        sql_upper = sql.strip().upper()
        is_read = sql_upper.startswith(("SELECT", "SHOW", "DESCRIBE"))
        if not is_read:
            logger.info(f"[DRY RUN] Would execute: {sql}")
            return []

    # Route through pluggable executor (spark.sql in serverless jobs)
    if _sql_executor is not None:
        return _sql_executor(sql)

    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            _rate_limiter.acquire()
            return _execute_sql_once(client, warehouse_id, sql)
        except RuntimeError:
            # SQL failures (syntax error, permission denied) — don't retry
            raise
        except Exception as e:
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
