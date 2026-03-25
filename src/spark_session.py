"""Managed Spark session via Databricks Connect v2.

Provides a singleton DatabricksSession that connects to a remote Databricks
cluster or serverless compute. Used by DQX engine and ODCS contract validation
for DataFrame-level operations.

Configuration is read from:
1. Environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
2. Databricks CLI profile (~/.databrickscfg)
3. clone_config.yaml: cluster_id field

The session is created lazily on first call to get_spark() and reused across
all subsequent calls.
"""

import logging
import os
import threading

logger = logging.getLogger(__name__)

_spark = None
_spark_lock = threading.Lock()
_spark_config = {
    "cluster_id": "",
    "serverless": True,  # Default to serverless compute
}


def ensure_configured():
    """Auto-configure Spark from environment if not already configured.

    Called before get_spark() to make sure host/token from the app's auth
    context are used (set in os.environ by the auth flow).
    """
    global _spark, _spark_config
    host_env = os.environ.get("DATABRICKS_HOST", "")
    token_env = os.environ.get("DATABRICKS_TOKEN", "")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    current_host = _spark_config.get("host", "")

    logger.debug(f"Spark ensure_configured: env_host={host_env[:30] if host_env else 'NOT SET'}, "
                 f"env_token={'SET' if token_env else 'NOT SET'}, "
                 f"client_id={'SET' if client_id else 'NOT SET'}, "
                 f"current_config_host={current_host[:30] if current_host else 'EMPTY'}, "
                 f"session_exists={_spark is not None}")

    # If env host is set and different from current config → reconfigure
    if host_env and host_env != current_host:
        logger.info(f"Spark: auto-configuring from env (host: {host_env})")
        _spark_config["host"] = host_env
        if token_env:
            _spark_config["token"] = token_env
        # Reset session so it reconnects with new credentials
        if _spark is not None:
            try:
                _spark.stop()
            except Exception:
                pass
            _spark = None


def configure_spark(cluster_id: str = "", serverless: bool = False, host: str = "", token: str = "", profile: str = ""):
    """Configure Spark session parameters before first use.

    Args:
        cluster_id: Databricks cluster ID for Databricks Connect.
        serverless: Use serverless compute (no cluster needed).
        host: Databricks workspace URL (overrides DEFAULT profile).
        token: Databricks PAT (overrides DEFAULT profile).
        profile: Databricks CLI profile name to use.
    """
    global _spark_config
    _spark_config["cluster_id"] = cluster_id
    _spark_config["serverless"] = serverless
    _spark_config["host"] = host
    _spark_config["token"] = token
    _spark_config["profile"] = profile
    logger.info(f"Spark configured: cluster_id={cluster_id or '(auto)'}, serverless={serverless}, host={host[:30] + '...' if host else '(env)'}")

    # Reset existing session so next get_spark() picks up new config
    reset_spark()


def get_spark():
    """Get or create a DatabricksSession.

    Creates the session lazily on first call. Thread-safe via lock.
    Uses Databricks Connect v2 to run Spark remotely against a cluster.

    Raises:
        RuntimeError: If Databricks Connect is not installed or cannot connect.
    """
    global _spark

    # Auto-configure from env if needed (picks up host/token from auth flow)
    ensure_configured()

    if _spark is not None:
        return _spark

    with _spark_lock:
        # Double-check after acquiring lock
        if _spark is not None:
            return _spark

        try:
            from databricks.connect import DatabricksSession
        except ImportError:
            raise RuntimeError(
                "databricks-connect is not installed. "
                "Install with: pip install databricks-connect"
            )

        try:
            builder = DatabricksSession.builder

            # Explicitly set host/token from the app's auth context
            # (avoids falling back to DEFAULT profile in ~/.databrickscfg)
            host = _spark_config.get("host") or os.environ.get("DATABRICKS_HOST", "")
            token = _spark_config.get("token") or os.environ.get("DATABRICKS_TOKEN", "")
            profile = _spark_config.get("profile") or os.environ.get("DATABRICKS_CONFIG_PROFILE", "")

            if host:
                builder = builder.host(host)
                logger.info(f"Spark: using host {host}")
            if token:
                builder = builder.token(token)
                logger.info("Spark: using token from app auth")
            if profile and not host:
                builder = builder.profile(profile)
                logger.info(f"Spark: using profile {profile}")

            # Apply cluster/serverless configuration
            cluster_id = _spark_config.get("cluster_id") or os.environ.get("DATABRICKS_CLUSTER_ID", "")
            serverless = _spark_config.get("serverless", False)

            if serverless:
                builder = builder.serverless(True)
                logger.info("Creating Spark session with serverless compute")
            elif cluster_id:
                builder = builder.clusterId(cluster_id)
                logger.info(f"Creating Spark session with cluster: {cluster_id}")
            else:
                # Default to serverless when no cluster_id is configured
                builder = builder.serverless(True)
                logger.info("Creating Spark session with serverless compute (auto — no cluster_id configured)")

            _spark = builder.getOrCreate()
            logger.info("Spark session created successfully")
            return _spark

        except Exception as e:
            raise RuntimeError(
                f"Failed to create Spark session: {e}. "
                "Ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set, "
                "and a cluster_id is configured (or use serverless=True)."
            )


def get_spark_safe():
    """Get Spark session or return None if not available.

    Use this when Spark is optional (e.g., DQX profiling is a nice-to-have).
    """
    try:
        return get_spark()
    except (RuntimeError, Exception) as e:
        logger.debug(f"Spark not available: {e}")
        return None


def is_spark_available() -> bool:
    """Check if a Spark session can be created."""
    return get_spark_safe() is not None


def get_spark_status() -> dict:
    """Get detailed Spark session status for health checks."""
    result = {
        "available": False,
        "cluster_id": _spark_config.get("cluster_id", ""),
        "serverless": _spark_config.get("serverless", False),
        "session_active": _spark is not None,
        "connect_installed": False,
        "error": None,
    }

    # Check if databricks-connect is installed
    try:
        from databricks.connect import DatabricksSession  # noqa: F401
        result["connect_installed"] = True
    except ImportError:
        result["error"] = "databricks-connect not installed"
        return result

    # Try to get/create session
    try:
        spark = get_spark()
        result["available"] = True
        result["session_active"] = True

        # Get cluster info from session
        try:
            conf = spark.conf
            result["cluster_id"] = conf.get("spark.databricks.clusterUsageTags.clusterId", result["cluster_id"])
            result["spark_version"] = conf.get("spark.databricks.clusterUsageTags.sparkVersion", "")
        except Exception:
            pass

    except RuntimeError as e:
        result["error"] = str(e)

    return result


def reset_spark():
    """Stop and reset the Spark session. Next get_spark() will create a new one."""
    global _spark
    with _spark_lock:
        if _spark is not None:
            try:
                _spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.debug(f"Error stopping Spark session: {e}")
            _spark = None
