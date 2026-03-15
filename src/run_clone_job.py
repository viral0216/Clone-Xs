"""Entry point for serverless spark_python_task clone jobs.

This script runs inside Databricks Runtime on serverless compute.
It wires spark.sql() into the execute_sql path so the entire clone
engine works without a SQL warehouse.

Submitted by: src/serverless.py via jobs.submit(spark_python_task)
Installed as: part of the Clone-Xs wheel

Usage (automatic — called by serverless.submit_clone_job):
    spark-submit run_clone_job.py '{"source_catalog":"prod","dest_catalog":"staging",...}'
"""

import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    # Parse config from command-line arg
    if len(sys.argv) < 2:
        logger.error("Usage: run_clone_job.py '<json_config>'")
        sys.exit(1)

    config = json.loads(sys.argv[1])
    logger.info("Clone job starting: %s -> %s", config.get("source_catalog"), config.get("dest_catalog"))

    # Get SparkSession (available in Databricks Runtime)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Wire spark.sql() as the SQL executor — this is the key line.
    # All execute_sql() calls throughout the clone engine will now
    # route through spark.sql() instead of the SQL Statement API.
    def spark_sql_executor(sql: str) -> list[dict]:
        df = spark.sql(sql)
        return [row.asDict() for row in df.collect()]

    from src.client import set_sql_executor
    set_sql_executor(spark_sql_executor)

    # Run the clone
    from src.catalog_clone_api import clone_full_catalog

    result = clone_full_catalog(
        source_catalog=config["source_catalog"],
        dest_catalog=config["dest_catalog"],
        warehouse_id=config.get("warehouse_id", "SPARK_SQL"),
        clone_type=config.get("clone_type", "DEEP"),
        load_type=config.get("load_type", "FULL"),
        dry_run=config.get("dry_run", False),
        max_workers=config.get("max_workers", 4),
        parallel_tables=config.get("parallel_tables", 2),
        exclude_schemas=config.get("exclude_schemas"),
        include_schemas=config.get("include_schemas"),
        copy_permissions=config.get("copy_permissions", True),
        copy_ownership=config.get("copy_ownership", True),
        copy_tags=config.get("copy_tags", True),
        validate_after_clone=config.get("validate_after_clone", False),
        enable_rollback=config.get("enable_rollback", False),
    )

    # Output result as JSON (retrieved by serverless.py via job logs)
    result_json = json.dumps(result, default=str)
    print("__CLONE_RESULT__")
    print(result_json)

    logger.info("Clone job complete.")


if __name__ == "__main__":
    main()
