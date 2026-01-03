"""Test and validate archive tables for Nature paper 2024 data.

This script validates that all archive tables are correctly set up in Athena,
tests basic queries, verifies partition pruning, and benchmarks performance.
"""

import time
from typing import Any, Dict

from lib.aws.athena import Athena, DEFAULT_DB_NAME
from lib.aws.helper import create_client
from lib.log.logger import get_logger

logger = get_logger(__name__)

# Expected archive tables
EXPECTED_ARCHIVE_TABLES = [
    "archive_fetch_posts_used_in_feeds",
    "archive_generated_feeds",
    "archive_in_network_user_activity",
    "archive_ml_inference_ime",
    "archive_ml_inference_perspective_api",
    "archive_ml_inference_sociopolitical",
    "archive_ml_inference_valence_classifier",
    "archive_post_scores",
    "archive_user_session_logs",
    "archive_study_user_activity_post",
    "archive_study_user_activity_like",
    "archive_study_user_activity_follow",
    "archive_study_user_activity_reply",
    "archive_study_user_activity_repost",
    "archive_study_user_activity_block",
]


class ArchiveTableValidator:
    """Validates archive tables in Athena."""

    def __init__(self):
        self.athena = Athena()
        self.athena_client = create_client("athena")
        self.validation_results: Dict[str, Any] = {
            "table_existence": {},
            "schema_validation": {},
            "basic_queries": {},
            "partition_pruning": {},
            "joins": {},
            "performance": {},
        }
        self.table_inventory: Dict[str, int] = {}

    def get_query_execution_info(self, query_execution_id: str) -> Dict[str, Any]:
        """Get detailed query execution information."""
        response = self.athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        return response["QueryExecution"]

    def run_query_with_metrics(
        self, query: str, db_name: str = DEFAULT_DB_NAME
    ) -> Dict[str, Any]:
        """Run a query and return execution metrics."""
        logger.info(f"Running query: {query}")
        start_time = time.time()

        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": db_name},
            ResultConfiguration={
                "OutputLocation": "s3://bluesky-research/athena-results/"
            },
            WorkGroup="prod_workgroup",
        )
        query_execution_id = response["QueryExecutionId"]
        status = "RUNNING"

        num_waits = 0
        max_waits = 60  # 5 minutes max wait

        while status in ["RUNNING", "QUEUED"]:
            time.sleep(5)
            execution_info = self.get_query_execution_info(query_execution_id)
            status = execution_info["Status"]["State"]
            if status in ["FAILED", "CANCELLED"]:
                raise Exception(
                    f"Query {status}: {execution_info['Status'].get('StateChangeReason', 'Unknown error')}"
                )
            num_waits += 1
            if num_waits >= max_waits:
                raise Exception(f"Query exceeded max wait time: {status}")

        execution_time = time.time() - start_time
        execution_info = self.get_query_execution_info(query_execution_id)

        # Extract metrics
        stats = execution_info.get("Statistics", {})
        data_scanned_bytes = stats.get("DataScannedInBytes", 0)
        data_scanned_gb = data_scanned_bytes / (1024**3)
        cost = data_scanned_gb * 5.0  # $5 per TB = $0.005 per GB

        return {
            "query_execution_id": query_execution_id,
            "execution_time_seconds": execution_time,
            "data_scanned_bytes": data_scanned_bytes,
            "data_scanned_gb": data_scanned_gb,
            "estimated_cost_usd": cost,
            "status": status,
        }

    def validate_table_existence(self) -> bool:
        """Validate that all expected archive tables exist."""
        logger.info("Validating table existence...")
        try:
            # Get list of tables
            query = "SHOW TABLES LIKE 'archive_%'"
            df = self.athena.query_results_as_df(query)
            actual_tables = set(
                df.iloc[:, 0].tolist()
            )  # First column contains table names

            missing_tables = set(EXPECTED_ARCHIVE_TABLES) - actual_tables
            extra_tables = actual_tables - set(EXPECTED_ARCHIVE_TABLES)

            self.validation_results["table_existence"] = {
                "expected_count": len(EXPECTED_ARCHIVE_TABLES),
                "actual_count": len(actual_tables),
                "missing_tables": list(missing_tables),
                "extra_tables": list(extra_tables),
                "all_present": len(missing_tables) == 0,
            }

            if missing_tables:
                logger.error(f"Missing tables: {missing_tables}")
            if extra_tables:
                logger.warning(f"Extra tables found: {extra_tables}")

            return len(missing_tables) == 0
        except Exception as e:
            logger.error(f"Error validating table existence: {e}")
            self.validation_results["table_existence"] = {
                "error": str(e),
                "all_present": False,
            }
            return False

    def validate_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Validate schema for a specific table."""
        try:
            query = f"DESCRIBE {table_name}"
            df = self.athena.query_results_as_df(query)

            # Check for partition_date column
            has_partition_date = False
            partition_date_is_partition = False
            columns = []

            for _, row in df.iterrows():
                col_name = row.iloc[0].strip()
                col_type = row.iloc[1].strip() if len(row) > 1 else ""
                col_comment = row.iloc[2].strip() if len(row) > 2 else ""

                columns.append({"name": col_name, "type": col_type})

                if col_name.lower() == "partition_date":
                    has_partition_date = True
                    # Check if it's marked as a partition
                    if "partition" in col_comment.lower() or col_type == "":
                        partition_date_is_partition = True

            return {
                "has_partition_date": has_partition_date,
                "partition_date_is_partition": partition_date_is_partition,
                "column_count": len(columns),
                "columns": columns[:10],  # First 10 columns for brevity
                "valid": has_partition_date and partition_date_is_partition,
            }
        except Exception as e:
            logger.error(f"Error validating schema for {table_name}: {e}")
            return {"error": str(e), "valid": False}

    def validate_all_schemas(self) -> bool:
        """Validate schemas for all archive tables."""
        logger.info("Validating table schemas...")
        all_valid = True

        for table in EXPECTED_ARCHIVE_TABLES:
            schema_result = self.validate_table_schema(table)
            self.validation_results["schema_validation"][table] = schema_result
            if not schema_result.get("valid", False):
                all_valid = False
                logger.error(f"Schema validation failed for {table}")

        return all_valid

    def test_basic_query(self, table_name: str) -> Dict[str, Any]:
        """Test a basic COUNT query on a table."""
        try:
            query = f"SELECT COUNT(*) as row_count FROM {table_name} LIMIT 1"
            metrics = self.run_query_with_metrics(query)
            df = self.athena.query_results_as_df(query)
            row_count = int(df.iloc[0]["row_count"]) if len(df) > 0 else 0

            self.table_inventory[table_name] = row_count

            return {
                "success": True,
                "row_count": row_count,
                "execution_time_seconds": metrics["execution_time_seconds"],
                "data_scanned_gb": metrics["data_scanned_gb"],
                "estimated_cost_usd": metrics["estimated_cost_usd"],
            }
        except Exception as e:
            logger.error(f"Error testing basic query for {table_name}: {e}")
            return {"success": False, "error": str(e)}

    def test_all_basic_queries(self) -> bool:
        """Test basic queries on all tables."""
        logger.info("Testing basic queries on all tables...")
        all_success = True

        for table in EXPECTED_ARCHIVE_TABLES:
            logger.info(f"Testing basic query for {table}...")
            result = self.test_basic_query(table)
            self.validation_results["basic_queries"][table] = result
            if not result.get("success", False):
                all_success = False

        return all_success

    def test_partition_pruning(self, table_name: str) -> Dict[str, Any]:
        """Test partition pruning with a date filter."""
        try:
            # Query with partition filter
            query = f"SELECT COUNT(*) as row_count FROM {table_name} WHERE partition_date = '2024-01-01' LIMIT 1"
            metrics = self.run_query_with_metrics(query)

            return {
                "success": True,
                "execution_time_seconds": metrics["execution_time_seconds"],
                "data_scanned_gb": metrics["data_scanned_gb"],
                "estimated_cost_usd": metrics["estimated_cost_usd"],
                "partition_pruning_working": metrics["data_scanned_gb"]
                < 1.0,  # Should scan < 1GB with partition pruning
            }
        except Exception as e:
            logger.error(f"Error testing partition pruning for {table_name}: {e}")
            return {"success": False, "error": str(e)}

    def test_all_partition_pruning(self) -> bool:
        """Test partition pruning on all tables."""
        logger.info("Testing partition pruning on all tables...")
        all_success = True

        for table in EXPECTED_ARCHIVE_TABLES:
            logger.info(f"Testing partition pruning for {table}...")
            result = self.test_partition_pruning(table)
            self.validation_results["partition_pruning"][table] = result
            if not result.get("success", False):
                all_success = False

        return all_success

    def test_joins(self) -> bool:
        """Test joins between related tables."""
        logger.info("Testing joins between related tables...")
        join_tests = [
            {
                "name": "posts_with_perspective_api",
                "query": """
                    SELECT COUNT(*) as row_count
                    FROM archive_fetch_posts_used_in_feeds p
                    JOIN archive_ml_inference_perspective_api ml ON p.uri = ml.uri
                    WHERE p.partition_date = '2024-01-01'
                    LIMIT 1
                """,
            },
            {
                "name": "posts_with_sociopolitical",
                "query": """
                    SELECT COUNT(*) as row_count
                    FROM archive_fetch_posts_used_in_feeds p
                    JOIN archive_ml_inference_sociopolitical ml ON p.uri = ml.uri
                    WHERE p.partition_date = '2024-01-01'
                    LIMIT 1
                """,
            },
            {
                "name": "posts_with_post_scores",
                "query": """
                    SELECT COUNT(*) as row_count
                    FROM archive_fetch_posts_used_in_feeds p
                    JOIN archive_post_scores ps ON p.uri = ps.uri
                    WHERE p.partition_date = '2024-01-01'
                    LIMIT 1
                """,
            },
        ]

        all_success = True
        for test in join_tests:
            try:
                logger.info(f"Testing join: {test['name']}...")
                metrics = self.run_query_with_metrics(test["query"])
                self.validation_results["joins"][test["name"]] = {
                    "success": True,
                    "execution_time_seconds": metrics["execution_time_seconds"],
                    "data_scanned_gb": metrics["data_scanned_gb"],
                    "estimated_cost_usd": metrics["estimated_cost_usd"],
                }
            except Exception as e:
                logger.error(f"Error testing join {test['name']}: {e}")
                self.validation_results["joins"][test["name"]] = {
                    "success": False,
                    "error": str(e),
                }
                all_success = False

        return all_success

    def generate_report(self) -> str:
        """Generate a validation report."""
        report_lines = [
            "=" * 80,
            "Archive Tables Validation Report",
            "=" * 80,
            "",
        ]

        # Table Existence
        report_lines.append("## Table Existence")
        te = self.validation_results["table_existence"]
        if te.get("all_present", False):
            report_lines.append(
                f"✅ All {te['expected_count']} expected tables are present"
            )
        else:
            report_lines.append(f"❌ Missing tables: {te.get('missing_tables', [])}")
        report_lines.append("")

        # Schema Validation
        report_lines.append("## Schema Validation")
        schema_valid = all(
            result.get("valid", False)
            for result in self.validation_results["schema_validation"].values()
        )
        if schema_valid:
            report_lines.append("✅ All tables have valid schemas with partition_date")
        else:
            report_lines.append("❌ Some tables have invalid schemas")
            for table, result in self.validation_results["schema_validation"].items():
                if not result.get("valid", False):
                    report_lines.append(
                        f"  - {table}: {result.get('error', 'Invalid schema')}"
                    )
        report_lines.append("")

        # Basic Queries
        report_lines.append("## Basic Query Tests")
        basic_queries_success = all(
            result.get("success", False)
            for result in self.validation_results["basic_queries"].values()
        )
        if basic_queries_success:
            report_lines.append("✅ All basic queries succeeded")
        else:
            report_lines.append("❌ Some basic queries failed")
        report_lines.append("")

        # Table Inventory
        report_lines.append("## Table Inventory (Row Counts)")
        for table, count in sorted(self.table_inventory.items()):
            report_lines.append(f"  {table}: {count:,} rows")
        report_lines.append("")

        # Partition Pruning
        report_lines.append("## Partition Pruning Tests")
        partition_pruning_success = all(
            result.get("success", False)
            for result in self.validation_results["partition_pruning"].values()
        )
        if partition_pruning_success:
            report_lines.append("✅ All partition pruning tests succeeded")
        else:
            report_lines.append("❌ Some partition pruning tests failed")
        report_lines.append("")

        # Joins
        report_lines.append("## Join Tests")
        joins_success = all(
            result.get("success", False)
            for result in self.validation_results["joins"].values()
        )
        if joins_success:
            report_lines.append("✅ All join tests succeeded")
        else:
            report_lines.append("❌ Some join tests failed")
        report_lines.append("")

        # Performance Summary
        report_lines.append("## Performance Summary")
        all_metrics = []
        for table, result in self.validation_results["basic_queries"].items():
            if result.get("success", False):
                all_metrics.append(
                    {
                        "table": table,
                        "time": result.get("execution_time_seconds", 0),
                        "data_scanned_gb": result.get("data_scanned_gb", 0),
                        "cost": result.get("estimated_cost_usd", 0),
                    }
                )

        if all_metrics:
            avg_time = sum(m["time"] for m in all_metrics) / len(all_metrics)
            avg_data = sum(m["data_scanned_gb"] for m in all_metrics) / len(all_metrics)
            avg_cost = sum(m["cost"] for m in all_metrics) / len(all_metrics)
            report_lines.append(f"Average query time: {avg_time:.2f} seconds")
            report_lines.append(f"Average data scanned: {avg_data:.2f} GB")
            report_lines.append(f"Average cost: ${avg_cost:.4f}")
        report_lines.append("")

        # Overall Status
        report_lines.append("## Overall Status")
        all_checks_passed = (
            te.get("all_present", False)
            and schema_valid
            and basic_queries_success
            and partition_pruning_success
            and joins_success
        )
        if all_checks_passed:
            report_lines.append("✅ ALL VALIDATION CHECKS PASSED")
        else:
            report_lines.append("❌ SOME VALIDATION CHECKS FAILED")
        report_lines.append("")

        report_lines.append("=" * 80)

        return "\n".join(report_lines)

    def run_all_validations(self) -> bool:
        """Run all validation checks."""
        logger.info("Starting archive table validation...")

        # Run all validations
        table_existence_ok = self.validate_table_existence()
        schema_validation_ok = self.validate_all_schemas()
        basic_queries_ok = self.test_all_basic_queries()
        partition_pruning_ok = self.test_all_partition_pruning()
        joins_ok = self.test_joins()

        # Generate and print report
        report = self.generate_report()
        print(report)

        return all(
            [
                table_existence_ok,
                schema_validation_ok,
                basic_queries_ok,
                partition_pruning_ok,
                joins_ok,
            ]
        )


def main():
    """Main entry point."""
    validator = ArchiveTableValidator()
    success = validator.run_all_validations()

    if success:
        logger.info("All validation checks passed!")
        return 0
    else:
        logger.error("Some validation checks failed. See report above.")
        return 1


if __name__ == "__main__":
    exit(main())
