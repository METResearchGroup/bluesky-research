"""DuckDB metrics collection and reporting."""

from functools import wraps
from typing import Any, Callable, Tuple

import pandas as pd

from lib.log.logger import get_logger
from lib.telemetry.metrics import MetricsCollector

logger = get_logger(__file__)


class DuckDBMetricsCollector(MetricsCollector):
    def collect_query_metrics(self):
        """
        Decorator for collecting DuckDB query metrics.
        Designed to work with class methods that execute DuckDB queries and return DataFrames.
        """

        def decorator(func: Callable) -> Callable:
            @self.measure("duckdb_query")
            @wraps(func)
            def wrapper(
                instance: Any, query: str, *args, **kwargs
            ) -> Tuple[pd.DataFrame, dict]:
                try:
                    # Set up profiling with the correct PRAGMAs
                    instance.conn.execute("SET enable_profiling=json")
                    instance.conn.execute("SET profile_output='query_profile.json'")
                except Exception as e:
                    logger.warning(f"Failed to enable DuckDB profiling: {e}")

                # Execute the original function to get the DataFrame
                df = func(instance, query, *args, **kwargs)

                try:
                    # Get basic metrics that are always available
                    duckdb_metrics = {
                        "duckdb": {
                            "query": {
                                "sql": query,
                                "result_shape": {
                                    "rows": df.shape[0],
                                    "columns": df.shape[1],
                                },
                                "result_memory_usage_mb": df.memory_usage(
                                    deep=True
                                ).sum()
                                / 1024
                                / 1024,
                            }
                        }
                    }

                    # Try to get table statistics
                    try:
                        table_stats = instance.conn.execute("""
                            SELECT 
                                sum(estimated_size)/1024/1024 as size_mb,
                                count(*) as table_count
                            FROM duckdb_tables()
                        """).fetchone()

                        if table_stats:
                            duckdb_metrics["duckdb"]["database"] = {
                                "total_size_mb": table_stats[0],
                                "table_count": table_stats[1],
                            }
                    except Exception as e:
                        logger.debug(f"Failed to get table statistics: {e}")

                    # Try to get query profile
                    try:
                        # Execute an EXPLAIN ANALYZE to get performance metrics
                        profile = instance.conn.execute(f"EXPLAIN ANALYZE {query}").df()
                        if not profile.empty:
                            duckdb_metrics["duckdb"]["profile"] = {
                                "execution_time_ms": float(profile.iloc[0]["timing"])
                                if "timing" in profile
                                else None,
                                "total_rows_processed": int(
                                    profile.iloc[0]["rows_processed"]
                                )
                                if "rows_processed" in profile
                                else None,
                            }
                    except Exception as e:
                        logger.debug(f"Failed to get query profile: {e}")

                except Exception as e:
                    # If all profiling fails, still return basic metrics
                    duckdb_metrics = {
                        "duckdb": {
                            "query": {
                                "sql": query,
                                "result_shape": {
                                    "rows": df.shape[0],
                                    "columns": df.shape[1],
                                },
                                "result_memory_usage_mb": df.memory_usage(
                                    deep=True
                                ).sum()
                                / 1024
                                / 1024,
                            },
                            "profile_error": str(e),
                        }
                    }

                return df, duckdb_metrics

            return wrapper

        return decorator
