"""Tests for DuckDB metrics collection."""

import duckdb
import pandas as pd
import pytest

from lib.telemetry.duckdb_metrics import DuckDBMetricsCollector


class TestDuckDBConnection:
    """Test class that simulates a DuckDB connection wrapper."""
    
    @DuckDBMetricsCollector().collect_query_metrics()
    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).df()


@pytest.fixture
def db_connection():
    """Fixture providing a test DuckDB connection."""
    connection = TestDuckDBConnection()
    connection.conn = duckdb.connect(':memory:')
    yield connection
    if hasattr(connection, 'conn'):
        connection.conn.close()


def test_basic_query_metrics(db_connection):
    """Test basic query metrics collection."""
    # Create a test table
    db_connection.conn.execute("""
        CREATE TABLE test_table AS 
        SELECT * FROM (VALUES 
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
        ) AS t(id, value)
    """)
    
    # Execute query with metrics collection
    df, metrics = db_connection.execute_query("SELECT * FROM test_table")
    
    # Verify DataFrame results
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 2)
    
    # Verify basic metrics structure
    assert "duckdb" in metrics
    assert "query" in metrics["duckdb"]
    
    # Verify query metrics
    query_metrics = metrics["duckdb"]["query"]
    assert query_metrics["sql"] == "SELECT * FROM test_table"
    assert query_metrics["result_shape"]["rows"] == 3
    assert query_metrics["result_shape"]["columns"] == 2
    assert isinstance(query_metrics["result_memory_usage_mb"], float)


def test_database_statistics(db_connection):
    """Test database statistics collection."""
    # Create multiple test tables
    db_connection.conn.execute("CREATE TABLE table1 AS SELECT 1 as id")
    db_connection.conn.execute("CREATE TABLE table2 AS SELECT 2 as id")
    
    _, metrics = db_connection.execute_query("SELECT * FROM table1")
    
    # Verify database statistics
    assert "database" in metrics["duckdb"]
    db_stats = metrics["duckdb"]["database"]
    assert isinstance(db_stats["total_size_mb"], float)
    assert db_stats["table_count"] >= 2  # At least our two tables


def test_query_profiling(db_connection):
    """Test query profiling metrics."""
    db_connection.conn.execute("""
        CREATE TABLE profile_test AS 
        SELECT * FROM range(1000) t(id)
    """)
    
    _, metrics = db_connection.execute_query("""
        SELECT COUNT(*) 
        FROM profile_test 
        WHERE id > 500
    """)
    
    # Verify profiling metrics
    assert "profile" in metrics["duckdb"]
    profile = metrics["duckdb"]["profile"]
    assert isinstance(profile["execution_time_ms"], (float, type(None)))
    assert isinstance(profile["total_rows_processed"], (int, type(None)))


def test_error_handling(db_connection):
    """Test metrics collection with invalid query."""
    with pytest.raises(Exception):
        db_connection.execute_query("SELECT * FROM nonexistent_table")
    
    # Test with a valid query after an error to ensure collector still works
    df, metrics = db_connection.execute_query("SELECT 1 as id")
    assert isinstance(df, pd.DataFrame)
    assert "duckdb" in metrics
    
    # Verify basic metrics structure
    assert "duckdb" in metrics
    assert "query" in metrics["duckdb"]
    
    # Verify query metrics
    query_metrics = metrics["duckdb"]["query"]
    assert query_metrics["sql"] == "SELECT 1 as id"
    assert query_metrics["result_shape"]["rows"] == 1
    assert query_metrics["result_shape"]["columns"] == 1
    assert isinstance(query_metrics["result_memory_usage_mb"], float) 