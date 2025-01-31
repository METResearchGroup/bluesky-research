"""Tests for DuckDB wrapper class.

This test suite verifies the functionality of the DuckDB wrapper class, including:
1. Basic connection handling and initialization 
2. Parquet file handling and querying
3. Query execution and DataFrame conversion
4. Error handling and edge cases

The tests use temporary files and in-memory databases to avoid side effects.
Each test focuses on a specific aspect of the functionality to ensure:
- Proper handling of Parquet files
- Efficient query execution 
- Proper error handling
"""

import pytest
import pandas as pd
import duckdb
import tempfile
import os

from lib.db.sql.duckdb import DuckDB, get_duckdb_instance

@pytest.fixture
def db():
    """Create a fresh DuckDB instance for each test."""
    return get_duckdb_instance()

@pytest.fixture
def sample_parquet_file():
    """Create a temporary Parquet file with test data."""
    # Create sample DataFrame
    df = pd.DataFrame({
        'uri': ['uri1', 'uri2', 'uri3'],
        'author': ['author1', 'author2', 'author3'],
        'created_at': pd.date_range('2024-01-01', periods=3)
    })
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        df.to_parquet(tmp.name)
        yield tmp.name
    
    # Cleanup after test
    os.unlink(tmp.name)

def test_initialization():
    """Test DuckDB initialization with and without connection."""
    # Test default initialization
    db1 = DuckDB()
    assert db1.conn is not None
    
    # Test with custom connection
    custom_conn = duckdb.connect(":memory:")
    db2 = DuckDB(conn=custom_conn)
    assert db2.conn == custom_conn

def test_create_parquet_connection(sample_parquet_file):
    """Test Parquet connection creation and querying."""
    # Create connection with specific tables and columns
    tables = [{"name": "parquet_data", "columns": ["uri", "author"]}]
    conn = DuckDB.create_parquet_connection(
        filepaths=[sample_parquet_file],
        tables=tables
    )
    
    # Test query execution
    result = conn.execute("SELECT * FROM parquet_data").df()
    assert set(result.columns) == {"uri", "author"}
    assert len(result) == 3

def test_run_query_as_df_default_mode(db):
    """Test query execution in default mode."""
    # Create test table
    db.conn.execute("""
        CREATE TABLE test (
            id INTEGER,
            value TEXT
        )
    """)
    db.conn.execute("INSERT INTO test VALUES (1, 'one'), (2, 'two')")
    
    # Test query
    result = db.run_query_as_df("SELECT * FROM test")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert set(result.columns) == {"id", "value"}

def test_run_query_as_df_parquet_mode(sample_parquet_file):
    """Test query execution in parquet mode."""
    db = get_duckdb_instance()
    
    tables = [{"name": "parquet_data", "columns": ["uri", "author"]}]
    result, _ = db._run_query_as_df(  # Note: _run_query_as_df returns (df, metrics) tuple
        "SELECT uri, author FROM parquet_data",
        mode="parquet",
        filepaths=[sample_parquet_file],
        query_metadata={"tables": tables}
    )
    
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {"uri", "author"}
    assert len(result) == 3

def test_error_handling():
    """Test error handling in various scenarios."""
    db = get_duckdb_instance()
    
    # Test invalid SQL
    with pytest.raises(duckdb.Error):
        db.run_query_as_df("SELECT * FROM nonexistent_table")
    
    # Test parquet mode without filepaths returns empty dataframe
    query_metadata = {"tables": [{"columns": ["col1", "col2"]}]}
    result = db.run_query_as_df(
        "SELECT * FROM data", 
        mode="parquet",
        query_metadata=query_metadata
    )
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ["col1", "col2"]

def test_multiple_parquet_files(tmp_path):
    """Test handling multiple Parquet files."""
    # Create multiple test files
    files = []
    for i in range(3):
        df = pd.DataFrame({
            'uri': [f'uri{i}_1', f'uri{i}_2'],
            'author': [f'author{i}_1', f'author{i}_2']
        })
        file_path = tmp_path / f"test_{i}.parquet"
        df.to_parquet(file_path)
        files.append(str(file_path))
    
    db = get_duckdb_instance()
    tables = [{"name": "parquet_data", "columns": ["uri", "author"]}]
    result, _ = db._run_query_as_df(  # Note: _run_query_as_df returns (df, metrics) tuple
        "SELECT * FROM parquet_data",
        mode="parquet",
        filepaths=files,
        query_metadata={"tables": tables}
    )
    
    assert len(result) == 6  # 2 rows * 3 files
    assert set(result.columns) == {"uri", "author"}