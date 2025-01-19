"""Tests for DuckDB wrapper class.

This test suite verifies the functionality of the DuckDB wrapper class, including:
1. Basic connection handling and initialization
2. Query metadata extraction (tables, columns)
3. Parquet file handling and querying
4. Query execution and DataFrame conversion
5. Error handling and edge cases

The tests use temporary files and in-memory databases to avoid side effects.
Each test focuses on a specific aspect of the functionality to ensure:
- Correct parsing of SQL queries
- Proper handling of Parquet files
- Accurate metadata extraction
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

def test_get_query_metadata_simple():
    """Test metadata extraction from simple queries."""
    db = get_duckdb_instance()
    
    # Test simple SELECT
    metadata = db.get_query_metadata("SELECT uri, author FROM posts")
    assert metadata["tables"] == ["posts"]
    assert set(metadata["columns"]) == {"uri", "author"}

def test_get_query_metadata_where():
    """Test metadata extraction from queries with WHERE clause."""
    db = get_duckdb_instance()

    # Test with WHERE clause
    metadata = db.get_query_metadata("SELECT uri FROM posts WHERE author = 'test'")
    breakpoint()
    assert metadata["tables"] == ["posts"]
    assert set(metadata["columns"]) == {"uri", "author"}

def test_get_query_metadata_complex():
    """Test metadata extraction from complex queries."""
    db = get_duckdb_instance()
    
    # Test with multiple tables
    metadata = db.get_query_metadata("""
        SELECT p.uri, u.name 
        FROM posts p 
        JOIN users u ON p.author = u.id
    """)
    breakpoint()
    assert set(metadata["tables"]) == {"posts", "users"}
    assert set(metadata["columns"]) == {"uri", "name", "author", "id"}

def test_get_query_metadata_wildcard():
    """Test metadata extraction with wildcards."""
    db = get_duckdb_instance()
    
    metadata = db.get_query_metadata("SELECT * FROM posts")
    assert metadata["tables"] == ["posts"]
    assert metadata["columns"] == ["*"]

def test_create_parquet_connection(sample_parquet_file):
    """Test Parquet connection creation and querying."""
    # Create connection with specific columns
    conn = DuckDB.create_parquet_connection(
        filepaths=[sample_parquet_file],
        columns=["uri", "author"]
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
    
    result = db.run_query_as_df(
        "SELECT uri, author FROM parquet_data",
        mode="parquet",
        filepaths=[sample_parquet_file]
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
    
    # Test parquet mode without filepaths
    with pytest.raises(ValueError):
        db.run_query_as_df("SELECT * FROM data", mode="parquet")

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
    result = db.run_query_as_df(
        "SELECT * FROM parquet_data",
        mode="parquet",
        filepaths=files
    )
    
    assert len(result) == 6  # 2 rows * 3 files
    assert set(result.columns) == {"uri", "author"}

def test_query_metadata_performance():
    """Test performance of metadata extraction with large queries."""
    db = get_duckdb_instance()
    
    # Create a complex query
    complex_query = """
    SELECT 
        p.uri,
        p.author,
        u.name,
        c.content,
        r.rating
    FROM posts p
    LEFT JOIN users u ON p.author = u.id
    LEFT JOIN comments c ON p.uri = c.post_uri
    LEFT JOIN ratings r ON p.uri = r.post_uri
    WHERE p.created_at > '2024-01-01'
    GROUP BY p.uri, p.author, u.name, c.content, r.rating
    HAVING COUNT(*) > 1
    ORDER BY p.created_at DESC
    """
    
    metadata = db.get_query_metadata(complex_query)
    assert len(metadata["tables"]) > 0
    assert len(metadata["columns"]) > 0 