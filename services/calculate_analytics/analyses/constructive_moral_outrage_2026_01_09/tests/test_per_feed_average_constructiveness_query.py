"""Tests for get_per_feed_average_constructiveness SQL query.

This test suite validates the SQL query logic using DuckDB with synthetic test data.
It tests the core aggregation logic for calculating per-feed average constructiveness.

We're testing the query logic in "get_per_feed_average_constructiveness.sql"
"""

import json
from typing import Any

import duckdb
import pandas as pd
import pytest


def setup_duckdb_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Set up DuckDB tables with synthetic test data.
    
    Creates feed_posts table (pre-processed from JSON) and labels table.
    This follows the pattern from experiment.py where JSON is parsed in Python first.
    
    Args:
        conn: DuckDB connection to register tables on
    """
    # Create synthetic feed data with JSON arrays
    feeds_data = [
        {
            "feed_id": "feed1",
            "user": "user1",
            "bluesky_handle": "@user1.bsky.social",
            "condition": "control",
            "feed_generation_timestamp": "2024-01-01 10:00:00",
            "feed": json.dumps([{"item": "at://did:plc:abc123/post1", "is_in_network": False}]),
        },
        {
            "feed_id": "feed2",
            "user": "user2",
            "bluesky_handle": "@user2.bsky.social",
            "condition": "treatment",
            "feed_generation_timestamp": "2024-01-01 11:00:00",
            "feed": json.dumps([
                {"item": "at://did:plc:def456/post2", "is_in_network": True},
                {"item": "at://did:plc:abc123/post1", "is_in_network": False},
            ]),
        },
        {
            "feed_id": "feed3",
            "user": "user3",
            "bluesky_handle": "@user3.bsky.social",
            "condition": "control",
            "feed_generation_timestamp": "2024-01-01 12:00:00",
            "feed": json.dumps([
                {"item": "at://did:plc:ghi789/post3", "is_in_network": False},
                {"item": "at://did:plc:jkl012/post4", "is_in_network": True},
                {"item": "at://did:plc:mno345/post5", "is_in_network": False},
            ]),
        },
        {
            "feed_id": "feed4",
            "user": "user4",
            "bluesky_handle": "@user4.bsky.social",
            "condition": "treatment",
            "feed_generation_timestamp": "2024-01-01 13:00:00",
            "feed": json.dumps([]),  # Empty feed
        },
    ]
    
    # Parse JSON and create feed_posts table (one row per post URI)
    feed_posts_data = []
    for feed in feeds_data:
        items = json.loads(feed["feed"])
        for item_obj in items:
            if isinstance(item_obj, dict) and "item" in item_obj:
                feed_posts_data.append(
                    {
                        "feed_id": feed["feed_id"],
                        "user": feed["user"],
                        "bluesky_handle": feed["bluesky_handle"],
                        "condition": feed["condition"],
                        "feed_generation_timestamp": feed["feed_generation_timestamp"],
                        "uri": item_obj["item"],
                    }
                )
    
    feed_posts_df = pd.DataFrame(feed_posts_data)
    
    # Create synthetic label data
    # Include multiple labels for same URI to test deduplication (most recent wins)
    labels_data = [
        {
            "uri": "at://did:plc:abc123/post1",
            "prob_constructive": 0.8,
            "label_timestamp": "2024-01-01-09:00:00",
        },
        {
            "uri": "at://did:plc:abc123/post1",  # Duplicate URI - older timestamp
            "prob_constructive": 0.7,
            "label_timestamp": "2024-01-01-08:00:00",
        },
        {
            "uri": "at://did:plc:def456/post2",
            "prob_constructive": 0.6,
            "label_timestamp": "2024-01-01-10:00:00",
        },
        {
            "uri": "at://did:plc:ghi789/post3",
            "prob_constructive": 0.9,
            "label_timestamp": "2024-01-01-11:00:00",
        },
        {
            "uri": "at://did:plc:jkl012/post4",
            "prob_constructive": 0.5,
            "label_timestamp": "2024-01-01-12:00:00",
        },
        # post5 has no label - tests partial coverage
    ]
    
    labels_df = pd.DataFrame(labels_data)
    
    # Register tables in DuckDB
    conn.register("feed_posts", feed_posts_df)
    conn.register("archive_ml_inference_perspective_api", labels_df)


# DuckDB-compatible SQL query
# Adapted from Athena/Presto syntax to DuckDB syntax
# Note: JSON parsing is done in Python (setup_duckdb_tables), so we start with feed_posts table
DUCKDB_QUERY = """
WITH
distinct_uris AS (
  SELECT DISTINCT uri
  FROM feed_posts
),

-- Filter to just URIs used in these feeds and dedupe per URI
labels_filtered AS (
  SELECT
    l.uri,
    l.prob_constructive,
    try_strptime(l.label_timestamp, '%Y-%m-%d-%H:%M:%S') AS label_ts,
    row_number() OVER (
      PARTITION BY l.uri
      ORDER BY try_strptime(l.label_timestamp, '%Y-%m-%d-%H:%M:%S') DESC NULLS LAST
    ) AS rn
  FROM archive_ml_inference_perspective_api l
  WHERE l.uri IN (SELECT uri FROM distinct_uris)
    AND l.prob_constructive IS NOT NULL
),

labels_dedup AS (
  SELECT uri, prob_constructive
  FROM labels_filtered
  WHERE rn = 1
)

-- Final output: one row per feed with avg(prob_constructive) over its posts
SELECT
  p.feed_id,
  p.bluesky_handle,
  p.condition,
  p.feed_generation_timestamp,
  avg(l.prob_constructive) AS avg_prob_constructive,
  count(*) AS n_posts_in_feed,
  count(l.prob_constructive) AS n_labeled_posts_in_feed
FROM feed_posts p
LEFT JOIN labels_dedup l
  ON p.uri = l.uri
GROUP BY 1,2,3,4
ORDER BY feed_generation_timestamp, feed_id
"""


def run_query(conn: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    """Execute SQL query and return results as DataFrame.
    
    Args:
        conn: DuckDB connection to execute query on
        query: SQL query string to execute
        
    Returns:
        pandas DataFrame with query results
        
    Raises:
        Exception: If query execution fails
    """
    try:
        result = conn.execute(query).df()
        return result
    except Exception as e:
        raise RuntimeError(f"Query execution failed: {e}") from e


def _validate_result_type(result: pd.DataFrame) -> None:
    """Validate that result is a DataFrame."""
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"


def _validate_columns(result: pd.DataFrame) -> None:
    """Validate that result contains all expected columns."""
    expected_columns = {
        "feed_id",
        "bluesky_handle",
        "condition",
        "feed_generation_timestamp",
        "avg_prob_constructive",
        "n_posts_in_feed",
        "n_labeled_posts_in_feed",
    }
    actual_columns = set(result.columns)
    assert actual_columns == expected_columns, (
        f"Expected columns {expected_columns}, got {actual_columns}"
    )


def _validate_data_types(result: pd.DataFrame) -> None:
    """Validate that columns have correct data types."""
    assert result["feed_id"].dtype == "object", "feed_id should be string"
    assert result["bluesky_handle"].dtype == "object", "bluesky_handle should be string"
    assert result["condition"].dtype == "object", "condition should be string"
    assert result["feed_generation_timestamp"].dtype == "object", (
        "feed_generation_timestamp should be string"
    )
    assert pd.api.types.is_numeric_dtype(result["avg_prob_constructive"]), (
        "avg_prob_constructive should be numeric"
    )
    assert pd.api.types.is_integer_dtype(result["n_posts_in_feed"]), (
        "n_posts_in_feed should be integer"
    )
    assert pd.api.types.is_integer_dtype(result["n_labeled_posts_in_feed"]), (
        "n_labeled_posts_in_feed should be integer"
    )


def _validate_label_counts(result: pd.DataFrame) -> None:
    """Validate that labeled posts count does not exceed total posts count."""
    assert (result["n_labeled_posts_in_feed"] <= result["n_posts_in_feed"]).all(), (
        "n_labeled_posts_in_feed should not exceed n_posts_in_feed"
    )


def _validate_feed_result(
    result_dict: dict[str, dict[str, Any]],
    feed_id: str,
    expected_avg: float,
    expected_n_posts: int,
    expected_n_labeled: int,
) -> None:
    """Validate results for a specific feed.
    
    Args:
        result_dict: Dictionary mapping feed_id to result data
        feed_id: Feed ID to validate
        expected_avg: Expected average constructiveness score
        expected_n_posts: Expected number of posts in feed
        expected_n_labeled: Expected number of labeled posts in feed
    """
    assert feed_id in result_dict, f"{feed_id} should be in results"
    
    actual_avg = result_dict[feed_id]["avg_prob_constructive"]
    assert abs(actual_avg - expected_avg) < 0.001, (
        f"{feed_id} avg should be {expected_avg}, got {actual_avg}"
    )
    
    actual_n_posts = result_dict[feed_id]["n_posts_in_feed"]
    assert actual_n_posts == expected_n_posts, (
        f"{feed_id} should have {expected_n_posts} posts, got {actual_n_posts}"
    )
    
    actual_n_labeled = result_dict[feed_id]["n_labeled_posts_in_feed"]
    assert actual_n_labeled == expected_n_labeled, (
        f"{feed_id} should have {expected_n_labeled} labeled posts, got {actual_n_labeled}"
    )


def _validate_aggregation_results(result: pd.DataFrame) -> None:
    """Validate aggregation logic matches expected results based on test data."""
    result_dict = result.set_index("feed_id").to_dict("index")
    
    # feed1: 1 post with label 0.8 (most recent, not 0.7 - tests deduplication)
    _validate_feed_result(result_dict, "feed1", 0.8, 1, 1)
    
    # feed2: 2 posts with labels 0.6 and 0.8, avg = (0.6 + 0.8) / 2 = 0.7
    _validate_feed_result(result_dict, "feed2", 0.7, 2, 2)
    
    # feed3: 3 posts with labels 0.9, 0.5, and NULL (post5), avg = (0.9 + 0.5) / 2 = 0.7
    _validate_feed_result(result_dict, "feed3", 0.7, 3, 2)
    
    # feed4: empty feed - filtered out (no rows in feed_posts table)


def _validate_ordering(result: pd.DataFrame) -> None:
    """Validate that results are ordered by feed_generation_timestamp."""
    assert result["feed_generation_timestamp"].is_monotonic_increasing, (
        "Results should be ordered by feed_generation_timestamp"
    )


def validate_query(result: pd.DataFrame) -> None:
    """Validate query results.
    
    Validates:
    - Expected columns exist
    - Data types are correct
    - Aggregation logic is correct
    - Deduplication works (most recent label_timestamp is used)
    - Partial label coverage is handled
    - Results are properly ordered
    
    Args:
        result: DataFrame with query results
        
    Raises:
        AssertionError: If validation fails
    """
    _validate_result_type(result)
    _validate_columns(result)
    _validate_data_types(result)
    _validate_label_counts(result)
    _validate_aggregation_results(result)
    _validate_ordering(result)


def test_sql_query() -> None:
    """Test SQL query execution and validation with pretty-printed output."""
    # Set up pretty-printing
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    
    # Create DuckDB in-memory connection
    conn = duckdb.connect(":memory:")
    
    # Set up tables
    print("\n=== Setting up DuckDB tables ===")
    setup_duckdb_tables(conn)
    print("Tables created successfully")
    
    # Run query
    print("\n=== Running SQL query ===")
    result = run_query(conn, DUCKDB_QUERY)
    print(f"Query executed successfully. Retrieved {len(result)} rows.")
    
    # Validate query
    print("\n=== Validating query results ===")
    validate_query(result)
    print("Validation passed!")
    
    # Pretty-print results
    print("\n=== Query Results ===")
    print(result)
    
    print("\n=== Summary Statistics ===")
    print(f"Total feeds: {len(result)}")
    print(f"Average posts per feed: {result['n_posts_in_feed'].mean():.2f}")
    print(f"Average labeled posts per feed: {result['n_labeled_posts_in_feed'].mean():.2f}")
    print(f"Overall average constructiveness: {result['avg_prob_constructive'].mean():.4f}")

if __name__ == "__main__":
    test_sql_query()
