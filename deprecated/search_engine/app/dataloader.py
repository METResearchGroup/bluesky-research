from typing import Any, Dict
from lib.db.manage_local_data import load_data_from_local_storage


def get_data_from_keyword(query: str, **kwargs) -> Dict[str, Any]:
    """
    Loader for keyword-based queries. Returns ALL posts from fixed date range.
    If query is 'test query', returns empty dummy result for testing.
    """
    print("Retrieving data from keyword")
    print(f"Query: {query}")
    print(f"kwargs: {kwargs}")
    if query == "test query":
        return {"posts": [], "total_count": 0, "source": "keyword"}
    df = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=["cache"],
        export_format="parquet",
        start_partition_date="2024-11-10",
        end_partition_date="2024-11-21",
    )
    total_count = len(df)
    posts = df.to_dict(orient="records")
    return {"posts": posts, "total_count": total_count, "source": "keyword"}


def get_data_from_query_engine(query: str, **kwargs) -> Dict[str, Any]:
    """
    Loader for query engine-based queries. Returns ALL posts from fixed date range.
    If query is 'test query', returns empty dummy result for testing.
    """
    print("Retrieving data from query_engine")
    print(f"Query: {query}")
    print(f"kwargs: {kwargs}")
    if query == "test query":
        return {"posts": [], "total_count": 0, "source": "query_engine"}
    df = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=["cache"],
        export_format="parquet",
        start_partition_date="2024-11-10",
        end_partition_date="2024-11-21",
    )
    total_count = len(df)
    posts = df.to_dict(orient="records")
    return {"posts": posts, "total_count": total_count, "source": "query_engine"}


def get_data_from_rag(query: str, **kwargs) -> Dict[str, Any]:
    """
    Loader for RAG-based queries. Returns ALL posts from fixed date range.
    If query is 'test query', returns empty dummy result for testing.
    """
    print("Retrieving data from rag")
    print(f"Query: {query}")
    print(f"kwargs: {kwargs}")
    if query == "test query":
        return {"posts": [], "total_count": 0, "source": "rag"}
    df = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=["cache"],
        export_format="parquet",
        start_partition_date="2024-11-10",
        end_partition_date="2024-11-21",
    )
    total_count = len(df)
    posts = df.to_dict(orient="records")
    return {"posts": posts, "total_count": total_count, "source": "rag"}


def load_data(source: str, query: str, **kwargs) -> Dict[str, Any]:
    """
    Dispatch to the appropriate data loader based on source.
    """
    if source == "keyword":
        return get_data_from_keyword(query, **kwargs)
    elif source == "query_engine":
        return get_data_from_query_engine(query, **kwargs)
    elif source == "rag":
        return get_data_from_rag(query, **kwargs)
    else:
        raise ValueError(f"Unknown data source: {source}")
