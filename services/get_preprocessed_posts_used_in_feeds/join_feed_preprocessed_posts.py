"""Join in-feed URIs from ``fetch_posts_used_in_feeds`` to ``preprocessed_posts`` rows."""

from __future__ import annotations

from typing import Any

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.models import StorageTier
from lib.log.logger import get_logger

logger = get_logger(__file__)


def load_posts_used_in_feeds_from_storage(partition_date: str) -> pd.DataFrame:
    """Load unique post URIs used in feeds for a single feed ``partition_date``."""
    active_df = load_data_from_local_storage(
        service="fetch_posts_used_in_feeds",
        storage_tiers=[StorageTier.ACTIVE],
        partition_date=partition_date,
    )
    cache_df = load_data_from_local_storage(
        service="fetch_posts_used_in_feeds",
        storage_tiers=[StorageTier.CACHE],
        partition_date=partition_date,
    )
    out = pd.concat([active_df, cache_df], ignore_index=True)
    if not out.empty and "uri" in out.columns:
        out = out.drop_duplicates(subset=["uri"], keep="first")
    return out


def _empty_preprocessed_frame(table_columns: list[str] | None) -> pd.DataFrame:
    if table_columns:
        return pd.DataFrame(columns=table_columns)
    return pd.DataFrame()


def _load_preprocessed_posts_lookback_raw(
    lookback_start_date: str,
    lookback_end_date: str,
    *,
    table_columns: list[str] | None,
) -> pd.DataFrame:
    if table_columns:
        cols = ", ".join(table_columns)
        query = f"""
            SELECT {cols}
            FROM preprocessed_posts
            WHERE text IS NOT NULL
            AND text != ''
        """
        metadata_columns = list(dict.fromkeys(table_columns))
        query_metadata = {
            "tables": [{"name": "preprocessed_posts", "columns": metadata_columns}]
        }
        extra: dict[str, Any] = {
            "duckdb_query": query,
            "query_metadata": query_metadata,
        }
    else:
        extra = {}

    active_df = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=[StorageTier.ACTIVE],
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        **extra,
    )
    cache_df = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=[StorageTier.CACHE],
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        **extra,
    )
    return pd.concat([active_df, cache_df], ignore_index=True)


def load_preprocessed_posts_used_in_feeds_for_partition_date(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
    *,
    table_columns: list[str] | None = None,
    dedupe_uri_keep_first: bool = False,
) -> pd.DataFrame:
    """Intersect ``fetch_posts_used_in_feeds`` URIs with ``preprocessed_posts`` in lookback.

    Scans ``preprocessed_posts`` partitions from ``lookback_start_date`` through
    ``lookback_end_date`` (inclusive), then keeps rows whose ``uri`` appears in
    the feed URI set for ``partition_date``.
    """
    logger.info(
        f"Loading preprocessed posts used in feeds for partition_date={partition_date} "
        f"lookback={lookback_start_date}..{lookback_end_date}"
    )
    posts_used_df = load_posts_used_in_feeds_from_storage(partition_date)
    if posts_used_df.empty or "uri" not in posts_used_df.columns:
        return _empty_preprocessed_frame(table_columns)

    preprocessed_df = _load_preprocessed_posts_lookback_raw(
        lookback_start_date,
        lookback_end_date,
        table_columns=table_columns,
    )
    if preprocessed_df.empty:
        return _empty_preprocessed_frame(table_columns)

    uris: set[str] = set(posts_used_df["uri"].astype(str))
    filtered = preprocessed_df[preprocessed_df["uri"].isin(uris)].copy()  # type: ignore

    if (
        dedupe_uri_keep_first
        and not filtered.empty
        and "preprocessing_timestamp" in filtered.columns
    ):
        filtered = filtered.sort_values("preprocessing_timestamp", ascending=True)  # type: ignore
        filtered = filtered.drop_duplicates(subset=["uri"], keep="first")

    logger.info(
        f"Joined {len(filtered)} preprocessed posts to {len(uris)} in-feed URIs "
        f"for partition_date={partition_date}"
    )
    return filtered  # type: ignore
