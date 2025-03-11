"""Enriches and hydrates the posts used in the feeds."""

from typing import Optional

import pandas as pd

from lib.db.manage_local_data import (
    load_data_from_local_storage,
    export_data_to_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import calculate_start_end_date_for_lookback, get_partition_dates
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.constants import (
    num_days_lookback,
    min_lookback_date,
)
from services.backfill.posts_used_in_feeds.load_data import (
    load_posts_used_in_feeds,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
)
from services.backfill.posts.load_data import INTEGRATIONS_LIST

default_start_date = "2024-09-29"
default_end_date = "2024-10-08"

logger = get_logger(__file__)


def enrich_hydrate_posts(
    preprocessed_posts_used_in_feeds: pd.DataFrame,
    integration_to_df_map: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Enriches and hydrates the posts used in the feeds.

    Dedupes the preprocessed posts and then joins with the integration data
    iteratively.
    """
    # Start with deepcopy of preprocessed posts and dedupe
    result_df = preprocessed_posts_used_in_feeds.copy(deep=True)
    result_df = result_df.drop_duplicates(subset=["uri"])

    logger.info(f"Starting with {len(result_df)} preprocessed posts after deduping")

    # Loop through each integration and join
    for integration, integration_df in integration_to_df_map.items():
        # Dedupe integration df
        integration_df = integration_df.drop_duplicates(subset=["uri"])

        logger.info(
            f"Joining with {integration} integration containing {len(integration_df)} rows"
        )

        # Left join to keep all preprocessed posts
        pre_join_rows = len(result_df)
        result_df = result_df.merge(integration_df, on="uri", how="left")

        # Check if row count changed
        if len(result_df) != pre_join_rows:
            logger.warning(
                f"Row count changed after joining {integration}. "
                f"Pre-join: {pre_join_rows}, Post-join: {len(result_df)}"
            )

        logger.info(f"After joining {integration}, result has {len(result_df)} rows")

    return result_df


# TODO: Update this. I need to link the posts used in feeds to the
# preprocessed posts (like I do in the posts_used_in_feeds backfill).
def enrich_hydrate_posts_for_partition_date(
    partition_date: str, integrations: Optional[list[str]] = None
) -> None:
    """Enriches and hydrates the posts used in the feeds for a given partition date."""

    posts_used_in_feeds: pd.DataFrame = load_posts_used_in_feeds(
        partition_date=partition_date,
    )
    preprocessed_posts_used_in_feeds: list[dict] = (
        load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date,
        )
    )
    preprocessed_posts_used_in_feeds = pd.DataFrame(preprocessed_posts_used_in_feeds)

    assert len(posts_used_in_feeds) == len(preprocessed_posts_used_in_feeds), (
        "The number of posts used in feeds and preprocessed posts used in feeds "
        "should be the same."
    )

    integrations = integrations or INTEGRATIONS_LIST
    integration_to_df_map: dict[str, pd.DataFrame] = {}

    logger.info(
        f"Loading posts from integrations {integrations} for partition date {partition_date}"
    )

    for integration in integrations:
        start_date, end_date = calculate_start_end_date_for_lookback(
            partition_date=partition_date,
            num_days_lookback=num_days_lookback,
            min_lookback_date=min_lookback_date,
        )
        cached_df: pd.DataFrame = load_data_from_local_storage(
            service=integration,
            export_format="duckdb",
            duckdb_query=MAP_SERVICE_TO_METADATA[integration]["duckdb_query"],
            query_metadata={
                "tables": [
                    {
                        "name": integration,
                        "columns": MAP_SERVICE_TO_METADATA[integration]["columns"],
                    }
                ]
            },
            start_partition_date=start_date,
            end_partition_date=end_date,
        )
        active_df: pd.DataFrame = load_data_from_local_storage(
            service=integration,
            directory="active",
            duckdb_query=MAP_SERVICE_TO_METADATA[integration]["duckdb_query"],
            query_metadata={
                "tables": [
                    {
                        "name": integration,
                        "columns": MAP_SERVICE_TO_METADATA[integration]["columns"],
                    }
                ]
            },
            start_date=start_date,
            end_date=end_date,
            sorted_by_partition_date=False,
            ascending=False,
        )
        integration_df = pd.concat([cached_df, active_df])
        integration_to_df_map[integration] = integration_df
        logger.info(
            f"Loaded {len(integration_df)} posts from {integration} for partition date {partition_date}"
        )

    enriched_hydrated_posts: pd.DataFrame = enrich_hydrate_posts(
        preprocessed_posts_used_in_feeds=preprocessed_posts_used_in_feeds,
        integration_to_df_map=integration_to_df_map,
    )

    export_data_to_local_storage(
        service="enriched_hydrated_posts",
        df=enriched_hydrated_posts,
        export_format="parquet",
    )

    logger.info(
        f"Exported enriched and hydrated posts to local storage for partition date {partition_date}"
    )


def main(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if start_date is None:
        start_date = default_start_date
    if end_date is None:
        end_date = default_end_date
    partition_dates = get_partition_dates(start_date, end_date)
    for partition_date in partition_dates:
        enrich_hydrate_posts_for_partition_date(partition_date)
    logger.info(
        f"Completed enrichment and hydration of posts for {start_date} to {end_date}"
    )


if __name__ == "__main__":
    main()
