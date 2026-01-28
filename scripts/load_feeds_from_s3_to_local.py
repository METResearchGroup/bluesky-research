"""Load archived generated_feeds from S3 to local storage.

This script loads feeds from the archived S3 location:
s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/generated_feeds/cache/

The feeds are partitioned by partition_date and are loaded date by date
from study_start_date to study_end_date. For each date, it compares the
local and S3 versions and prompts for confirmation before downloading
if they differ.
"""

import pandas as pd

from lib.constants import study_end_date, study_start_date
from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.db.manage_s3_data import S3ParquetBackend, S3ParquetDatasetRef
from lib.db.models import StorageTier
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger

logger = get_logger(__name__)

# Get column names from service constants
GENERATED_FEEDS_COLUMNS = list(
    MAP_SERVICE_TO_METADATA["generated_feeds"]["dtypes_map"].keys()
)


def compare_dataframes(df_local: pd.DataFrame, df_s3: pd.DataFrame) -> dict:
    """Compare two DataFrames by feed_id and return comparison results.

    Args:
        df_local: Local DataFrame
        df_s3: S3 DataFrame

    Returns:
        Dictionary with comparison results:
        - identical: bool
        - local_feed_ids: set of feed_ids in local
        - s3_feed_ids: set of feed_ids in S3
        - only_in_local: set of feed_ids only in local
        - only_in_s3: set of feed_ids only in S3
        - in_both: set of feed_ids in both
    """
    local_feed_ids = (
        set(df_local["feed_id"].unique())
        if not df_local.empty and "feed_id" in df_local.columns
        else set()
    )
    s3_feed_ids = (
        set(df_s3["feed_id"].unique())
        if not df_s3.empty and "feed_id" in df_s3.columns
        else set()
    )

    only_in_local = local_feed_ids - s3_feed_ids
    only_in_s3 = s3_feed_ids - local_feed_ids
    in_both = local_feed_ids & s3_feed_ids

    identical = (
        len(only_in_local) == 0
        and len(only_in_s3) == 0
        and len(local_feed_ids) == len(s3_feed_ids)
        and len(local_feed_ids) > 0
    )

    return {
        "identical": identical,
        "local_feed_ids": local_feed_ids,
        "s3_feed_ids": s3_feed_ids,
        "only_in_local": only_in_local,
        "only_in_s3": only_in_s3,
        "in_both": in_both,
    }


def load_and_write_feeds_for_partition_date(partition_date: str) -> int:
    """Load feeds for a given partition date from S3 and write to local storage.

    Compares local and S3 versions, shows differences, and prompts for confirmation
    before downloading if they differ.

    Args:
        partition_date: Partition date in YYYY-MM-DD format

    Returns:
        Number of feeds loaded and written (0 if no data found or user declined)
    """
    logger.info(f"Processing partition_date={partition_date}...")

    try:
        # Load local data if it exists
        logger.info(f"Loading local feeds for partition_date={partition_date}...")
        df_local = load_data_from_local_storage(
            service="generated_feeds",
            storage_tiers=[StorageTier.CACHE],
            partition_date=partition_date,
            source_file_format="parquet",
        )

        local_count = len(df_local) if not df_local.empty else 0
        logger.info(f"Found {local_count} feeds in local storage")

        # Load S3 data
        logger.info(f"Loading feeds from S3 for partition_date={partition_date}...")
        backend = S3ParquetBackend()
        dataset = S3ParquetDatasetRef(dataset="generated_feeds")

        # Query metadata for DuckDB
        query_metadata = {
            "tables": [
                {
                    "name": "generated_feeds",
                    "columns": GENERATED_FEEDS_COLUMNS,
                }
            ]
        }

        df_s3 = backend.query_dataset_as_df(
            dataset=dataset,
            storage_tiers=[StorageTier.CACHE],
            partition_date=partition_date,
            query="SELECT * FROM generated_feeds",
            query_metadata=query_metadata,
        )

        s3_count = len(df_s3) if not df_s3.empty else 0
        logger.info(f"Found {s3_count} feeds in S3")

        # Handle empty cases
        if df_s3.empty and df_local.empty:
            logger.warning(
                f"No feeds found in either local or S3 for partition_date={partition_date}. Skipping."
            )
            return 0

        if df_s3.empty:
            logger.warning(
                f"No feeds found in S3 for partition_date={partition_date}, but {local_count} found locally. Skipping."
            )
            return 0

        if df_local.empty:
            logger.info(
                f"No local feeds found for partition_date={partition_date}, but {s3_count} found in S3."
            )
            # Proceed to download since there's no local data
            logger.info(f"Writing {s3_count} feeds to local storage...")
            export_data_to_local_storage(
                service="generated_feeds",
                df=df_s3,
                export_format="parquet",
            )
            logger.info(
                f"Successfully wrote {s3_count} feeds for partition_date={partition_date} to local storage."
            )
            return s3_count

        # Compare DataFrames
        comparison = compare_dataframes(df_local, df_s3)

        # Print comparison results
        print("\n" + "=" * 60)
        print(f"COMPARISON RESULTS for partition_date={partition_date}")
        print("=" * 60)
        print(f"Local feeds: {len(comparison['local_feed_ids'])} unique feed_ids")
        print(f"S3 feeds: {len(comparison['s3_feed_ids'])} unique feed_ids")
        print(f"Feed IDs in both: {len(comparison['in_both'])}")
        print(f"Feed IDs only in local: {len(comparison['only_in_local'])}")
        print(f"Feed IDs only in S3: {len(comparison['only_in_s3'])}")

        if comparison["identical"]:
            print("\n✅ DataFrames are IDENTICAL - no download needed.")
            print("=" * 60 + "\n")
            return 0
        else:
            print("\n❌ DataFrames are NOT IDENTICAL")
            print(f"   - {len(comparison['only_in_local'])} feed_ids only in local")
            print(f"   - {len(comparison['only_in_s3'])} feed_ids only in S3")
            print("=" * 60 + "\n")

            # Prompt for confirmation
            response = (
                input(
                    f"Do you want to download S3 data for partition_date={partition_date} "
                    f"and overwrite local? (yes/no): "
                )
                .strip()
                .lower()
            )

            if response not in ["yes", "y"]:
                logger.info(
                    f"User declined to download for partition_date={partition_date}. Skipping."
                )
                return 0

            # Write to local storage
            logger.info(f"Writing {s3_count} feeds to local storage...")
            export_data_to_local_storage(
                service="generated_feeds",
                df=df_s3,
                export_format="parquet",
            )
            logger.info(
                f"Successfully wrote {s3_count} feeds for partition_date={partition_date} "
                f"to local storage."
            )
            return s3_count

    except Exception as e:
        logger.error(
            f"Error loading/writing feeds for partition_date={partition_date}: {e}",
            exc_info=True,
        )
        raise


def main() -> None:
    """Main function that loads feeds for all partition dates in the study period."""
    logger.info(
        f"Starting to load feeds from S3 to local storage "
        f"for date range: {study_start_date} to {study_end_date}"
    )

    # Get all partition dates in the study period
    partition_dates = get_partition_dates(
        start_date=study_start_date,
        end_date=study_end_date,
    )

    logger.info(f"Processing {len(partition_dates)} partition dates...")

    total_feeds_loaded = 0
    successful_dates = 0
    failed_dates = 0

    for partition_date in partition_dates:
        try:
            count = load_and_write_feeds_for_partition_date(partition_date)
            total_feeds_loaded += count
            if count > 0:
                successful_dates += 1
            else:
                logger.info(f"Skipped partition_date={partition_date} (no data found)")
        except Exception as e:
            failed_dates += 1
            logger.error(
                f"Failed to process partition_date={partition_date}: {e}",
                exc_info=True,
            )
            # Continue processing other dates even if one fails
            continue

    # Summary
    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total partition dates processed: {len(partition_dates)}")
    logger.info(f"Successful dates (with data): {successful_dates}")
    logger.info(
        f"Dates with no data: {len(partition_dates) - successful_dates - failed_dates}"
    )
    logger.info(f"Failed dates: {failed_dates}")
    logger.info(f"Total feeds loaded and written: {total_feeds_loaded}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
