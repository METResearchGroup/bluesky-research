"""Verify the number of records currently in storage."""

from datetime import datetime

import click
import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__name__)


def get_num_cached_records_by_partition_date(integration: str) -> dict[str, int]:
    """Get the number of records by partition date."""
    queue = Queue(
        f"output_{integration}"
    )  # we only want the output queues, since this is what we're writing to the DB.
    records: list[dict] = queue.load_dict_items_from_queue()

    partition_date_to_count_map = {}
    for record in records:
        timestamp = record["preprocessing_timestamp"]
        partition_date = datetime.strptime(timestamp, "%Y-%m-%d-%H:%M:%S").strftime(
            "%Y-%m-%d"
        )
        if partition_date not in partition_date_to_count_map:
            partition_date_to_count_map[partition_date] = 0
        partition_date_to_count_map[partition_date] += 1

    # Sort partition dates chronologically
    sorted_dates = sorted(partition_date_to_count_map.keys())

    print("\nNumber of cache queue records by date:")
    total_records = 0
    for date in sorted_dates:
        count = partition_date_to_count_map[date]
        total_records += count
        print(f"{date} {count}")

    print(f"\nTotal: {total_records}")

    return partition_date_to_count_map


def get_total_current_db_records_by_partition_date(
    integration: str, partition_dates: str
) -> dict[str, int]:
    """Get the total number of records in the database by partition date."""
    partition_date_to_count_map = {}
    for partition_date in partition_dates:
        df: pd.DataFrame = load_data_from_local_storage(
            service=integration,
            directory="cache",
            partition_date=partition_date,
        )
        partition_date_to_count_map[partition_date] = len(df)

    # Sort partition dates chronologically
    sorted_dates = sorted(partition_date_to_count_map.keys())

    print("\nNumber of existing DB records by date:")
    total_records = 0
    for date in sorted_dates:
        count = partition_date_to_count_map[date]
        total_records += count
        print(f"{date} {count}")

    print(f"\nTotal: {total_records}")
    return partition_date_to_count_map


def verify_storage(integration: str):
    """Main function to verify storage."""
    num_cached_records_by_partition_date = get_num_cached_records_by_partition_date(
        integration=integration
    )
    if num_cached_records_by_partition_date:
        num_db_records_by_partition_date = (
            get_total_current_db_records_by_partition_date(
                integration=integration,
                partition_dates=num_cached_records_by_partition_date.keys(),
            )
        )
        total_expected_records_by_partition_date = {
            partition_date: num_cached_records_by_partition_date[partition_date]
            + num_db_records_by_partition_date[partition_date]
            for partition_date in num_cached_records_by_partition_date.keys()
        }
    else:
        logger.info("No cached records found. Skipping DB verification.")
        print("Total cached records: 0")
        return

    sorted_dates = sorted(total_expected_records_by_partition_date.keys())

    print("\nTotal expected records:")
    total_expected_records = 0
    total_cached_records = 0
    total_existing_records = 0
    for date in sorted_dates:
        cached_count = num_cached_records_by_partition_date[date]
        existing_count = num_db_records_by_partition_date[date]
        total_count = total_expected_records_by_partition_date[date]

        total_cached_records += cached_count
        total_existing_records += existing_count
        total_expected_records += total_count

        print(
            f"{date} {total_count} ({cached_count} cached, {existing_count} existing, {cached_count} + {existing_count} = {total_count})"
        )

    print(
        f"\nTotal: {total_expected_records} ({total_cached_records} cached, {total_existing_records} existing, {total_cached_records} + {total_existing_records} = {total_expected_records})"
    )


@click.command()
@click.option(
    "--integration",
    type=click.Choice(
        [
            "ml_inference_perspective_api",
            "ml_inference_sociopolitical",
            "ml_inference_ime",
        ]
    ),
    required=True,
    help="Integration to verify storage for",
)
def main(integration):
    verify_storage(integration)


if __name__ == "__main__":
    main()
