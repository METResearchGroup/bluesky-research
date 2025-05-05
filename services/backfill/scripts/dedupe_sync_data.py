"""Goes through each record type in the sync storage and deduplicates it."""

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import get_partition_dates


def get_total_duplicates_for_date(record_type: str, date: str) -> int:
    """Gets the total number of duplicates for a given record type and date."""
    df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        partition_date=date,
        custom_args={
            "record_type": record_type,
        },
    )
    total_dupes = df.duplicated(subset=["uri"]).sum()
    print(f"Total duplicates for {record_type} on {date}: {total_dupes}")
    return total_dupes


def get_total_duplicates_for_date_range(
    record_type: str, start_date: str, end_date: str
) -> int:
    """Gets the total number of duplicates for a given record type and date range."""
    total_duplicates = 0
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=[],
    )
    for date in partition_dates:
        total_duplicates += get_total_duplicates_for_date(record_type, date)
    print(
        f"Total duplicates for {record_type} from {start_date} to {end_date}: {total_duplicates}"
    )
    return total_duplicates


# TODO: first verify that there is a systematic example of duplicates.
def deduplicate_records(record_type: str) -> None:
    """Deduplicates records of a given type in the sync storage.

    Steps:
    1. Load records, day by day, from the sync storage, for the given record type.
    2. Deduplicates based on 'uri'.
    3. Writes the records to the sync storage as .parquet.
    4. List out the files for that date and record type.
    5. Delete files older than when this script was run (since those would
    be the old files)
    """
    pass


def main():
    # record_types = ["post", "reply", "repost", "like", "follow", "block"]
    record_types = ["post", "reply"]
    for record_type in record_types:
        get_total_duplicates_for_date_range(record_type, "2024-09-28", "2024-12-01")


if __name__ == "__main__":
    main()
