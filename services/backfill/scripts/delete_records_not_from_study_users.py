"""Deletes records that are not from study users.

Used to fix a mistake where we got the wrong records for users who are not
in the study, so we want to delete those and resync.
"""

import os
import shutil

from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


logger = get_logger(__name__)

users: list[UserToBlueskyProfileModel] = get_all_users()
user_dids: list[str] = [user.bluesky_user_did for user in users]

# end format:
# {
#     "post": {
#         "2024-11-20": {
#            "before_filter": 100,
#            "after_filter": 50,
#         },
#         "2024-11-21": {
#            "before_filter": 100,
#            "after_filter": 50,
#         },
#     },
#     "reply": {},
#     "repost": {},
# }
record_date_to_record_count = {}
record_date_to_record_count["post"] = {}
record_date_to_record_count["reply"] = {}
record_date_to_record_count["repost"] = {}


def return_fp_for_records(record_type: str, date: str) -> str:
    """Returns the filepath for the records for a given date."""
    return os.path.join(
        root_local_data_directory,
        "raw_sync",
        "create",
        record_type,
        "cache",
        f"partition_date={date}",
    )


def delete_and_rewrite_records_for_date(record_type: str, date: str) -> None:
    """Deletes and rewrites records for a given date.

    Steps:
    1. Loads the records for the given date.
    2. Deletes the records that are not from study users.
    3. Deletes the old .parquet files.
    4. Writes the records to the sync storage.
    """
    df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        partition_date=date,
        custom_args={"record_type": record_type},
    )
    len_before_filter = len(df)
    logger.info(
        f"(Record type: {record_type}, Date: {date}) Loaded {len_before_filter} records."
    )
    df = df[df["author"].isin(user_dids)]
    len_after_filter = len(df)
    logger.info(
        f"(Record type: {record_type}, Date: {date}) Filtered to {len_after_filter} records."
    )

    if date not in record_date_to_record_count[record_type]:
        record_date_to_record_count[record_type][date] = {}
    record_date_to_record_count[record_type][date]["before_filter"] = len_before_filter
    record_date_to_record_count[record_type][date]["after_filter"] = len_after_filter

    # # Delete the old .parquet files.
    fp = return_fp_for_records(record_type, date)
    if os.path.exists(fp):
        shutil.rmtree(fp)
    logger.info(
        f"(Record type: {record_type}, Date: {date}) Deleted old .parquet files at {fp}."
    )

    if len(df) > 0:
        # Write the new .parquet files.
        export_data_to_local_storage(
            service="raw_sync",
            df=df,
            custom_args={"record_type": record_type},
        )
        logger.info(
            f"(Record type: {record_type}, Date: {date}) Wrote {len(df)} records to {fp}."
        )
    else:
        logger.info(f"(Record type: {record_type}, Date: {date}) No records to write.")


def delete_and_rewrite_records_for_date_range(
    record_type: str,
    start_date: str,
    end_date: str,
) -> None:
    """Deletes and rewrites records for a given date range.

    Steps:
    1. Loads the records for the given date range.
    2. Deletes the records that are not from study users.
    3. Deletes the old .parquet files.
    4. Writes the records to the sync storage.
    """
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=[],
    )

    for date in partition_dates:
        delete_and_rewrite_records_for_date(record_type, date)


def delete_and_rewrite_records(start_date: str, end_date: str) -> None:
    """Deletes and rewrites records for a given date range."""
    record_types = ["post", "reply", "repost"]

    for record_type in record_types:
        logger.info(
            f"Deleting and rewriting records for {record_type} from {start_date} to {end_date}."
        )
        delete_and_rewrite_records_for_date_range(record_type, start_date, end_date)
        logger.info(
            f"Finished deleting and rewriting records for {record_type} from {start_date} to {end_date}."
        )


def main():
    # NOTE: check that `backup_raw_sync_data.py` is run first.
    start_date = "2024-09-01"
    end_date = "2024-12-01"
    delete_and_rewrite_records(start_date, end_date)


if __name__ == "__main__":
    main()
