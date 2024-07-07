"""Helper functions for calculating superposters."""
from datetime import datetime, timedelta, timezone
from typing import Literal
import os

import pandas as pd

from lib.aws.s3 import S3
from lib.constants import (
    current_datetime_str, root_local_data_directory, timestamp_format
)
from lib.db.manage_local_data import load_jsonl_data, write_jsons_to_local_store  # noqa
from services.preprocess_raw_data.export_data import s3_export_key_map

superposter_threshold = 5

s3 = S3()

s3_export_root_key = "superposters"


def load_posts(
    source: Literal["s3", "local"],
    source_feeds: list[str] = ["firehose", "most_liked"],
    lookback_hours: int = 24
) -> list[dict]:
    """Loads the latest preprocessed posts.

    Has a default lookback period that we use to determine the earliest
    preprocessed posts that we'll load.
    """
    lookback_timestamp: str = (
        datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    ).strftime(timestamp_format)
    # NOTE: assumes lookback_hours <= 24, else you can have an edge case
    # where there's a day in between the two partition keys.
    current_date_partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=current_datetime_str
    )
    lookback_partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=lookback_timestamp
    )

    # remove last 2 subdirs from partition keys, since those are hour and
    # minute values.
    current_date_partition_key = os.path.join(*current_date_partition_key.split("/")[:-2])  # noqa
    lookback_partition_key = os.path.join(*lookback_partition_key.split("/")[:-2])  # noqa

    partition_keys = [lookback_partition_key, current_date_partition_key]

    if source == "s3":
        jsonl_data: list[dict] = []
        for source_feed in source_feeds:
            s3_keys = []
            for partition_key in partition_keys:
                prefix = os.path.join(
                    s3_export_key_map["post"], source_feed, partition_key
                )
                keys = s3.list_keys_given_prefix(prefix=prefix)
                s3_keys.extend(keys)
            for key in s3_keys:
                data = s3.read_jsonl_from_s3(key)
                jsonl_data.extend(data)
    elif source == "local":
        jsonl_data: list[dict] = []
        for source_feed in source_feeds:
            for partition_key in partition_keys:
                full_import_filedir = os.path.join(
                    root_local_data_directory,
                    s3_export_key_map["post"],
                    source_feed,
                    partition_key
                )
                files_to_load = []
                for root, _, files in os.walk(full_import_filedir):
                    for file in files:
                        files_to_load.append(os.path.join(root, file))
                for filepath in files_to_load:
                    data = load_jsonl_data(filepath)
                    jsonl_data.extend(data)
    return jsonl_data


def export_superposters(
    superposters: pd.DataFrame,
    external_stores: list[Literal["local", "s3"]] = ["local", "s3"]
):
    """Export superposters."""
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=current_datetime_str
    )
    filename = f"superposters_{current_datetime_str}.jsonl"
    superposters_dicts = superposters.to_dict(orient="records")
    full_key = os.path.join(s3_export_root_key, partition_key, filename)  # noqa
    for external_store in external_stores:
        if external_store == "s3":
            s3.write_dicts_jsonl_to_s3(data=superposters_dicts, key=full_key)
        elif external_store == "local":
            full_export_filepath = os.path.join(
                root_local_data_directory, full_key
            )
            write_jsons_to_local_store(
                records=superposters_dicts,
                export_filepath=full_export_filepath
            )
        else:
            raise ValueError("Invalid export store.")


def calculate_latest_superposters():
    """Calculate the latest superposters."""
    posts: list[dict] = load_posts(source="s3")
    df = pd.DataFrame(posts)

    superposters = df['author_did'].value_counts()
    superposters = superposters[superposters >= superposter_threshold]

    superposter_counts_df = pd.DataFrame(superposters).reset_index()
    superposter_counts_df.columns = ['author_did', 'number_of_posts']
    superposter_counts_df["superposter_calculation_timestamp"] = current_datetime_str  # noqa
    superposter_counts_df["insert_timestamp"] = current_datetime_str
    export_superposters(superposter_counts_df)
