"""Loads data for Perspective API classification."""
import json
import os
from typing import Literal, Optional

from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    find_files_after_timestamp, load_jsonl_data
)
from services.ml_inference.perspective_api.constants import (
    perspective_api_root_s3_key, previously_classified_post_uris_filename,
    root_cache_path
)
from services.preprocess_raw_data.export_data import s3_export_key_map
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

s3 = S3()


def load_previously_classified_post_uris(source: Literal["local", "s3"]) -> set[str]:  # noqa
    """Loads previously classified post data.

    Sets are not JSON-serializable, so we actually store the data as a list.
    """
    full_key = os.path.join(
        perspective_api_root_s3_key, previously_classified_post_uris_filename
    )
    if source == "s3":
        previously_classified_uris: Optional[list[str]] = S3.read_json_from_s3(key=full_key)  # noqa
    elif source == "local":
        full_import_filepath = os.path.join(
            root_local_data_directory, full_key
        )
        with open(full_import_filepath, "r") as f:
            previously_classified_uris: Optional[list[str]] = json.load(f)
    if not previously_classified_uris:
        print("No previously classified post URIs found.")
        return set()
    return set(previously_classified_uris)


# TODO: load from DynamoDB.
def load_previous_session_metadata() -> dict:
    """Loads the metadata from the previous classification session."""
    pass


def load_cached_post_uris() -> dict:
    """Returns the URIs of posts that have already been classified by the
    Perspective API. This is useful for filtering out posts that have already
    been classified but haven't been exported to an external store yet, so that
    we don't classify them again.

    Each cached post is saved as {uri}.json, so we extract the list of these
    URIs and return those as our results.
    """
    firehose_dir = os.path.join(root_cache_path, "firehose")
    if not os.path.exists(firehose_dir):
        print(f"Cached directory for firehose posts doesn't exist. Creating it at {firehose_dir}")  # noqa
        os.makedirs(firehose_dir)
    most_liked_dir = os.path.join(root_cache_path, "most_liked")
    if not os.path.exists(most_liked_dir):
        print(f"Cached directory for most liked posts doesn't exist. Creating it at {most_liked_dir}")

    firehose_valid_uris = set()
    firehose_invalid_uris = set()
    most_liked_valid_uris = set()
    most_liked_invalid_uris = set()

    # loop through all the filenames in each cached subdirectory, add the URIs
    # of the files.
    for validity in ["valid", "invalid"]:
        for feed_dir in [firehose_dir, most_liked_dir]:
            full_dir = os.path.join(feed_dir, validity)
            if not os.path.exists(full_dir):
                print(f"Cached directory for {validity} {feed_dir} posts doesn't exist. Creating it at {full_dir}")  # nqoa
                os.makedirs(full_dir)
            else:
                # loop through each file.
                # fetch the name, which is formatted as {uri}.json
                # add the {uri} portion to the corresponding set
                for filename in os.listdir(full_dir):
                    uri = filename.split(".")[0]
                    if feed_dir == firehose_dir:
                        if validity == "valid":
                            firehose_valid_uris.add(uri)
                        elif validity == "invalid":
                            firehose_invalid_uris.add(uri)
                    elif feed_dir == most_liked_dir:
                        if validity == "valid":
                            most_liked_valid_uris.add(uri)
                        elif validity == "invalid":
                            most_liked_invalid_uris.add(uri)

    return {
        "firehose": {
            "valid": firehose_valid_uris,
            "invalid": firehose_invalid_uris,
        },
        "most_liked": {
            "valid": most_liked_valid_uris,
            "invalid": most_liked_invalid_uris
        }
    }


def load_posts_to_classify(
    source: Literal["local", "s3"],
    source_feed: Literal["firehose", "most_liked"],
    session_metadata: dict
) -> list[FilteredPreprocessedPostModel]:
    """Loads posts for Perspective API classification. Loads posts which
    were added after the most recent batch of classified posts.

    Loads the timestamp of the latest preprocessed batch that was classified,
    so that any posts preprocessed after that batch can be classified.
    """
    previous_timestamp = session_metadata["previous_classification_timestamp"]
    latest_partition_timestamp = (
        S3.create_partition_key_based_on_timestamp(previous_timestamp)
    )
    prefix = os.path.join(s3_export_key_map["post"], source_feed)
    if source == "s3":
        keys = s3.list_keys_given_prefix(prefix=prefix)
        keys = [
            key for key in keys
            if key > os.path.join(prefix, latest_partition_timestamp)
        ]
        jsonl_data: list[dict] = []
        for key in keys:
            data = s3.read_jsonl_from_s3(key)
            jsonl_data.extend(data)
        transformed_jsonl_data: list[FilteredPreprocessedPostModel] = [
            FilteredPreprocessedPostModel(**post) for post in jsonl_data
        ]
    elif source == "local":
        full_import_filedir = os.path.join(root_local_data_directory, prefix)
        files_to_load: list[str] = find_files_after_timestamp(
            base_path=full_import_filedir,
            target_timestamp_path=previous_timestamp
        )
        jsonl_data: list[dict] = []
        for filepath in files_to_load:
            data = load_jsonl_data(filepath)
            jsonl_data.extend(data)
        transformed_jsonl_data: list[FilteredPreprocessedPostModel] = [
            FilteredPreprocessedPostModel(**post) for post in jsonl_data
        ]

    sorted_posts = sorted(
        transformed_jsonl_data, key=lambda x: x.synctimestamp, reverse=False
    )
    print(f"Number of posts loaded for classification using Perspective API: {len(sorted_posts)}")  # noqa
    return sorted_posts
