"""Loads data for Perspective API classification."""

import os
from typing import Optional

import pandas as pd

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.log.logger import get_logger
from lib.helper import track_performance
from services.ml_inference.models import PerspectiveApiLabelsModel
from services.ml_inference.perspective_api.constants import (
    root_cache_path,
)
from services.ml_inference.helper import load_cached_jsons_as_df

logger = get_logger(__name__)
s3 = S3()

dynamodb_table_name = "perspectiveApiClassificationMetadata"
dynamodb = DynamoDB()
dynamodb_table = dynamodb.resource.Table(dynamodb_table_name)


def load_previous_session_metadata() -> dict:
    """Loads the metadata from the previous classification session."""
    response = dynamodb_table.scan()
    items = response["Items"]
    if not items:
        return None
    latest_item = max(items, key=lambda x: x["current_classification_timestamp"])  # noqa
    return latest_item


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
        print(
            f"Cached directory for firehose posts doesn't exist. Creating it at {firehose_dir}"
        )  # noqa
        os.makedirs(firehose_dir)
    most_liked_dir = os.path.join(root_cache_path, "most_liked")
    if not os.path.exists(most_liked_dir):
        print(
            f"Cached directory for most liked posts doesn't exist. Creating it at {most_liked_dir}"
        )

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
                print(
                    f"Cached directory for {validity} {feed_dir} posts doesn't exist. Creating it at {full_dir}"
                )  # nqoa
                os.makedirs(full_dir)
            else:
                # loop through each file.
                # fetch the name, which is formatted as {author_did}_{post_id}.json # noqa
                # we reassemble the actual uri, which comes in the form
                # at://{author_did}/app.bsky.feed.post/{post_id}
                for filename in os.listdir(full_dir):
                    joint_pk = filename.split(".")[0]
                    author_did, post_id = joint_pk.split("_")
                    uri = f"at://{author_did}/app.bsky.feed.post/{post_id}"
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
            "invalid": most_liked_invalid_uris,
        },
    }


@track_performance
def load_classified_posts_from_cache() -> dict:
    """Loads all the classified posts from cache into memory.

    Note: shouldn't be too much memory, though it depends in practice on how
    often this service is run.
    """
    firehose_path = os.path.join(root_cache_path, "firehose")
    most_liked_path = os.path.join(root_cache_path, "most_liked")
    firehose_valid_path = os.path.join(firehose_path, "valid")
    firehose_invalid_path = os.path.join(firehose_path, "invalid")
    most_liked_valid_path = os.path.join(most_liked_path, "valid")
    most_liked_invalid_path = os.path.join(most_liked_path, "invalid")

    firehose_valid_posts: list[PerspectiveApiLabelsModel] = []
    firehose_invalid_posts: list[PerspectiveApiLabelsModel] = []
    most_liked_valid_posts: list[PerspectiveApiLabelsModel] = []
    most_liked_invalid_posts: list[PerspectiveApiLabelsModel] = []

    firehose_valid_paths = []
    firehose_invalid_paths = []
    most_liked_valid_paths = []
    most_liked_invalid_paths = []

    for path in [
        firehose_valid_path,
        firehose_invalid_path,
        most_liked_valid_path,
        most_liked_invalid_path,
    ]:
        paths = os.listdir(path)
        if path == firehose_valid_path:
            firehose_valid_paths.extend([os.path.join(path, p) for p in paths])
        elif path == firehose_invalid_path:
            firehose_invalid_paths.extend([os.path.join(path, p) for p in paths])
        elif path == most_liked_valid_path:
            most_liked_valid_paths.extend([os.path.join(path, p) for p in paths])
        elif path == most_liked_invalid_path:
            most_liked_invalid_paths.extend([os.path.join(path, p) for p in paths])

    df_firehose_valid: Optional[pd.DataFrame] = load_cached_jsons_as_df(firehose_valid_paths)
    df_firehose_invalid: Optional[pd.DataFrame] = load_cached_jsons_as_df(firehose_invalid_paths)
    df_most_liked_valid: Optional[pd.DataFrame] = load_cached_jsons_as_df(most_liked_valid_paths)
    df_most_liked_invalid: Optional[pd.DataFrame] = load_cached_jsons_as_df(most_liked_invalid_paths)

    df_dicts_firehose_valid = (
        df_firehose_valid.to_dict(orient="records")
        if df_firehose_valid is not None
        else []
    )
    df_dicts_firehose_invalid = (
        df_firehose_invalid.to_dict(orient="records")
        if df_firehose_invalid is not None
        else []
    )
    df_dicts_most_liked_valid = (
        df_most_liked_valid.to_dict(orient="records")
        if df_most_liked_valid is not None
        else []
    )
    df_dicts_most_liked_invalid = (
        df_most_liked_invalid.to_dict(orient="records")
        if df_most_liked_invalid is not None
        else []
    )

    firehose_valid_posts = [
        PerspectiveApiLabelsModel(**post_dict)
        for post_dict in df_dicts_firehose_valid
    ]
    firehose_invalid_posts = [
        PerspectiveApiLabelsModel(**post_dict)
        for post_dict in df_dicts_firehose_invalid
    ]
    most_liked_valid_posts = [
        PerspectiveApiLabelsModel(**post_dict)
        for post_dict in df_dicts_most_liked_valid
    ]
    most_liked_invalid_posts = [
        PerspectiveApiLabelsModel(**post_dict)
        for post_dict in df_dicts_most_liked_invalid
    ]

    return {
        "firehose": {
            "valid": firehose_valid_posts,
            "invalid": firehose_invalid_posts,
        },
        "most_liked": {
            "valid": most_liked_valid_posts,
            "invalid": most_liked_invalid_posts,
        },
    }
