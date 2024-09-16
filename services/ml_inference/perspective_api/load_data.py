"""Loads data for Perspective API classification."""

import json
import os
from typing import Literal, Optional

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.helper import track_performance
from services.ml_inference.models import PerspectiveApiLabelsModel
from services.ml_inference.perspective_api.constants import (
    perspective_api_root_s3_key,
    previously_classified_post_uris_filename,
    root_cache_path,
)

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


def load_previously_classified_post_uris(source: Literal["local", "s3"]) -> set[str]:  # noqa
    """Loads previously classified post data.

    Sets are not JSON-serializable, so we actually store the data as a list.

    The URIs are in their original form
    (i.e., at://{author_did}/app.bsky.feed.post/{post_id}) even though at
    certain points, we extract the author DID and post ID to create a unique
    key for the post.
    """
    full_key = os.path.join(
        perspective_api_root_s3_key, previously_classified_post_uris_filename
    )
    if source == "s3":
        previously_classified_uris: Optional[list[str]] = s3.read_json_from_s3(
            key=full_key
        )  # noqa
    elif source == "local":
        full_import_filepath = os.path.join(root_local_data_directory, full_key)
        with open(full_import_filepath, "r") as f:
            previously_classified_uris: Optional[list[str]] = json.load(f)
    if not previously_classified_uris:
        print("No previously classified post URIs found.")
        return set()
    return set(previously_classified_uris)


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

    for path in [
        firehose_valid_path,
        firehose_invalid_path,
        most_liked_valid_path,
        most_liked_invalid_path,
    ]:
        for filename in os.listdir(path):
            full_path = os.path.join(path, filename)
            with open(full_path, "r") as f:
                post = json.load(f)
                if "firehose" in full_path:
                    # need to have invalid before valid since "valid" is a
                    # subset of "invalid" (all strings with "invalid"
                    # also have "valid" in them, but not vice versa)
                    if "invalid" in full_path:
                        firehose_invalid_posts.append(PerspectiveApiLabelsModel(**post))
                    elif "valid" in full_path:
                        firehose_valid_posts.append(PerspectiveApiLabelsModel(**post))
                elif "most_liked" in full_path:
                    if "invalid" in full_path:
                        most_liked_invalid_posts.append(
                            PerspectiveApiLabelsModel(**post)
                        )
                    elif "valid" in full_path:
                        most_liked_valid_posts.append(PerspectiveApiLabelsModel(**post))

    return {
        "firehose": {"valid": firehose_valid_posts, "invalid": firehose_invalid_posts},
        "most_liked": {
            "valid": most_liked_valid_posts,
            "invalid": most_liked_invalid_posts,
        },
    }
