"""Exports the results of classifying posts."""

import os
import shutil
from typing import Literal

from lib.aws.s3 import S3
from services.ml_inference.models import SociopoliticalLabelsModel
from services.ml_inference.sociopolitical.constants import (
    root_cache_path,
    sociopolitical_root_s3_key,
)
from services.ml_inference.sociopolitical.load_data import (
    load_classified_posts_from_cache,
)  # noqa

s3 = S3()


def write_post_to_cache(
    classified_post: SociopoliticalLabelsModel,
    source_feed: Literal["firehose", "most_liked"],
):
    """Writes a post to local cache."""
    post_id = classified_post.uri.split("/")[-1]
    author_did = classified_post.uri.split("/")[-3]
    joint_pk = f"{author_did}_{post_id}"
    full_key = os.path.join(root_cache_path, source_feed, f"{joint_pk}.json")
    with open(full_key, "w") as f:
        f.write(classified_post.json())


def delete_cache_paths():
    """Deletes the cache paths."""
    if os.path.exists(root_cache_path):
        shutil.rmtree(root_cache_path)


def rebuild_cache_paths():
    """Rebuilds the cache paths."""
    if not os.path.exists(root_cache_path):
        os.makedirs(root_cache_path)
    firehose_path = os.path.join(root_cache_path, "firehose")
    most_liked_path = os.path.join(root_cache_path, "most_liked")
    for path in [firehose_path, most_liked_path]:
        if not os.path.exists(path):
            os.makedirs(path)


def export_results(
    current_timestamp: str, external_stores: list[Literal["s3", "local"]]
):
    """Exports the results of classifying posts to an external store."""
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=current_timestamp
    )
    filename = "classified_posts.jsonl"
    posts_from_cache_dict: dict = load_classified_posts_from_cache()
    firehose_posts: list[SociopoliticalLabelsModel] = posts_from_cache_dict["firehose"]  # noqa
    most_liked_posts: list[SociopoliticalLabelsModel] = posts_from_cache_dict[
        "most_liked"
    ]  # noqa
    source_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa

    for source, posts in source_to_posts_tuples:
        classified_post_dicts = [post.dict() for post in posts]
        for external_store in external_stores:
            full_key = os.path.join(
                sociopolitical_root_s3_key, source, partition_key, filename
            )
            if external_store == "s3":
                s3.write_dicts_jsonl_to_s3(data=classified_post_dicts, key=full_key)  # noqa
    # TODO: trigger Glue crawler.

    delete_cache_paths()
    rebuild_cache_paths()

    return {
        "total_classified_posts": len(firehose_posts) + len(most_liked_posts),
        "total_classified_posts_by_source": {
            "firehose": len(firehose_posts),
            "most_liked": len(most_liked_posts),
        },
    }


# in case we need to rebuild the cache paths before running the script.
rebuild_cache_paths()
