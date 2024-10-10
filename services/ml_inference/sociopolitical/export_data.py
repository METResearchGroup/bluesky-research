"""Exports the results of classifying posts."""

import os
import shutil
from typing import Literal

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import timestamp_format
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.ml_inference.models import SociopoliticalLabelsModel
from services.ml_inference.sociopolitical.constants import (
    root_cache_path,
)
from services.ml_inference.sociopolitical.load_data import (
    load_classified_posts_from_cache,
)  # noqa

athena = Athena()
glue = Glue()
s3 = S3()

logger = get_logger(__name__)


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


def export_classified_posts() -> dict:
    """Exports classified posts."""
    posts_from_cache_dict: dict = load_classified_posts_from_cache()
    firehose_posts: list[SociopoliticalLabelsModel] = posts_from_cache_dict["firehose"]  # noqa
    most_liked_posts: list[SociopoliticalLabelsModel] = posts_from_cache_dict[
        "most_liked"
    ]  # noqa
    source_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa
    dtype_map = MAP_SERVICE_TO_METADATA["ml_inference_sociopolitical"]["dtypes_map"]
    breakpoint()
    for source, posts in source_to_posts_tuples:
        if len(posts) == 0:
            continue
        classified_post_dicts = [post.dict() for post in posts]
        df = pd.DataFrame(classified_post_dicts)
        df["partition_date"] = pd.to_datetime(
            df["label_timestamp"], format=timestamp_format
        ).dt.date
        df["source"] = source
        df = df.astype(dtype_map)
        logger.info(f"Exporting {len(df)} posts from {source} to local storage.")
        breakpoint()
        export_data_to_local_storage(
            service="ml_inference_sociopolitical", df=df, custom_args={"source": source}
        )
    return {
        "total_classified_posts": len(firehose_posts) + len(most_liked_posts),
        "total_classified_posts_by_source": {
            "firehose": len(firehose_posts),
            "most_liked": len(most_liked_posts),
        },
    }


def export_results() -> dict:
    """Exports the results of classifying posts to an external store."""
    results = export_classified_posts()
    delete_cache_paths()
    rebuild_cache_paths()
    return results


# in case we need to rebuild the cache paths before running the script.
rebuild_cache_paths()
