"""Export data from the IME classification."""

import json
import os
import shutil
from typing import Literal
import uuid

import pandas as pd

from lib.constants import timestamp_format
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

from services.ml_inference.ime.constants import (
    root_cache_path,
)
from services.ml_inference.ime.load_data import (
    load_classified_posts_from_cache,
)

logger = get_logger(__name__)


def write_posts_to_cache(
    posts: pd.DataFrame,
    source_feed: Literal["firehose", "most_liked"],
    classification_type: Literal["valid", "invalid"],
):
    hashed_value = str(uuid.uuid4())
    timestamp = generate_current_datetime_str()
    filename = f"{source_feed}_{classification_type}_{timestamp}_{hashed_value}.jsonl"
    full_dir = os.path.join(root_cache_path, source_feed, classification_type)
    if not os.path.exists(full_dir):
        os.makedirs(full_dir)
    full_key = os.path.join(full_dir, filename)
    post_dicts = posts.to_dict(orient="records")
    with open(full_key, "w") as f:
        for post in post_dicts:
            f.write(json.dumps(post) + "\n")


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
    """Exports the results of classifying posts to an external store."""
    posts_from_cache_dict: dict = load_classified_posts_from_cache()
    firehose_posts: pd.DataFrame = posts_from_cache_dict["firehose"]
    most_liked_posts: pd.DataFrame = posts_from_cache_dict["most_liked"]

    source_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]

    dtypes_map = MAP_SERVICE_TO_METADATA["ml_inference_ime"]["dtypes_map"]
    for source, df in source_to_posts_tuples:
        if df.empty:
            logger.info(f"No posts to export for {source}.")
            continue
        df = df.astype(dtypes_map)
        df["partition_date"] = pd.to_datetime(
            df["label_timestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtypes_map)
        logger.info(f"Exporting {len(df)} posts from {source} to local storage.")
        export_data_to_local_storage(
            service="ml_inference_ime", df=df, custom_args={"source": source}
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
