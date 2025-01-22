"""Exports the results of classifying posts to an external store."""

import os
import shutil
from typing import Literal, Optional

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory, timestamp_format
from lib.db.manage_local_data import (
    write_jsons_to_local_store,
    export_data_to_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.db.queue import Queue
from lib.helper import generate_current_datetime_str
from services.ml_inference.models import PerspectiveApiLabelsModel
from services.ml_inference.perspective_api.constants import (
    perspective_api_root_s3_key,
    previously_classified_post_uris_filename,
    root_cache_path,
)
from services.ml_inference.perspective_api.load_data import (
    load_classified_posts_from_cache,
)  # noqa

athena = Athena()
s3 = S3()
glue = Glue()

input_queue = Queue(
    queue_name="input_ml_inference_perspective_api",
    create_new_queue=True,
)
output_queue = Queue(
    queue_name="output_ml_inference_perspective_api",
    create_new_queue=True,
)


def return_failed_labels_to_input_queue(
    failed_label_models: list[PerspectiveApiLabelsModel],
    source_feed: Literal["firehose", "most_liked"],
    batch_size: Optional[int] = None,
):
    """Returns failed labels to the input queue."""
    input_queue.batch_add_items_to_queue(
        items=[{"uri": post.uri, "text": post.text} for post in failed_label_models],
        batch_size=batch_size,
        metadata={
            "reason": "failed_label_perspective_api",
            "model_reason": failed_label_models[
                0
            ].reason,  # pick the first one, since they all likely failed for the same reason.
            "label_timestamp": generate_current_datetime_str(),
            "source_feed": source_feed,
        },
    )


def write_posts_to_cache(
    posts: list[PerspectiveApiLabelsModel],
    source_feed: Literal["firehose", "most_liked"],
    batch_size: Optional[int] = None,
):
    """Write successfully classified posts to the queue storage."""

    output_queue.batch_add_items_to_queue(
        items=[post.dict() for post in posts],
        batch_size=batch_size,
        metadata={"source_feed": source_feed},
    )


def export_classified_posts() -> dict:
    """Export classified posts.

    Loads latest posts from cache and exports them to an external store.
    """
    posts_from_cache_dict: dict = load_classified_posts_from_cache()
    firehose_posts: list[PerspectiveApiLabelsModel] = posts_from_cache_dict["firehose"][
        "valid"
    ]  # noqa
    most_liked_posts: list[PerspectiveApiLabelsModel] = posts_from_cache_dict[
        "most_liked"
    ]["valid"]  # noqa
    source_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa
    dtype_map = MAP_SERVICE_TO_METADATA["ml_inference_perspective_api"]["dtypes_map"]
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
        export_data_to_local_storage(
            service="ml_inference_perspective_api",
            df=df,
            custom_args={"source": source},
        )
    return {
        "total_classified_posts": len(firehose_posts) + len(most_liked_posts),
        "total_classified_posts_by_source": {
            "firehose": len(firehose_posts),
            "most_liked": len(most_liked_posts),
        },
    }


def export_classified_post_uris(post_uris: set[str], source: Literal["local", "s3"]):
    full_key = os.path.join(
        perspective_api_root_s3_key, previously_classified_post_uris_filename
    )
    if source == "s3":
        s3.write_dict_json_to_s3(data=list(post_uris), key=full_key)
    elif source == "local":
        full_export_filepath = os.path.join(root_local_data_directory, full_key)
        write_jsons_to_local_store(
            records=list(post_uris), export_filepath=full_export_filepath
        )


def delete_cache_paths():
    """Deletes the cache paths. Recursively removes from the root path."""
    if os.path.exists(root_cache_path):
        shutil.rmtree(root_cache_path)


def rebuild_cache_paths():
    if not os.path.exists(root_cache_path):
        os.makedirs(root_cache_path)
    for path in [
        os.path.join(root_cache_path, "firehose"),
        os.path.join(root_cache_path, "most_liked"),
        os.path.join(root_cache_path, "firehose", "valid"),
        os.path.join(root_cache_path, "firehose", "invalid"),
        os.path.join(root_cache_path, "most_liked", "valid"),
        os.path.join(root_cache_path, "most_liked", "invalid"),
    ]:
        if not os.path.exists(path):
            os.makedirs(path)


def export_results() -> dict:
    """Exports the results of classifying posts to external store, then empties
    out the cache.
    """
    results = export_classified_posts()
    delete_cache_paths()
    rebuild_cache_paths()
    return results


# in case we need to rebuild the cache paths before running the script.
rebuild_cache_paths()
