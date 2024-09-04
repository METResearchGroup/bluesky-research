"""Exports the results of classifying posts to an external store."""

import os
import shutil
from typing import Literal

from lib.aws.athena import Athena
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import write_jsons_to_local_store
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


def write_post_to_cache(
    classified_post: PerspectiveApiLabelsModel,
    source_feed: Literal["firehose", "most_liked"],
    classification_type: Literal["valid", "invalid"],
):
    """Writes a post to local cache.

    The complete post URI is given as a combination of the
    author's DID and a post ID. It comes in the form:
    at://<author_did>/app.bsky.feed.post/<post_id>

    We extract the author_did and post_id portion to create a unique key
    for the post.

    For example:
    - at://did:plc:z37zxpcg22ookqjpvmgansn2/app.bsky.feed.post/3kwfp7deuxm2i
        - We extract the "did:plc:z37zxpcg22ookqjpvmgansn2", and
        "3kwfp7deuxm2i" portions.

    We create a joint key of {author_did}_{post_id} to store the post.
    """
    post_id = classified_post.uri.split("/")[-1]
    author_did = classified_post.uri.split("/")[-3]
    joint_pk = f"{author_did}_{post_id}"
    full_key = os.path.join(
        root_cache_path, source_feed, classification_type, f"{joint_pk}.json"
    )
    with open(full_key, "w") as f:
        f.write(classified_post.json())


def export_classified_posts(
    current_timestamp: str,
    external_stores: list[Literal["local", "s3"]] = ["local", "s3"],
) -> dict:
    """Export classified posts.

    Loads latest posts from cache and exports them to an external store.
    """
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=current_timestamp
    )
    filename = "classified_posts.jsonl"
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

    for source, posts in source_to_posts_tuples:
        classified_post_dicts = [post.dict() for post in posts]
        for external_store in external_stores:
            full_key = os.path.join(
                perspective_api_root_s3_key, f"source={source}", partition_key, filename
            )
            if external_store == "s3":
                s3.write_dicts_jsonl_to_s3(data=classified_post_dicts, key=full_key)  # noqa
            elif external_store == "local":
                full_export_filepath = os.path.join(root_local_data_directory, full_key)
                write_jsons_to_local_store(
                    records=classified_post_dicts, export_filepath=full_export_filepath
                )
            else:
                raise ValueError("Invalid external store.")
    # trigger Glue crawler to recognize new data and partitions
    athena.run_query("MSCK REPAIR TABLE ml_inference_perspective_api")
    glue.start_crawler("perspective_api_labels_glue_crawler")
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


def export_results(
    current_timestamp: str,
    external_stores: list[Literal["local", "s3"]] = ["local", "s3"],
) -> dict:
    """Exports the results of classifying posts to external store, then empties
    out the cache.
    """
    results = export_classified_posts(
        current_timestamp=current_timestamp, external_stores=external_stores
    )
    delete_cache_paths()
    rebuild_cache_paths()
    return results


# in case we need to rebuild the cache paths before running the script.
rebuild_cache_paths()
