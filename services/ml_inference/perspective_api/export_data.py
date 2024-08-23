"""Exports the results of classifying posts to an external store."""

import os
import shutil
from typing import Literal

from lib.aws.dynamodb import DynamoDB
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

dynamodb_table_name = "perspectiveApiClassificationMetadata"
dynamodb = DynamoDB()
dynamodb_table = dynamodb.resource.Table(dynamodb_table_name)

s3 = S3()


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
                perspective_api_root_s3_key, source, partition_key, filename
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


def export_session_metadata(session_metadata: dict):
    dynamodb_table.put_item(Item=session_metadata)
    print("Session data exported to DynamoDB.")
    return


def delete_cache_paths():
    """Deletes the cache paths. Recursively removes from the root path."""
    if os.path.exists(root_cache_path):
        shutil.rmtree(root_cache_path)


def rebuild_cache_paths():
    if not os.path.exists(root_cache_path):
        os.makedirs(root_cache_path)
    os.mkdir(os.path.join(root_cache_path, "firehose"))
    os.mkdir(os.path.join(root_cache_path, "most_liked"))
    os.mkdir(os.path.join(root_cache_path, "firehose", "valid"))
    os.mkdir(os.path.join(root_cache_path, "firehose", "invalid"))
    os.mkdir(os.path.join(root_cache_path, "most_liked", "valid"))
    os.mkdir(os.path.join(root_cache_path, "most_liked", "invalid"))


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
    # total_classified_uris = previous_classified_post_uris.union(classified_uris)  # noqa
    # export_classified_post_uris(total_classified_uris, source="local")
    # export_classified_post_uris(total_classified_uris, source="s3")
    # export_session_metadata(session_metadata)

    delete_cache_paths()
    rebuild_cache_paths()

    return results

    # print(
    #     f"Exported results from classifying {len(classified_uris)} posts using the Perspective API."
    # )  # noqa
