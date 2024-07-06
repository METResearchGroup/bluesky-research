"""Export preprocessing data."""
import os
from typing import Literal

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import write_jsons_to_local_store
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

dynamodb_table_name = "preprocessingPipelineMetadata"
dynamodb = DynamoDB()
dynamodb_table = dynamodb.resource.Table(dynamodb_table_name)

s3 = S3()

root_s3_key = "preprocessed_data"
s3_export_key_map = {
    "post": os.path.join(root_s3_key, "post"),
    "like": os.path.join(root_s3_key, "like"),
    "follow": os.path.join(root_s3_key, "follow")
}


def export_session_metadata(session_metadata: dict) -> None:
    """Exports the session data to DynamoDB."""
    dynamodb_table.put_item(Item=session_metadata)
    print("Session data exported to DynamoDB.")
    return


def export_latest_preprocessed_posts(
    latest_posts: list[FilteredPreprocessedPostModel],
    session_metadata: dict,
    external_stores: list[Literal["local", "s3"]] = ["local", "s3"]
) -> None:  # noqa
    """Exports latest preprocessed posts."""
    current_timestamp = session_metadata["current_preprocessing_timestamp"]
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=current_timestamp
    )
    filename = "preprocessed_posts.jsonl"
    full_key = os.path.join(s3_export_key_map["post"], partition_key, filename)  # noqa
    data = [post.dict() for post in latest_posts]
    for external_store in external_stores:
        if external_store == "s3":
            s3.write_dicts_jsonl_to_s3(data=data, key=full_key)
        elif external_store == "local":
            full_export_filepath = os.path.join(
                root_local_data_directory, full_key
            )
            write_jsons_to_local_store(
                records=data, export_filepath=full_export_filepath
            )
        else:
            raise ValueError("Invalid export store.")


def export_latest_likes(latest_likes):
    pass


def export_latest_follows(latest_follows):
    pass
