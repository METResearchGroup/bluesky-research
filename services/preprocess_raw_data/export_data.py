"""Export preprocessing data."""

import os

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import timestamp_format
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

dynamodb_table_name = "preprocessingPipelineMetadata"
dynamodb = DynamoDB()
dynamodb_table = dynamodb.resource.Table(dynamodb_table_name)

athena = Athena()
glue = Glue()
s3 = S3()

preprocessing_root_s3_key = "preprocessed_data"
s3_export_key_map = {
    "post": os.path.join(preprocessing_root_s3_key, "preprocessed_posts"),
    "like": os.path.join(preprocessing_root_s3_key, "like"),
    "follow": os.path.join(preprocessing_root_s3_key, "follow"),
}


def export_session_metadata(session_metadata: dict) -> None:
    """Exports the session data to DynamoDB."""
    dynamodb_table.put_item(Item=session_metadata)
    print("Session data exported to DynamoDB.")
    return


def export_latest_preprocessed_posts(
    latest_posts: list[FilteredPreprocessedPostModel],
) -> None:  # noqa
    """Exports latest preprocessed posts."""
    firehose_posts: list[dict] = [
        post.dict() for post in latest_posts if post.source == "firehose"
    ]  # noqa
    most_liked_posts: list[dict] = [
        post.dict() for post in latest_posts if post.source == "most_liked"
    ]  # noqa
    feed_type_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa
    dtype_map = MAP_SERVICE_TO_METADATA["preprocessed_posts"]["dtypes_map"]
    for feed_type, posts in feed_type_to_posts_tuples:
        df = pd.DataFrame(posts)
        df["partition_date"] = pd.to_datetime(
            df["preprocessing_timestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtype_map)
        export_data_to_local_storage(
            service="preprocessed_posts", df=df, custom_args={"source": feed_type}
        )


def export_latest_likes(latest_likes):
    pass


def export_latest_follows(latest_follows):
    pass
