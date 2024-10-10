"""Export preprocessing data."""

import json
import os

import numpy as np
import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import timestamp_format
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger

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

logger = get_logger(__name__)


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            # Convert numpy.int64 to regular Python int
            if isinstance(v, np.int64):
                v = int(v)
            items.append((new_key, v))
    return dict(items)

def export_session_metadata(session_metadata: dict) -> None:
    """Exports the session data to DynamoDB."""
    # flatten dict, to avoid nested serialization problems
    flattened_dict = flatten_dict(session_metadata)
    dynamodb.insert_item_into_table(item=flattened_dict, table_name=dynamodb_table_name)
    print("Session data exported to DynamoDB.")
    return


def export_latest_preprocessed_posts(
    latest_posts: pd.DataFrame,
) -> None:  # noqa
    """Exports latest preprocessed posts."""
    firehose_df = latest_posts[latest_posts["source"] == "firehose"]
    most_liked_df = latest_posts[latest_posts["source"] == "most_liked"]
    firehose_posts: list[dict] = firehose_df.to_dict(orient="records")
    most_liked_posts: list[dict] = most_liked_df.to_dict(orient="records")

    feed_type_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa
    dtype_map = MAP_SERVICE_TO_METADATA["preprocessed_posts"]["dtypes_map"]
    for feed_type, posts in feed_type_to_posts_tuples:
        if len(posts) == 0:
            logger.info(f"No {feed_type} posts to export.")
            continue
        try:
            df = pd.DataFrame(posts)
            df["partition_date"] = pd.to_datetime(
                df["preprocessing_timestamp"], format=timestamp_format
            ).dt.date
            df["embed"] = df["embed"].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )
            df = df.astype(dtype_map)
            export_data_to_local_storage(
                service="preprocessed_posts", df=df, custom_args={"source": feed_type}
            )
        except Exception as e:
            print(f"Error exporting {feed_type} posts: {e}")


def export_latest_likes(latest_likes):
    pass


def export_latest_follows(latest_follows):
    pass
