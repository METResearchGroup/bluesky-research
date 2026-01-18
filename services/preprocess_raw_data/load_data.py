"""Load raw data for preprocessing"""

import json
from typing import Optional

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.db.bluesky_models.embed import (
    ProcessedExternalEmbed,
    ProcessedRecordEmbed,
)
from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa

logger = get_logger(__file__)

athena = Athena()
s3 = S3()
dynamodb_table_name = "preprocessingPipelineMetadata"
dynamodb = DynamoDB()
dynamodb_table = dynamodb.resource.Table(dynamodb_table_name)


def load_previous_session_metadata():
    """Loads previous session data from DynamoDB, if it exists.

    Loads latest based on "current_preprocessing_timestamp" field, which
    is the partition key for the table.
    """
    response = dynamodb_table.scan()
    items = response["Items"]
    if not items:
        return None
    latest_item = max(items, key=lambda x: x["current_preprocessing_timestamp"])  # noqa
    return latest_item


def transform_latest_posts(df: pd.DataFrame) -> list[ConsolidatedPostRecordModel]:
    df_dicts = df.to_dict(orient="records")
    df_dicts = parse_converted_pandas_dicts(df_dicts)
    df_dicts_cleaned = [post for post in df_dicts if post["text"] is not None]
    for post in df_dicts_cleaned:
        post["embed"] = json.loads(post["embed"])
        if not isinstance(post["embed"]["embedded_record"], dict) and not isinstance(
            post["embed"]["embedded_record"], ProcessedRecordEmbed
        ):
            # should be dict if record exists. Otherwise it'll be a
            # string of value 'null', which we need to convert to json format.
            # NOTE: could also be None, in which case the extra check is required.
            if post["embed"]["embedded_record"]:
                post["embed"]["embedded_record"] = json.loads(
                    post["embed"]["embedded_record"]
                )
        if not isinstance(post["embed"]["external"], dict) and not isinstance(
            post["embed"]["external"], ProcessedExternalEmbed
        ):
            # should be dict if record exists. Otherwise it'll be a
            # string of value 'null', which we need to convert to json format.
            # NOTE: could also be None, in which case the extra check is required.
            if post["embed"]["external"]:
                post["embed"]["external"] = json.loads(post["embed"]["external"])
    return [ConsolidatedPostRecordModel(**post) for post in df_dicts_cleaned]


@track_performance
def load_latest_firehose_posts(
    timestamp: str, limit: Optional[int] = None
) -> pd.DataFrame:
    """Queries the firehose table for the latest posts."""
    in_network_user_posts_df: pd.DataFrame = load_data_from_local_storage(
        service="in_network_user_activity",
        latest_timestamp=timestamp,
        storage_tiers=["active"],
    )
    study_user_posts_df: pd.DataFrame = load_data_from_local_storage(
        service="study_user_activity",
        latest_timestamp=timestamp,
        storage_tiers=["active"],
    )
    df = pd.concat([in_network_user_posts_df, study_user_posts_df], ignore_index=True)
    if limit:
        df = df.head(limit)
    df = df[df["text"].notna()]
    df["embed"] = df["embed"].apply(lambda x: json.loads(x) if pd.notna(x) else x)
    return df


@track_performance
def load_latest_most_liked_posts(
    timestamp: str, limit: Optional[int] = None
) -> pd.DataFrame:  # noqa
    df: pd.DataFrame = load_data_from_local_storage(
        service="sync_most_liked_posts",
        latest_timestamp=timestamp,
        storage_tiers=["active"],
    )
    if limit:
        df = df.head(limit)
    df = df[df["text"].notna()]
    df["embed"] = df["embed"].apply(lambda x: json.loads(x) if pd.notna(x) else x)
    return df
