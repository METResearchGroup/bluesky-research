"""Utility functions for managing the cursor state for the firehose stream."""

import os
from typing import Optional

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.db.bluesky_models.raw import FirehoseSubscriptionStateCursorModel

dynamodb = DynamoDB()
s3 = S3()

SUBSCRIPTION_STATE_TABLE_NAME = "firehoseSubscriptionState"


def update_cursor_state_dynamodb(
    cursor_model: FirehoseSubscriptionStateCursorModel,
) -> None:
    """Updates the cursor state in DynamoDB."""
    item = cursor_model.dict()
    dynamodb.insert_item_into_table(table_name="firehoseSubscriptionState", item=item)


def load_cursor_state_dynamodb(
    service_name: str,
) -> Optional[FirehoseSubscriptionStateCursorModel]:  # noqa
    """Loads the cursor state from DynamoDB, if it exists. If not, return
    None."""
    key = {"service": {"S": service_name}}
    result: Optional[dict] = dynamodb.get_item_from_table(
        table_name=SUBSCRIPTION_STATE_TABLE_NAME, key=key
    )
    if not result:
        return None
    return FirehoseSubscriptionStateCursorModel(**result)


def update_cursor_state_s3(cursor_model: FirehoseSubscriptionStateCursorModel) -> None:
    """Updates the cursor state in S3."""
    key = os.path.join("sync", "firehose", "cursor", f"{cursor_model.service}.json")  # noqa
    s3.write_dict_json_to_s3(data=cursor_model.dict(), key=key)


def load_cursor_state_s3(
    service_name: str,
) -> Optional[FirehoseSubscriptionStateCursorModel]:  # noqa
    """Loads the cursor state from S3, if it exists. If not, return None."""
    key = os.path.join("sync", "firehose", "cursor", f"{service_name}.json")
    result: Optional[dict] = s3.read_json_from_s3(key=key)
    if not result:
        return None
    return FirehoseSubscriptionStateCursorModel(**result)
