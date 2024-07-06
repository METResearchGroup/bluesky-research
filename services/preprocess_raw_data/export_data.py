"""Export preprocessing data."""
import os

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa

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


def export_latest_preprocessed_posts(latest_posts: list[ConsolidatedPostRecordModel]) -> None:  # noqa
    pass


def export_latest_likes():
    pass


def export_latest_follows():
    pass
