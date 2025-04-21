"""Utilities for interacting with DynamoDB for backfill metadata."""

from typing import List, Optional

from lib.aws.dynamodb import DynamoDB
from services.backfill.sync.models import UserBackfillMetadata

TABLE_NAME = "backfill_user_metadata"
DID_INDEX = "did-index"
HANDLE_INDEX = "bluesky_handle-index"

dynamodb_client = DynamoDB()


def serialize_user_metadata(metadata: UserBackfillMetadata) -> dict:
    """Convert UserBackfillMetadata to DynamoDB item format."""
    return {
        "pds_service_endpoint": {"S": metadata.pds_service_endpoint},
        "did": {"S": metadata.did},
        "bluesky_handle": {"S": metadata.bluesky_handle},
        "types": {"S": metadata.types},
        "total_records": {"N": str(metadata.total_records)},
        "total_records_by_type": {"S": metadata.total_records_by_type},
        "timestamp": {"S": metadata.timestamp},
    }


def deserialize_user_metadata(item: dict) -> UserBackfillMetadata:
    """Convert DynamoDB item to UserBackfillMetadata."""
    return UserBackfillMetadata(
        pds_service_endpoint=item["pds_service_endpoint"]["S"],
        did=item["did"]["S"],
        bluesky_handle=item["bluesky_handle"]["S"],
        types=item["types"]["S"],
        total_records=int(item["total_records"]["N"]),
        total_records_by_type=item["total_records_by_type"]["S"],
        timestamp=item["timestamp"]["S"],
    )


def save_user_metadata(metadata: UserBackfillMetadata) -> None:
    """Save UserBackfillMetadata to DynamoDB."""
    item = serialize_user_metadata(metadata)
    dynamodb_client.insert_item_into_table(item, TABLE_NAME)


def batch_save_user_metadata(metadata_list: List[UserBackfillMetadata]) -> None:
    """Save multiple UserBackfillMetadata items efficiently."""
    # DynamoDB batch writes are limited to 25 items
    batch_size = 25
    for i in range(0, len(metadata_list), batch_size):
        batch = metadata_list[i : i + batch_size]
        request_items = {
            TABLE_NAME: [
                {"PutRequest": {"Item": serialize_user_metadata(metadata)}}
                for metadata in batch
            ]
        }
        dynamodb_client.client.batch_write_item(RequestItems=request_items)


def did_exists(did: str) -> bool:
    """Check if a DID exists in the backfill metadata."""
    return dynamodb_client.check_item_exists_by_index(TABLE_NAME, DID_INDEX, "did", did)


def handle_exists(handle: str) -> bool:
    """Check if a Bluesky handle exists in the backfill metadata."""
    return dynamodb_client.check_item_exists_by_index(
        TABLE_NAME, HANDLE_INDEX, "bluesky_handle", handle
    )


def get_user_metadata_by_did(did: str) -> Optional[UserBackfillMetadata]:
    """Get UserBackfillMetadata by DID."""
    items = dynamodb_client.query_items_by_index(TABLE_NAME, DID_INDEX, "did", did)
    if not items:
        return None
    return deserialize_user_metadata(items[0])


def get_user_metadata_by_handle(handle: str) -> Optional[UserBackfillMetadata]:
    """Get UserBackfillMetadata by Bluesky handle."""
    items = dynamodb_client.query_items_by_index(
        TABLE_NAME, HANDLE_INDEX, "bluesky_handle", handle
    )
    if not items:
        return None
    return deserialize_user_metadata(items[0])


def get_dids_by_pds_endpoint(pds_endpoint: str) -> List[str]:
    """Get all DIDs for a specific PDS endpoint."""
    response = dynamodb_client.client.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="pds_service_endpoint = :value",
        ExpressionAttributeValues={":value": {"S": pds_endpoint}},
        ProjectionExpression="did",
    )
    return [item["did"]["S"] for item in response.get("Items", [])]
