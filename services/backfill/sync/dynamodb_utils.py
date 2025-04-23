"""Utilities for interacting with DynamoDB for backfill metadata."""

from typing import Optional

from lib.aws.dynamodb import DynamoDB
from lib.log.logger import get_logger
from services.backfill.sync.models import UserBackfillMetadata

TABLE_NAME = "backfill_user_metadata"
DID_INDEX = "did-index"
HANDLE_INDEX = "bluesky_handle-index"

dynamodb_client = DynamoDB()

logger = get_logger(__name__)


def create_did_timestamp_key(did: str, timestamp: str) -> str:
    """Create a composite key from DID and timestamp."""
    return f"{did}#{timestamp}"


def serialize_user_metadata(metadata: UserBackfillMetadata) -> dict:
    """Convert UserBackfillMetadata to DynamoDB item format."""
    return {
        "pds_service_endpoint": {"S": metadata.pds_service_endpoint},
        "did_timestamp": {
            "S": create_did_timestamp_key(metadata.did, metadata.timestamp)
        },
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


def batch_save_user_metadata(metadata_list: list[UserBackfillMetadata]) -> None:
    """Save multiple UserBackfillMetadata items efficiently."""
    total_metadata = len(metadata_list)
    try:
        resource = dynamodb_client.resource
        table = resource.Table(TABLE_NAME)

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html
        with table.batch_writer() as batch:
            for i, metadata in enumerate(metadata_list):
                if i > 0 and i % 100 == 0:
                    logger.info(f"Processed {i}/{total_metadata} items...")

                # Create the composite sort key
                did_timestamp = f"{metadata.did}#{metadata.timestamp}"

                # we add a default "<unknown>" Bluesky handle for the records
                # that we don't have a handle for. We should've grabbed this
                # from the PLC doc, but we didn't.  Eventually we'll want the
                # ability to query by handle though, and DynamoDB doesn't support
                # empty strings as index keys.
                bluesky_handle = metadata.bluesky_handle
                if not bluesky_handle:
                    bluesky_handle = "<unknown>"

                # Serialize the metadata
                item = {
                    "pds_service_endpoint": metadata.pds_service_endpoint,
                    "did_timestamp": did_timestamp,
                    "did": metadata.did,
                    "bluesky_handle": bluesky_handle,
                    "types": metadata.types,
                    "total_records": metadata.total_records,
                    "total_records_by_type": metadata.total_records_by_type,
                    "timestamp": metadata.timestamp,
                }

                # Add the item to the batch
                batch.put_item(Item=item)

        logger.info(f"Successfully saved {total_metadata} user metadata.")

    except Exception as e:
        logger.error(f"Error saving user metadata to DynamoDB: {e}")
        raise


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


def get_dids_by_pds_endpoint(pds_endpoint: str) -> list[str]:
    """Get all DIDs for a specific PDS endpoint (most recent backfill only)."""
    response = dynamodb_client.client.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="pds_service_endpoint = :value",
        ExpressionAttributeValues={":value": {"S": pds_endpoint}},
        ProjectionExpression="did",
    )
    # Get unique DIDs since we might have multiple records per DID
    return list(set(item["did"]["S"] for item in response.get("Items", [])))


def get_backfill_history_for_did(did: str) -> list[UserBackfillMetadata]:
    """Get all backfill history for a specific DID."""
    items = dynamodb_client.query_items_by_index(TABLE_NAME, DID_INDEX, "did", did)
    return [deserialize_user_metadata(item) for item in items]


def get_latest_backfill_for_did(did: str) -> Optional[UserBackfillMetadata]:
    """Get the most recent backfill for a specific DID."""
    items = dynamodb_client.query_items_by_index(TABLE_NAME, DID_INDEX, "did", did)
    if not items:
        return None

    # Sort by timestamp (newest first) and return the first item
    sorted_items = sorted(items, key=lambda x: x["timestamp"]["S"], reverse=True)
    return deserialize_user_metadata(sorted_items[0])
