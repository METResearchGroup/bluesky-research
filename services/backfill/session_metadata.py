"""Helper utilities for managing backfill-specific session metadata.

Separate from api/integrations_router/run.py, because that manages session
metadata for integrations instead of backfills (though TBH there's an argument
for refactoring that to make it more consistent).
"""

from lib.aws.dynamodb import DynamoDB
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata

dynamodb = DynamoDB()
dynamodb_table_name = "backfill_metadata"

logger = get_logger(__name__)


def load_latest_backfill_session_metadata() -> RunExecutionMetadata:
    pass


def write_backfill_metadata_to_db(backfill_metadata: RunExecutionMetadata) -> None:
    try:
        dynamodb.insert_item_into_table(
            item=backfill_metadata.dict(),
            table_name=dynamodb_table_name,
        )
        logger.info(
            f"Successfully inserted backfill metadata: {backfill_metadata.dict()}"
        )
    except Exception as e:
        logger.error(f"Error writing backfill metadata to DynamoDB: {e}")
        raise
