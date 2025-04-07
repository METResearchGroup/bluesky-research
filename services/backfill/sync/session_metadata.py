"""Helper utilities for managing backfill-specific session metadata.

Separate from api/integrations_router/run.py, because that manages session
metadata for integrations instead of backfills (though TBH there's an argument
for refactoring that to make it more consistent).
"""

import os

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from services.backfill.sync.models import UserBackfillMetadata

athena = Athena()
dynamodb = DynamoDB()
dynamodb_table_name = "backfill_metadata"
s3 = S3()

logger = get_logger(__name__)


def load_latest_backfill_session_metadata() -> RunExecutionMetadata:
    pass


def load_latest_backfilled_users() -> list[dict]:
    """Loads the latest backfilled users from S3."""
    query = """
        WITH ranked_users AS (
            SELECT 
                did, 
                bluesky_handle,
                timestamp,
                ROW_NUMBER() OVER (PARTITION BY did ORDER BY timestamp DESC) as row_num
            FROM backfill_metadata
        )
        SELECT did, bluesky_handle 
        FROM ranked_users
        WHERE row_num = 1
    """
    df = athena.query_results_as_df(query=query)
    return df.to_dict(orient="records")


def write_session_backfill_job_metadata_to_db(
    session_backfill_metadata: RunExecutionMetadata,
):
    """Writes metadata of the backfill job to the database."""
    try:
        dynamodb.insert_item_into_table(
            item=session_backfill_metadata.dict(),
            table_name=dynamodb_table_name,
        )
        logger.info(
            f"Successfully inserted session backfill metadata: {session_backfill_metadata.dict()}"
        )
    except Exception as e:
        logger.error(f"Error writing session backfill metadata to DynamoDB: {e}")
        raise


def write_user_session_backfill_metadata_to_db(
    user_backfill_metadata: list[UserBackfillMetadata],
) -> None:
    """Writes metadata for the users backfilled during a backfill job."""
    user_backfill_metadata_dicts: list[dict] = [
        user_backfill_metadata.model_dump()
        for user_backfill_metadata in user_backfill_metadata
    ]
    if len(user_backfill_metadata_dicts) == 0:
        logger.warning("No user backfill metadata to write to S3.")
        return
    timestamp = generate_current_datetime_str()
    key = os.path.join("backfill_metadata", f"user_backfill_metadata_{timestamp}.jsonl")
    s3.write_dicts_jsonl_to_s3(data=user_backfill_metadata_dicts, key=key)
    logger.info(f"Successfully wrote user backfill metadata to S3: {key}")


def write_backfill_metadata_to_db(
    session_backfill_metadata: RunExecutionMetadata,
    user_backfill_metadata: list[UserBackfillMetadata],
) -> None:
    """Writes metadata for the backfill job to the database.

    Writes two sets of metadata:
    - Metadata about the actual backfill job.
    - Metadata about the users backfilled during the job.
    """
    write_user_session_backfill_metadata_to_db(user_backfill_metadata)
    write_session_backfill_job_metadata_to_db(session_backfill_metadata)
    logger.info("completed writing backfill metadata to db")
