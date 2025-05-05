import json
import traceback

from api.integrations_router.constants import dynamodb_table_name
from lib.helper import generate_current_datetime_str
from lib.log.logger import Logger
from services.preprocess_raw_data.helper import preprocess_latest_raw_data

logger = Logger(__name__)


def lambda_handler(event: dict, context: dict):
    try:
        if not event:
            event = {
                "backfill_period": None,  # either "days" or "hours"
                "backfill_duration": None,
            }
        logger.info("Starting preprocessing pipeline in Lambda.")
        backfill_period = event.get("backfill_period", None)
        backfill_duration = event.get("backfill_duration", None)
        if backfill_duration is not None:
            backfill_duration = int(backfill_duration)
        session_metadata: dict = preprocess_latest_raw_data(
            backfill_period=backfill_period,
            backfill_duration=backfill_duration,
            event=event,
        )
        logger.info("Completed preprocessing pipeline in Lambda.")
        session_status_metadata = {
            "service": "preprocess_raw_data",
            "timestamp": session_metadata["preprocessing_timestamp"],
            "status_code": 200,
            "body": json.dumps("Preprocessing completed successfully"),
            "metadata_table_name": dynamodb_table_name,
            "metadata": json.dumps(session_metadata),
        }
        return session_status_metadata
    except Exception as e:
        logger.error(f"Error in preprocessing pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(f"Error in preprocessing pipeline: {str(e)}"),
                }
            )
        )
        session_metadata = {
            "service": "preprocess_raw_data",
            "timestamp": generate_current_datetime_str(),
            "status_code": 500,
            "body": json.dumps(f"Error in preprocessing pipeline: {str(e)}"),
            "metadata_table_name": dynamodb_table_name,
            "metadata": json.dumps(traceback.format_exc()),
        }
        return session_metadata


if __name__ == "__main__":
    lambda_handler(None, None)
