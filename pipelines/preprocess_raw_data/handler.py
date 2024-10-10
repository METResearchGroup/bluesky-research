import json
import traceback

from lib.log.logger import Logger
from services.preprocess_raw_data.helper import preprocess_latest_raw_data

logger = Logger(__name__)


def lambda_handler(event: dict, context: dict):
    try:
        if not event:
            event = {
                "backfill_period": "days",  # either "days" or "hours"
                "backfill_duration": 5,
            }
        logger.info("Starting preprocessing pipeline in Lambda.")
        backfill_period = event.get("backfill_period", None)
        backfill_duration = event.get("backfill_duration", None)
        if backfill_duration is not None:
            backfill_duration = int(backfill_duration)
        preprocess_latest_raw_data(
            backfill_period=backfill_period, backfill_duration=backfill_duration
        )
        logger.info("Completed preprocessing pipeline in Lambda.")
        return {
            "statusCode": 200,
            "body": json.dumps("Preprocessing completed successfully"),
        }
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
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
