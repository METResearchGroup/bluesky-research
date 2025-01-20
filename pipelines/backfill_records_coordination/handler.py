import json
import traceback

from lib.log.logger import get_logger
from services.backfill.main import backfill_records

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting backfill records process.")
        payload = event.get("payload", {})
        backfill_records(payload)
        logger.info("Completed backfill records process.")
        return {
            "statusCode": 200,
            "body": json.dumps("Backfill records process completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in backfill records process: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(f"Error in backfill records process: {str(e)}"),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler({"payload": {"record_type": "posts"}}, None)
