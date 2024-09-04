import json
import traceback

from lib.log.logger import get_logger
from services.compact_dedupe_data.helper import compact_dedupe_preprocessed_data

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting compact dedupe preprocessing.")
        compact_dedupe_preprocessed_data()
        logger.info("Completed compact dedupe preprocessing.")
        return {
            "statusCode": 200,
            "body": json.dumps("Compact dedupe preprocessing completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in compact dedupe preprocessing pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in compact dedupe preprocessing pipeline: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
