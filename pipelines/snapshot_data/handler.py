import json
import traceback

from lib.log.logger import Logger
from services.snapshot_data.helper import snapshot_data

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting snapshot data process.")
        snapshot_data()
        logger.info("Completed snapshot data process.")
        return {
            "statusCode": 200,
            "body": json.dumps("Snapshot data process completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in snapshot data process: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(f"Error in snapshot data process: {str(e)}"),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
