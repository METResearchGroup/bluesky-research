import json
import traceback

from lib.log.logger import Logger
from services.compact_user_session_logs.helper import main

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting compact user session logs.")
        main()
        logger.info("Completed compact user session logs.")
        return {
            "statusCode": 200,
            "body": json.dumps("Compact user session logs completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in compact user session logs pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in compact user session logs pipeline: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
