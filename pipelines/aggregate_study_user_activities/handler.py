import json
import traceback

from lib.log.logger import Logger
from services.aggregate_study_user_activities.helper import (
    main as aggregate_study_user_activities,
)

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting aggregate study user activities process.")
        aggregate_study_user_activities()
        logger.info("Completed aggregate study user activities process.")
        return {
            "statusCode": 200,
            "body": json.dumps(
                "Aggregate study user activities process completed successfully"
            ),
        }
    except Exception as e:
        logger.error(f"Error in aggregate study user activities process: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in aggregate study user activities process: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
