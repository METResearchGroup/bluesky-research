import json
import traceback

from lib.log.logger import get_logger
from services.calculate_superposters.helper import calculate_latest_superposters

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting calculation of superposters in Lambda.")
        calculate_latest_superposters(top_n_percent=None, threshold=5)
        logger.info("Completed calculation of superposters in Lambda.")
        return {
            "statusCode": 200,
            "body": json.dumps("Superposter calculation completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in superposter calculation pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in superposter calculation pipeline: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
