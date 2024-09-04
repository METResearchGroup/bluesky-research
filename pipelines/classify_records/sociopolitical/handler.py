import json
import traceback

from lib.log.logger import get_logger
from services.ml_inference.sociopolitical.sociopolitical import classify_latest_posts

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting classification of latest sociopolitical posts.")
        classify_latest_posts()
        logger.info("Completed classification of latest sociopolitical posts.")
        return {
            "statusCode": 200,
            "body": json.dumps(
                "Classification of latest sociopolitical posts completed successfully"
            ),
        }
    except Exception as e:
        logger.error(f"Error in classification of latest sociopolitical posts: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in classification of latest sociopolitical posts: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
