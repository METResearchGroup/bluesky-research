import json
import traceback

from lib.log.logger import Logger
from services.rank_score_feeds.helper import do_rank_score_feeds

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting rank score feeds.")
        payload = {
            "users_to_create_feeds_for": None,
            "skip_export_post_scores": False,
        }
        do_rank_score_feeds(**payload)
        logger.info("Completed rank score feeds.")
        return {
            "statusCode": 200,
            "body": json.dumps("Rank score feeds completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in rank score feeds pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(f"Error in rank score feeds pipeline: {str(e)}"),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
