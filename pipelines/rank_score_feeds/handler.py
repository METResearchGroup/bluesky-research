import json
import traceback

from lib.log.logger import Logger
from services.rank_score_feeds.helper import do_rank_score_feeds

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting rank score feeds.")
        do_rank_score_feeds()
        logger.info("Completed rank score feeds.")
        return {
            "statusCode": 200,
            "body": json.dumps("Rank score feeds completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in rank score feeds pipeline: {e}")
        logger.error(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error in rank score feeds pipeline: {str(e)}"),
        }


if __name__ == "__main__":
    lambda_handler(None, None)
