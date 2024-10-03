"""Lambda handler for getting most liked posts."""

import json
import traceback

from lib.log.logger import get_logger
from services.sync.most_liked_posts.helper import main as get_most_liked_posts

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting to get most liked posts.")
        args = {
            "use_latest_local": False,
            "store_local": True,
            "store_remote": False,
            "feeds": [
                "today",
                "verified_news",
                "what's reposted",
                "US Politics",
            ],
        }
        get_most_liked_posts(**args)
        logger.info("Successfully got most liked posts.")
        return {
            "statusCode": 200,
            "body": json.dumps("Get most liked posts completed successfully"),
        }
    except Exception as e:
        logger.error(
            f"Error in getting most liked posts: {e}\n{traceback.format_exc()}"
        )
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in getting most liked posts: {e}\n{traceback.format_exc()}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
