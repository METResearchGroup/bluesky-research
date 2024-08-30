"""Lambda handler for getting most liked posts."""

import json

from lib.log.logger import Logger
from services.sync.most_liked_posts.helper import main as get_most_liked_posts

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting to get most liked posts.")
        args = {
            "use_latest_local": False,
            "store_local": False,
            "store_remote": True,
            "feeds": ["today"],
        }
        get_most_liked_posts(**args)
        logger.info("Successfully got most liked posts.")
        return {
            "statusCode": 200,
            "body": json.dumps("Get most liked posts completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in getting most liked posts: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error in getting most liked posts: {str(e)}"),
        }


if __name__ == "__main__":
    lambda_handler(None, None)
