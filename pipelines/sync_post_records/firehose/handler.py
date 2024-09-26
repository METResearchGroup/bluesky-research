"""Lambda handler for getting most liked posts."""

import json
import traceback

from lib.log.logger import get_logger
from pipelines.sync_post_records.firehose.firehose import get_posts

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting to get posts from the firehose.")
        get_posts()
        logger.info("Successfully got posts from the firehose.")
        return {
            "statusCode": 200,
            "body": json.dumps("Get posts from the firehose completed successfully"),
        }
    except Exception as e:
        logger.error(
            f"Error in getting posts from the firehose: {e}\n{traceback.format_exc()}"
        )
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in getting posts from the firehose: {e}\n{traceback.format_exc()}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
