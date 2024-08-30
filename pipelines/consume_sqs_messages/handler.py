"""This lambda function consumes all the messages in the queues, gets their
results, and exports them to S3."""

import json
import traceback

from lib.log.logger import get_logger
from services.consume_sqs_messages.helper import consume_sqs_messages_from_queues  # noqa


logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting to consume SQS messages for queues.")
        # queue_names = ["syncsToBeProcessedQueue"]
        queue_names = [
            "firehoseSyncsToBeProcessedQueue",
            "mostLikedSyncsToBeProcessedQueue",
        ]
        consume_sqs_messages_from_queues(queue_names)
        logger.info("Completed consuming SQS messages for queues.")
        return {
            "statusCode": 200,
            "body": json.dumps("SQS messages consumed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in consuming SQS messages: {e}")
        logger.error(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error in consuming SQS messages: {str(e)}"),
        }


if __name__ == "__main__":
    lambda_handler(None, None)
