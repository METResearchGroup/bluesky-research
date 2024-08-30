"""Contains helper functions for consuming messages from SQS queues."""

import os

from lib.aws.s3 import S3
from lib.aws.sqs import queue_to_queue_url_map, SQS
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)

s3 = S3()
root_s3_key = "sqs_messages"

timestamp = generate_current_datetime_str()


def export_sqs_messages_to_s3(sqs_message_bodies: list[dict], queue_name: str):
    """Exports the SQS messages to S3 as a .jsonl file.

    Creates a compound partition key of queue name + timestamp partition (year/month/day/hour/minute/second)
    """  # noqa
    partition_key = s3.create_partition_key_based_on_timestamp(timestamp)
    key = os.path.join(
        root_s3_key, f"queue_name={queue_name}", partition_key, f"{timestamp}.jsonl"
    )  # noqa
    s3.write_dicts_jsonl_to_s3(sqs_message_bodies, key)
    logger.info(f"Successfully exported {len(sqs_message_bodies)} messages to {key}")  # noqa


def consume_sqs_messages_for_queue(queue: SQS) -> list[dict]:
    """Consumes all the messages in the queue, gets their results, and
    exports them to S3."""
    results: list[dict] = []
    while True:
        response = queue.receive_latest_messages(
            queue=queue.queue, max_num_messages=10, visibility_timeout=20
        )
        if not response:
            break
        logger.info(f"Received {len(response)} messages from {queue.queue}")
        data = [message["Body"] for message in response]
        results.extend(data)
        for message in response:
            queue.client.delete_message(
                QueueUrl=queue.queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            logger.info(f"Deleted message from {queue.queue}")
    return results


def consume_sqs_messages_from_queues(
    queue_names: list[str] = queue_to_queue_url_map.keys(),
):
    """Consumes all the messages in the queues, gets their results, and
    exports them to S3."""
    for queue_name in queue_names:
        logger.info(f"Consuming messages for queue: {queue_name}")
        queue = SQS(queue_name)
        sqs_message_bodies = consume_sqs_messages_for_queue(queue)
        export_sqs_messages_to_s3(
            sqs_message_bodies=sqs_message_bodies, queue_name=queue_name
        )
    logger.info("Finished consuming messages for all queues")


if __name__ == "__main__":
    test_sqs_queue = SQS("syncsToBeProcessedQueue")
    data = [{f"test_{i}": i} for i in range(25)]
    logger.info("Resetting queue...")
    test_sqs_queue.clear_messages_in_queue()
    logger.info(f"Queue reset, adding {len(data)} messages...")
    for i in data:
        test_sqs_queue.send_message(source="test", data=i)
    queue_names = ["syncsToBeProcessedQueue"]
    consume_sqs_messages_from_queues(queue_names)
