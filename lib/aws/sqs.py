"""Wrapper client for AWS SQS."""
import json
from typing import Optional

from lib.aws.helper import create_client, retry_on_aws_rate_limit
from lib.constants import current_datetime_str
from lib.log.logger import get_logger

queue_to_queue_url_map = {
    "syncsToBeProcessedQueue": "https://sqs.us-east-2.amazonaws.com/517478598677/syncsToBeProcessedQueue.fifo",  # noqa
    "firehoseSyncsToBeProcessedQueue": "https://sqs.us-east-2.amazonaws.com/517478598677/firehoseSyncsToBeProcessedQueue.fifo",  # noqa
    "mostLikedSyncsToBeProcessedQueue": "https://sqs.us-east-2.amazonaws.com/517478598677/mostLikedSyncsToBeProcessedQueue.fifo",  # noqa
}


logger = get_logger(__name__)


class SQS:
    """Wrapper class for all SQS interactions."""

    def __init__(self, queue: str):
        queue_url = queue_to_queue_url_map[queue]
        self.queue_url = queue_url
        self.client = create_client("sqs")

    @retry_on_aws_rate_limit
    def send_message(self, source: str, data: dict):
        """Send a message to the queue."""
        payload = {
            "source": source,
            "insert_timestamp": current_datetime_str,
            "data": data
        }
        json_payload = json.dumps(payload)
        self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json_payload,
            # make sure that messages from the same source are in the same
            # group, so that they're processed in order.
            MessageGroupId=source
        )

    @retry_on_aws_rate_limit
    def receive_latest_messages(
        self,
        queue: str,
        max_num_messages: Optional[int] = None,
        latest_timestamp: Optional[str] = None
    ):
        """Receive messages from a queue.

        Optionally can specify a latest timestamp to filter messages.
        Processes messages in order from the same source. If a message has
        a timestamp greater than the latest timestamp, it will not be
        processed in that round.
        """
        if not latest_timestamp:
            logger.warning(
                f"No latest timestamp provided, using current timestamp of {current_datetime_str}"  # noqa
            )
            latest_timestamp = current_datetime_str
        queue_url = queue_to_queue_url_map[queue]
        if max_num_messages is not None:
            response = self.client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_num_messages,
                MessageAttributeNames=["All"]
            )
        else:
            response = self.client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["All"],
                MessageSystemAttributeNames=["All"]
            )
        messages = response["Messages"]
        res: list[dict] = []
        for message in messages:
            message["Body"] = json.loads(message["Body"])
            if message["Body"].get("insert_timestamp") > latest_timestamp:
                logger.info(
                    f"Message with insert timestamp {message['Body'].get('insert_timestamp')} is "  # noqa
                    f"greater than latest timestamp {latest_timestamp}, skipping this message"  # noqa
                    "as well as all subsequent messages"  # noqa
                )
                break
            res.append(message)
        logger.info(f"Received {len(res)} messages from the queue {queue}")
        return res

    def delete_messages(self, messages: list[dict]):
        """Delete messages from the queue."""
        total_messages_deleted = 0
        for message in messages:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            total_messages_deleted += 1
        logger.info(
            f"Deleted {total_messages_deleted} latest messages from the queue"
        )

    def clear_messages_in_queue(self):
        """Clear all messages in the queue."""
        logger.warning(
            f"Clearing all messages in the queue {self.queue_url}. DESTRUCTIVE."
        )
        self.client.purge_queue(QueueUrl=self.queue_url)


if __name__ == "__main__":
    sqs = SQS("syncsToBeProcessedQueue")
    sqs.send_message(source="test-feed", data={"test": "test"})
    messages: list[dict] = sqs.receive_latest_messages(
        queue="syncsToBeProcessedQueue"
    )
    print(len(messages))
    sqs.delete_messages(messages)
