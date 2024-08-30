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
default_visibility_timeout = 300  # for prod.
# default_visibility_timeout = 30  # for debugging.


class SQS:
    """Wrapper class for all SQS interactions."""

    def __init__(self, queue: str):
        self._validate_queue(queue)
        queue_url = queue_to_queue_url_map[queue]
        self.queue = queue
        self.queue_url = queue_url
        self.client = create_client("sqs")

    def _validate_queue(self, queue: str):
        if queue not in queue_to_queue_url_map:
            raise ValueError(f"Invalid queue name: {queue}")

    @retry_on_aws_rate_limit
    def send_message(self, source: str, data: dict, custom_log: Optional[str] = None):
        """Send a message to the queue."""
        payload = {
            "source": source,
            "insert_timestamp": current_datetime_str,
            "data": data,
        }
        json_payload = json.dumps(payload)
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json_payload,
            # make sure that messages from the same source are in the same
            # group, so that they're processed in order.
            MessageGroupId=source,
            MessageDeduplicationId=str(hash(json_payload)),
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            error_message = (
                f"Failed to send message to queue {self.queue_url}: {response}"  # noqa
            )
            logger.error(error_message)
            raise Exception(error_message)
        if custom_log:
            logger.info(custom_log)

    @retry_on_aws_rate_limit
    def receive_latest_messages(
        self,
        queue: Optional[str] = None,
        max_num_messages: Optional[
            int
        ] = 10,  # default 10 is max, see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html # noqa
        latest_timestamp: Optional[str] = None,
        visibility_timeout: int = default_visibility_timeout,
    ) -> list[dict]:
        """Receive messages from a queue.

        Optionally can specify a latest timestamp to filter messages.
        Processes messages in order from the same source. If a message has
        a timestamp greater than the latest timestamp, it will not be
        processed in that round.

        `visibility_timeout` is the number of seconds to wait for a message
        to become visible before we start processing it. When we receive messages
        from an SQS queue, the messages are invisible to other consumers for the
        duration of the visibility timeout, and then become visible again. But the message is
        visible in the queue for the duration of the visibility timeout.

        When we repeatedly poll, the `visibility_timeout` is what guarantees
        that we don't see a message again; once a message is pulled, it becomes
        "invisible" and unable to be fetched again during the duration of the
        timeout period.

        Note that this means that we'll need to be able to load the messages,
        do the intermediate steps, and then delete the messages, all within
        the visibility timeout period, otherwise the message expires and is
        returned to the queue and needs to be re-polled.

        While debugging, should set `visibility_timeout` to something lower, like
        30 seconds. In production, should increase the timeout.
        """
        if not queue:
            queue = self.queue
        queue_url = queue_to_queue_url_map[queue]
        if not latest_timestamp:
            logger.warning(
                f"No latest timestamp provided, using current timestamp of {current_datetime_str}"  # noqa
            )
            latest_timestamp = current_datetime_str
        if max_num_messages is not None:
            logger.info(
                f"Attempting to receive a maximum of {max_num_messages} messages from the queue {queue}"  # noqa
            )
            response = self.client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_num_messages,
                MessageAttributeNames=["All"],
                VisibilityTimeout=visibility_timeout,
            )
        else:
            logger.info(f"Receiving all messages from the queue {queue}")
            response = self.client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                VisibilityTimeout=visibility_timeout,
            )
        # "Messages" won't be in the response if there are none to query.
        messages = response.get("Messages", [])
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

    def receive_all_messages(
        self,
        queue: Optional[str] = None,
        visibility_timeout: int = default_visibility_timeout,
    ) -> list[dict]:
        """Receive all messages from a queue by polling multiple times.

        This implementation is required since we can only receive a maximum of
        10 messages at a time from SQS.
        """
        all_messages = []
        processed_message_ids = set()
        if queue:
            self._validate_queue(queue)
        else:
            queue = self.queue
        while True:
            messages = self.receive_latest_messages(
                queue=queue, max_num_messages=10, visibility_timeout=visibility_timeout
            )
            if not messages:
                break
            for message in messages:
                message_id = message["MessageId"]
                if message_id not in processed_message_ids:
                    processed_message_ids.add(message_id)
                    all_messages.append(message)
        logger.info(f"Received {len(all_messages)} messages from the queue {queue}")  # noqa
        return all_messages

    def delete_messages(self, messages: list[dict]):
        """Delete messages from the queue."""
        total_messages_deleted = 0
        for message in messages:
            self.client.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            total_messages_deleted += 1
        logger.info(f"Deleted {total_messages_deleted} latest messages from the queue")

    def clear_messages_in_queue(self):
        """Clear all messages in the queue."""
        logger.warning(
            f"Clearing all messages in the queue {self.queue_url}. DESTRUCTIVE."
        )
        self.client.purge_queue(QueueUrl=self.queue_url)


if __name__ == "__main__":
    sqs = SQS("mostLikedSyncsToBeProcessedQueue")
    # sqs = SQS("syncsToBeProcessedQueue")
    # sqs.send_message(source="test-feed", data={"test": "test"})
    messages: list[dict] = sqs.receive_latest_messages()
    print(len(messages))
    # sqs.delete_messages(messages)
