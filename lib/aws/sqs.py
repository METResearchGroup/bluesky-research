"""Wrapper client for AWS SQS."""
import json
from typing import Optional

from lib.constants import current_datetime_str
from lib.aws.helper import create_client, retry_on_aws_rate_limit

queue_to_queue_url_map = {
    "syncsToBeProcessedQueue": "https://sqs.us-east-2.amazonaws.com/517478598677/syncsToBeProcessedQueue.fifo"  # noqa
}


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
                MessageSystemAttributeNames=["All"],
                # MessageAttributeNames=["All"]
            )
        messages = response["Messages"]
        res: list[dict] = []
        for message in messages:
            breakpoint()
            message_dict = json.loads(message)
            if message.get("insert_timestamp") > latest_timestamp:
                break
            res.append(message)

        return res

    def delete_messages(self, messages: list[dict]):
        """Delete messages from the queue."""
        for message in messages:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )


if __name__ == "__main__":
    sqs = SQS("syncsToBeProcessedQueue")
    sqs.send_message(source="test-feed", data={"test": "test"})
    messages = sqs.receive_latest_messages(
        queue="syncsToBeProcessedQueue"
    )
    print(messages)
