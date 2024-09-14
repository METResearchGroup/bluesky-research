"""Background queue for writing user session logs to S3."""

import os
import queue
import time

from lib.aws.s3 import S3
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)
s3 = S3()

# Create a thread-safe queue
log_queue = queue.Queue()

time_to_flush_seconds = 120  # every 2 minutes
max_num_logs = 100  # max logs to keep in queue before flushing


def background_s3_writer():
    while True:
        logs_to_write = []
        start_time = time.time()  # Record the start time
        # Collect logs from the queue
        while not log_queue.empty() and len(logs_to_write) < max_num_logs:
            if (
                time.time() - start_time >= time_to_flush_seconds
            ):  # Check if time_to_flush_seconds has passed
                break
            try:
                logs_to_write.append(log_queue.get_nowait())
            except queue.Empty:
                break
        if logs_to_write:
            timestamp = generate_current_datetime_str()
            filename = f"user_session_logs_{timestamp}.jsonl"
            key = os.path.join("user_session_logs", filename)
            s3.write_dicts_jsonl_to_s3(data=logs_to_write, key=key)
            logger.info(
                f"Exported {len(logs_to_write)}user session logs to S3 to key={key}"
            )  # noqa
        time.sleep(10)
