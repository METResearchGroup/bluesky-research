"""Background queue for writing user session logs to S3."""

from datetime import datetime, timezone
import os
import queue
import time

from lib.aws.s3 import S3
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)
s3 = S3()

# Create a thread-safe queue
log_queue = queue.Queue()

time_to_flush_minutes = 20
time_to_flush_seconds = time_to_flush_minutes * 60
max_num_logs = 10_000  # max logs to keep in queue before flushing


def background_s3_writer():
    while True:
        logs_to_write = []
        start_time = time.time()
        # Collect logs from the queue and flush to list.
        while not log_queue.empty() and len(logs_to_write) < max_num_logs:
            if (
                time.time() - start_time >= time_to_flush_seconds
            ):  # should never be hit, since time to clear flush is very quick.
                logger.info("Time to flush has passed, breaking...")
                break
            try:
                logs_to_write.append(log_queue.get_nowait())
            except queue.Empty:
                # should also never be explicitly hit (unless it's the start
                # of the thread, since log_queue.empty() will hit before
                # this will hit.
                logger.info("Queue is empty, breaking...")
                break
        if logs_to_write:
            timestamp = generate_current_datetime_str()
            partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            filename = f"user_session_logs_{timestamp}.jsonl"
            key = os.path.join(
                "user_session_logs",
                f"partition_date={partition_date}",
                filename,
            )
            backup_key = os.path.join(
                "backup_user_session_logs", f"partition_date={partition_date}", filename
            )
            s3.write_dicts_jsonl_to_s3(data=logs_to_write, key=key)
            s3.write_dicts_jsonl_to_s3(data=logs_to_write, key=backup_key)
            logger.info(
                f"Exported {len(logs_to_write)} user session logs to S3 to key={key} and backup key {backup_key}"
            )  # noqa
        time.sleep(time_to_flush_seconds)
