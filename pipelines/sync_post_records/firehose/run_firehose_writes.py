"""Writes the firehose posts for the post records."""

import time

from lib.log.logger import get_logger
from services.sync.stream.export_data import export_batch

sleep_time_minutes = 5
sleep_time_seconds = sleep_time_minutes * 60

logger = get_logger(__name__)


def run_firehose_writes():
    logger.info("Running firehose writes.")
    export_batch(external_store=["local"], clear_filepaths=True)
    logger.info("Finished firehose writes.")


def main():
    while True:
        run_firehose_writes()
        time.sleep(sleep_time_seconds)


if __name__ == "__main__":
    main()
