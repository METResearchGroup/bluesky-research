"""Writes the firehose posts for the post records."""

import time

from services.sync.stream.export_data import export_batch

sleep_time_minutes = 5
sleep_time_seconds = sleep_time_minutes * 60


def run_firehose_writes():
    export_batch(external_store=["local"], clear_filepaths=True)


def main():
    while True:
        run_firehose_writes()
        time.sleep(sleep_time_seconds)


if __name__ == "__main__":
    main()
