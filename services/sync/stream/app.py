"""Flask app for connecting to the firehose data stream and running  pipeline."""

import sys
import signal
import threading
import time
from typing import Optional

from flask import Flask

from services.sync.stream import firehose
from services.sync.stream.data_filter import operations_callback
from services.sync.stream.setup import setup_sync_export_system


app = Flask(__name__)
default_firehose_name = "firehose_stream"


def start_app(
    firehose_name: Optional[str] = default_firehose_name, restart_cursor: bool = False
):
    start_time = time.time()
    stream_stop_event = threading.Event()

    # Create sync export context
    context = setup_sync_export_system()

    # Create callback with context
    def callback_with_context(operations_by_type: dict) -> bool:
        return operations_callback(operations_by_type, context)

    stream_kwargs = {
        "name": firehose_name,
        "operations_callback": callback_with_context,
        "stream_stop_event": stream_stop_event,
        "restart_cursor": restart_cursor,
    }
    stream_thread = threading.Thread(target=firehose.run, kwargs=stream_kwargs)
    print("Starting data stream...")
    # something like 3:49pm UTC
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_time))
    print(f"Start time: {start_time_str}")
    stream_thread.start()

    def sigint_handler(*_):
        print("Stopping data stream...")
        stream_stop_event.set()
        stream_thread.join()  # wait for thread to finish
        end_time = time.time()
        end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_time))
        total_minutes = round((end_time - start_time) / 60, 1)
        print(f"End time: {end_time_str}")
        print(f"Total runtime: {total_minutes} minutes")
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)


if __name__ == "__main__":
    start_app()
