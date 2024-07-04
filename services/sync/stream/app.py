import sys
import signal
import threading
import time

from flask import Flask

from services.sync.stream import firehose
from services.sync.stream.data_filter import operations_callback


app = Flask(__name__)
firehose_name = "firehose_stream"


def start_app():
    start_time = time.time()
    stream_stop_event = threading.Event()
    stream_args = (firehose_name, operations_callback, stream_stop_event)
    stream_thread = threading.Thread(target=firehose.run, args=stream_args)
    print('Starting data stream...')
    # something like 3:49pm UTC
    start_time_str = time.strftime(
        '%Y-%m-%d %H:%M:%S', time.gmtime(start_time))
    print(f"Start time: {start_time_str}")
    stream_thread.start()

    def sigint_handler(*_):
        print('Stopping data stream...')
        stream_stop_event.set()
        stream_thread.join()  # wait for thread to finish
        end_time = time.time()
        end_time_str = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.gmtime(end_time))
        total_minutes = round((end_time - start_time) / 60, 1)
        print(f"End time: {end_time_str}")
        print(f"Total runtime: {total_minutes} minutes")
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)


if __name__ == "__main__":
    start_app()
