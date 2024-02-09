import sys
import signal
import threading

from flask import Flask

from services.sync.stream import firehose
from services.sync.stream.data_filter import operations_callback


app = Flask(__name__)

stream_stop_event = threading.Event()
stream_args = ("test", operations_callback, stream_stop_event)
stream_thread = threading.Thread(target=firehose.run, args=stream_args)
stream_thread.start()


def sigint_handler(*_):
    print('Stopping data stream...')
    stream_stop_event.set()
    stream_thread.join() # wait for thread to finish
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)
