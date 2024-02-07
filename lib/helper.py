from dotenv import load_dotenv
import logging
import os
import time

from atproto import Client

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../.env"))
load_dotenv(env_path)

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_PASSWORD")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

client = Client()
client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def track_function_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        execution_time_seconds = round(end_time - start_time)
        execution_time_minutes = execution_time_seconds // 60
        execution_time_leftover_seconds = execution_time_seconds - (60 * execution_time_minutes)

        print(f"Execution time: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds")

        return result

    return wrapper
