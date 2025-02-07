"""Helper functions."""

from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from functools import wraps
import json
import logging
from memory_profiler import memory_usage
import os
import threading
import time
from typing import Optional

from atproto import Client

from lib.aws.secretsmanager import get_secret
from lib.constants import timestamp_format

current_file_directory = os.path.dirname(os.path.abspath(__file__))


def setup_env() -> None:
    env_path = os.path.abspath(os.path.join(current_file_directory, "../.env"))
    load_dotenv(env_path)


setup_env()

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_PASSWORD")
DEV_BLUESKY_HANDLE = os.getenv("DEV_BLUESKY_HANDLE")
DEV_BLUESKY_PASSWORD = os.getenv("DEV_BLUESKY_PASSWORD")
RUN_MODE = os.getenv("RUN_MODE", "test")  # local (local dev), test (CI), or prod

if RUN_MODE == "test":
    print("Running in test mode...")
    BLUESKY_HANDLE = "test"
    BLUESKY_APP_PASSWORD = "test"
    DEV_BLUESKY_HANDLE = "test"
    DEV_BLUESKY_PASSWORD = "test"
    BSKY_DATA_DIR = "~/tmp/"
    if not os.path.exists(BSKY_DATA_DIR):
        os.makedirs(BSKY_DATA_DIR)
    print(f"BSKY_DATA_DIR: {BSKY_DATA_DIR}")
else:
    BSKY_DATA_DIR = os.getenv("BSKY_DATA_DIR")


if (not BLUESKY_HANDLE or not BLUESKY_APP_PASSWORD) and RUN_MODE == "prod":
    print("Fetching secrets from AWS Secrets Manager instead of the env...")
    bsky_credentials = json.loads(get_secret("bluesky_account_credentials"))
    BLUESKY_HANDLE = bsky_credentials["bluesky_handle"]
    BLUESKY_APP_PASSWORD = bsky_credentials["bluesky_password"]
else:
    print("Fetching secrets from the local env...")

if not RUN_MODE:
    raise ValueError("RUN_MODE must be set to either 'local' or 'prod'")
if not BSKY_DATA_DIR and RUN_MODE == "prod":
    raise ValueError(
        "BSKY_DATA_DIR must be set to the path to the Bluesky data directory"
    )

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NYTIMES_API_KEY = os.getenv("NYTIMES_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_AI_STUDIO_KEY = os.getenv("GOOGLE_AI_STUDIO_KEY")
NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
LANGTRACE_API_KEY = os.getenv("LANGTRACE_API_KEY")
COMET_API_KEY = os.getenv("COMET_API_KEY")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


# https://github.com/MarshalX/atproto/discussions/286
class RateLimitedClient(Client):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._limit = self._remaining = self._reset = None

    def get_rate_limit(self):
        return self._limit, self._remaining, self._reset

    def _invoke(self, *args, **kwargs):
        response = super()._invoke(*args, **kwargs)
        self._limit = response.headers.get("RateLimit-Limit")
        self._remaining = response.headers.get("RateLimit-Remaining")
        self._reset = response.headers.get("RateLimit-Reset")
        return response


def get_client() -> RateLimitedClient:
    client = RateLimitedClient()
    try:
        client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
    except Exception as e:
        print(f"Error logging in to Bluesky: {e}\nLogging in to dev account...")
        client.login(DEV_BLUESKY_HANDLE, DEV_BLUESKY_PASSWORD)
    return client


def track_function_runtime(func):
    """Tracks the runtime of a function."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        execution_time_seconds = round(end_time - start_time)
        execution_time_minutes = execution_time_seconds // 60
        execution_time_leftover_seconds = execution_time_seconds - (
            60 * execution_time_minutes
        )
        print(
            f"Execution time for {func.__name__}: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds"
        )  # noqa
        return result

    return wrapper


def track_memory_usage(func):
    """Tracks the memory usage of a function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        mem_before = memory_usage(-1, interval=0.1, timeout=1)
        result = func(*args, **kwargs)
        mem_after = memory_usage(-1, interval=0.1, timeout=1)

        print(
            f"Memory usage for {func.__name__}: {max(mem_after) - min(mem_before)} MB"
        )  # noqa

        return result

    return wrapper


def track_performance(func):
    """Tracks both the runtime and memory usage of a function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        mem_before = memory_usage(-1, interval=0.1, timeout=1)

        result = func(*args, **kwargs)

        end_time = time.time()
        mem_after = memory_usage(-1, interval=0.1, timeout=1)

        execution_time_seconds = round(end_time - start_time)
        execution_time_minutes = execution_time_seconds // 60
        execution_time_leftover_seconds = execution_time_seconds - (
            60 * execution_time_minutes
        )
        try:
            func_name = func.__name__
        except AttributeError:
            func_name = func.__class__.__name__
        print(
            f"Execution time for {func_name}: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds"
        )  # noqa
        print(f"Memory usage for {func_name}: {max(mem_after) - min(mem_before)} MB")  # noqa
        return result

    return wrapper


def add_rate_limit(rate_limit: float):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            print(f"Sleeping for {rate_limit} seconds...")
            time.sleep(rate_limit)
            return res

        return wrapper

    return decorator


class ThreadSafeCounter:
    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.counter += 1
            return self.counter

    def reset(self):
        with self.lock:
            self.counter = 0

    def get_value(self):
        with self.lock:
            return self.counter


def create_batches(batch_list, batch_size) -> list[list]:
    """Create batches of a given size.

    NOTE: if batching becomes a problem due to, for example, keeping things
    in memory, can move to yielding instead. Loading everything in memory
    was easier to iterate first, but can investigate things like yields.
    """
    batches: list[list] = []
    for i in range(0, len(batch_list), batch_size):
        batches.append(batch_list[i : i + batch_size])
    return batches


def generate_current_datetime_str() -> str:
    return datetime.now(timezone.utc).strftime(timestamp_format)


def get_partition_dates(
    start_date: str,
    end_date: str,
    exclude_partition_dates: Optional[list[str]] = None,
) -> list[str]:
    """Returns a list of dates between start_date and end_date, inclusive.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of dates in YYYY-MM-DD format
    """
    partition_dates = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_timestamp:
        date_str = current_date.strftime("%Y-%m-%d")
        if exclude_partition_dates and date_str in exclude_partition_dates:
            current_date += timedelta(days=1)
            continue
        partition_dates.append(date_str)
        current_date += timedelta(days=1)

    return partition_dates
