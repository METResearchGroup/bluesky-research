"""Helper functions."""

from datetime import datetime, timezone
from dotenv import load_dotenv
from functools import wraps
import json
import logging
from memory_profiler import memory_usage
import os
import threading
import time

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

if not BLUESKY_HANDLE or not BLUESKY_APP_PASSWORD:
    print("Fetching secrets from AWS Secrets Manager instead of the env...")
    bsky_credentials = json.loads(get_secret("bluesky_account_credentials"))
    BLUESKY_HANDLE = bsky_credentials["bluesky_handle"]
    BLUESKY_APP_PASSWORD = bsky_credentials["bluesky_password"]
else:
    print("Fetching secrets from the local env...")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NYTIMES_API_KEY = os.getenv("NYTIMES_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_AI_STUDIO_KEY = os.getenv("GOOGLE_AI_STUDIO_KEY")
NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
LANGTRACE_API_KEY = os.getenv("LANGTRACE_API_KEY")

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
    client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
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
        print(
            f"Execution time for {func.__name__}: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds"
        )  # noqa
        print(
            f"Memory usage for {func.__name__}: {max(mem_after) - min(mem_before)} MB"
        )  # noqa
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
