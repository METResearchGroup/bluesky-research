"""Helper functions.

Note: `lib.helper` is intentionally a grab-bag and performs environment setup at
import time. Datetime-specific helpers have been migrated to
`lib.datetime_utils` to avoid importing this module from lightweight, pure
date-math call sites.
"""

from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from functools import wraps
import json
import logging
from memory_profiler import memory_usage
import os
import threading
import time
from typing import Literal, Optional, TypeVar, Callable, ParamSpec

from atproto import Client

from lib.aws.secretsmanager import get_secret
from lib.constants import timestamp_format, partition_date_format

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
        # exist_ok=True handles TOCTOU race condition when parallel test workers
        # concurrently import this module and attempt to create the directory
        os.makedirs(BSKY_DATA_DIR, exist_ok=True)
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
    def __init__(self, initial_value=0):
        self.counter = initial_value
        self.lock = threading.Lock()

    def increment(self, delta=1):
        with self.lock:
            self.counter += delta
            return self.counter

    def reset(self, value=0):
        with self.lock:
            self.counter = value

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


P = ParamSpec("P")
R = TypeVar("R")


def rate_limit(delay_seconds: int = 5) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Simple rate limiter decorator that adds a delay after each function call.

    Args:
        delay_seconds: Number of seconds to wait after function execution (default: 5)

    Returns:
        A decorated function that will pause for the specified time after execution
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # Execute the original function
            result = func(*args, **kwargs)

            # Wait for the specified delay
            time.sleep(delay_seconds)

            # Return the original result
            return result

        return wrapper

    return decorator


def determine_backfill_latest_timestamp(
    backfill_duration: Optional[int] = None,
    backfill_period: Optional[Literal["days", "hours"]] = None,
) -> Optional[str]:
    """Calculates the timestamp for backfilling data based on a duration and period.

    This function computes a historical timestamp by subtracting a specified duration
    from the current UTC time. The duration can be specified in either days or hours.

    Args:
        backfill_duration (Optional[int]): The number of time units to look back. Must be a positive integer.
        backfill_period (Optional[Literal["days", "hours"]]): The time unit for backfilling.
            Must be either "days" or "hours".

    Returns:
        Optional[str]: A timestamp string in format YYYY-MM-DD-HH:MM:SS (from lib/constants.py timestamp_format)
            representing the calculated historical point in time, or None if invalid parameters
            are provided.

    Raises:
        ValueError: If backfill_duration is provided but is not a positive integer.

    Control Flow:
        1. Validates backfill_duration is positive if provided (raises ValueError if invalid)
        2. Returns None early if backfill_duration is None or backfill_period is invalid
        3. Gets current UTC time
        4. Calculates backfill_time by subtracting duration based on period ("days" or "hours")
        5. Formats and returns timestamp string
    """
    # Validate backfill_duration is positive if provided
    if backfill_duration is not None and backfill_duration <= 0:
        raise ValueError("backfill_duration must be a positive integer")

    # Early return for invalid parameters
    if backfill_duration is None or backfill_period not in ["days", "hours"]:
        return None

    # Calculate backfill time
    current_time = datetime.now(timezone.utc)
    if backfill_period == "days":
        backfill_time = current_time - timedelta(days=backfill_duration)
        logger.info(f"Backfilling {backfill_duration} days of data.")
    else:  # backfill_period == "hours"
        backfill_time = current_time - timedelta(hours=backfill_duration)
        logger.info(f"Backfilling {backfill_duration} hours of data.")

    # Format and return timestamp
    return backfill_time.strftime(timestamp_format)


# TODO: a lot of the utilities in lib/helper.py are time-related, so could
# eventually move these to a lib/time_utils.py file.
def get_partition_date_from_timestamp(
    timestamp: str,
    ts_fmt: str | None = None,
    partition_date_fmt: str | None = None,
) -> str:
    """Get the partition date from a timestamp."""
    if ts_fmt is None:
        ts_fmt = timestamp_format
    if partition_date_fmt is None:
        partition_date_fmt = partition_date_format
    return datetime.strptime(timestamp, ts_fmt).strftime(partition_date_fmt)
