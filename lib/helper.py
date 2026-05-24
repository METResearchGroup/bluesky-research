"""Helper functions.

Note: `lib.helper` is intentionally a grab-bag and performs environment setup at
import time (historically; env var loading has been migrated to
`lib.load_env_vars`). Datetime-specific helpers have been migrated to
`lib.datetime_utils` to avoid importing this module from lightweight, pure
date-math call sites.
"""

from datetime import datetime, timezone, timedelta
from functools import wraps
import inspect
import logging
from memory_profiler import memory_usage
import threading
import time
from typing import Literal, Optional, TypeVar, Callable, ParamSpec

from atproto import Client

from lib.constants import timestamp_format, partition_date_format
from lib.load_env_vars import EnvVarsContainer

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
        client.login(
            EnvVarsContainer.get_env_var("BLUESKY_HANDLE", required=True),
            EnvVarsContainer.get_env_var("BLUESKY_PASSWORD", required=True),
        )
    except Exception as e:
        print(f"Error logging in to Bluesky: {e}\nLogging in to dev account...")
        client.login(
            EnvVarsContainer.get_env_var("DEV_BLUESKY_HANDLE"),
            EnvVarsContainer.get_env_var("DEV_BLUESKY_PASSWORD"),
        )
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


def _calculate_memory_stats(mem_usage_list: list, mem_baseline: float) -> dict:
    """Calculate comprehensive memory statistics from a list of memory samples.

    Args:
        mem_usage_list: List of memory usage samples in MB
        mem_baseline: Baseline memory before function execution in MB

    Returns:
        Dictionary with memory statistics: peak, average, end, increase, growth_rate
    """
    if not isinstance(mem_usage_list, list) or len(mem_usage_list) == 0:
        return {
            "peak": float(mem_baseline),
            "average": float(mem_baseline),
            "end": float(mem_baseline),
            "increase": 0.0,
            "growth_rate": None,
        }

    # Convert all to float
    mem_samples = [float(x) for x in mem_usage_list]

    peak_memory = max(mem_samples)
    avg_memory = sum(mem_samples) / len(mem_samples)
    end_memory = mem_samples[-1]
    memory_increase = peak_memory - mem_baseline

    # Calculate growth rate (MB/second) using linear regression slope
    # Only if we have enough samples (at least 10)
    growth_rate = None
    if len(mem_samples) >= 10:
        # Simple linear regression: y = mx + b
        # x = time index (0, 1, 2, ...), y = memory value
        n = len(mem_samples)
        sum_x = sum(range(n))
        sum_y = sum(mem_samples)
        sum_xy = sum(i * mem for i, mem in enumerate(mem_samples))
        sum_x2 = sum(i * i for i in range(n))

        # Slope m = (n*sum(xy) - sum(x)*sum(y)) / (n*sum(x^2) - sum(x)^2)
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator != 0:
            slope = (n * sum_xy - sum_x * sum_y) / denominator
            # Convert to MB/second (assuming 0.1 second intervals)
            growth_rate = slope / 0.1

    return {
        "peak": peak_memory,
        "average": avg_memory,
        "end": end_memory,
        "increase": memory_increase,
        "growth_rate": growth_rate,
    }


def track_memory_usage(func):
    """Tracks the memory usage of a function.

    Tracks comprehensive memory metrics:
    - Peak memory: Maximum memory used during execution (critical for OOM prevention)
    - Average memory: Mean memory usage over execution time (useful for capacity planning)
    - End memory: Memory at function completion (shows retained memory)
    - Memory increase: Peak memory minus baseline (shows function's contribution)
    - Growth rate: Memory growth rate in MB/second (useful for leak detection in long-running functions)
    """

    return (
        _track_memory_usage_async(func)
        if inspect.iscoroutinefunction(func)
        else _track_memory_usage_sync(func)
    )


def _track_memory_usage_async(func):
    """Async-capable implementation of `track_memory_usage`.

    Important: `memory_profiler.memory_usage((func, ...))` does not support async
    functions and can execute them incorrectly (creating un-awaited coroutines).
    For coroutine functions, we fall back to lightweight before/after sampling.

    We intentionally return a *sync* wrapper that produces an awaitable. This keeps
    the decorated callable from being a coroutine function (important because
    `unittest.mock.patch` will otherwise replace it with an `AsyncMock`, and some
    existing tests set `return_value` to a coroutine).
    """

    @wraps(func)
    def wrapped_async(*args, **kwargs):
        async def _runner():
            mem_baseline = memory_usage(-1, interval=0.1, timeout=0.1)[0]
            result = await func(*args, **kwargs)
            mem_end = memory_usage(-1, interval=0.1, timeout=0.1)[0]

            func_name = func.__name__
            print(  # noqa
                "\n".join(
                    [
                        f"Memory usage for {func_name} (async):",
                        f"  Baseline: {mem_baseline:.2f} MB",
                        f"  End: {mem_end:.2f} MB (delta: {mem_end - mem_baseline:.2f} MB)",
                        "  Note: peak/average/growth-rate are not available for async functions",
                    ]
                )
            )
            return result

        return _runner()

    return wrapped_async


def _track_memory_usage_sync(func):
    """Sync implementation of `track_memory_usage` using `memory_profiler` sampling."""

    @wraps(func)
    def wrapped_sync(*args, **kwargs):
        # Get baseline memory before execution (single sample)
        mem_baseline = memory_usage(-1, interval=0.1, timeout=0.1)[0]

        # Use memory_usage() to profile the function execution itself
        # Use retval=True so we don't have to call the function again.
        # This should return (mem_usage_list, retval) where mem_usage_list contains
        # memory samples taken during function execution.
        mem_usage_result = memory_usage(
            (func, args, kwargs),  # type: ignore[arg-type]
            interval=0.1,
            retval=True,
            max_iterations=1,
        )

        # Extract result and memory measurements
        if isinstance(mem_usage_result, tuple) and len(mem_usage_result) == 2:
            mem_usage_list, result = mem_usage_result
        else:
            # Defensive fallback: if memory_profiler returns just samples, avoid
            # re-executing side-effectful functions by returning the samples and
            # running the function once only as a last resort.
            mem_usage_list = (
                mem_usage_result if isinstance(mem_usage_result, list) else []
            )
            result = func(*args, **kwargs)

        # Calculate comprehensive memory statistics
        stats = _calculate_memory_stats(mem_usage_list, mem_baseline)

        # Build output message
        func_name = func.__name__
        output_lines = [
            f"Memory usage for {func_name}:",
            f"  Peak: {stats['peak']:.2f} MB (increase: {stats['increase']:.2f} MB)",
            f"  Average: {stats['average']:.2f} MB",
            f"  End: {stats['end']:.2f} MB",
        ]

        # Add growth rate if available (for long-running functions)
        if stats["growth_rate"] is not None:
            growth_indicator = "⚠️" if stats["growth_rate"] > 1.0 else "✓"
            output_lines.append(
                f"  Growth rate: {stats['growth_rate']:.2f} MB/s {growth_indicator}"
            )

        print("\n".join(output_lines))  # noqa

        return result

    return wrapped_sync


def track_performance(func):
    """Tracks both the runtime and memory usage of a function.

    Tracks comprehensive performance metrics:
    - Execution time: Total runtime in minutes and seconds
    - Peak memory: Maximum memory used during execution (critical for OOM prevention)
    - Average memory: Mean memory usage over execution time (useful for capacity planning)
    - End memory: Memory at function completion (shows retained memory)
    - Memory increase: Peak memory minus baseline (shows function's contribution)
    - Growth rate: Memory growth rate in MB/second (useful for leak detection in long-running functions)
    """

    return (
        _track_performance_async(func)
        if inspect.iscoroutinefunction(func)
        else _track_performance_sync(func)
    )


def _track_performance_async(func):
    """Async-capable implementation of `track_performance`.

    Important: `memory_profiler.memory_usage((func, ...))` does not support async
    functions and can execute them incorrectly (creating un-awaited coroutines).
    For coroutine functions, we fall back to lightweight before/after sampling.

    We intentionally return a *sync* wrapper that produces an awaitable. This keeps
    the decorated callable from being a coroutine function (important because
    `unittest.mock.patch` will otherwise replace it with an `AsyncMock`, and some
    existing tests set `return_value` to a coroutine).
    """

    @wraps(func)
    def wrapped_async(*args, **kwargs):
        async def _runner():
            start_time = time.time()
            mem_baseline = memory_usage(-1, interval=0.1, timeout=0.1)[0]

            result = await func(*args, **kwargs)

            mem_end = memory_usage(-1, interval=0.1, timeout=0.1)[0]
            end_time = time.time()

            execution_time_seconds = round(end_time - start_time)
            execution_time_minutes = execution_time_seconds // 60
            execution_time_leftover_seconds = execution_time_seconds - (
                60 * execution_time_minutes
            )

            try:
                func_name = func.__name__
            except AttributeError:
                func_name = func.__class__.__name__

            print(  # noqa
                "\n".join(
                    [
                        f"Execution time for {func_name}: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds",
                        f"Memory usage for {func_name} (async):",
                        f"  Baseline: {mem_baseline:.2f} MB",
                        f"  End: {mem_end:.2f} MB (delta: {mem_end - mem_baseline:.2f} MB)",
                        "  Note: peak/average/growth-rate are not available for async functions",
                    ]
                )
            )
            return result

        return _runner()

    return wrapped_async


def _track_performance_sync(func):
    """Sync implementation of `track_performance` using `memory_profiler` sampling."""

    @wraps(func)
    def wrapped_sync(*args, **kwargs):
        start_time = time.time()

        # Get baseline memory before execution (single sample)
        mem_baseline = memory_usage(-1, interval=0.1, timeout=0.1)[0]

        # Use memory_usage() to profile the function execution itself
        # Use retval=True so we don't have to call the function again.
        mem_usage_result = memory_usage(
            (func, args, kwargs),  # type: ignore[arg-type]
            interval=0.1,
            retval=True,
            max_iterations=1,
        )

        end_time = time.time()

        # Extract result and memory measurements
        if isinstance(mem_usage_result, tuple) and len(mem_usage_result) == 2:
            mem_usage_list, result = mem_usage_result
        else:
            # Defensive fallback: if memory_profiler returns just samples, avoid
            # re-executing side-effectful functions by returning the samples and
            # running the function once only as a last resort.
            mem_usage_list = (
                mem_usage_result if isinstance(mem_usage_result, list) else []
            )
            result = func(*args, **kwargs)

        execution_time_seconds = round(end_time - start_time)
        execution_time_minutes = execution_time_seconds // 60
        execution_time_leftover_seconds = execution_time_seconds - (
            60 * execution_time_minutes
        )

        # Calculate comprehensive memory statistics
        stats = _calculate_memory_stats(mem_usage_list, mem_baseline)

        try:
            func_name = func.__name__
        except AttributeError:
            func_name = func.__class__.__name__

        # Build output message
        output_lines = [
            f"Execution time for {func_name}: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds",
            f"Memory usage for {func_name}:",
            f"  Peak: {stats['peak']:.2f} MB (increase: {stats['increase']:.2f} MB)",
            f"  Average: {stats['average']:.2f} MB",
            f"  End: {stats['end']:.2f} MB",
        ]

        # Add growth rate if available (for long-running functions)
        if stats["growth_rate"] is not None:
            growth_indicator = "⚠️" if stats["growth_rate"] > 1.0 else "✓"
            output_lines.append(
                f"  Growth rate: {stats['growth_rate']:.2f} MB/s {growth_indicator}"
            )

        print("\n".join(output_lines))  # noqa
        return result

    return wrapped_sync


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
