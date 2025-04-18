"""Helper classes for managing rate limits."""

import asyncio
import time


class AsyncTokenBucket:
    """Async class for managing a token bucket."""

    def __init__(self, max_tokens: int, window_seconds: int) -> None:
        """Initialize the token bucket.

        Args:
            max_tokens: int, the maximum number of tokens in the bucket.
            window_seconds: int, the time window in seconds over which the tokens are refilled.
        """
        self._max_tokens = max_tokens
        self._window_seconds = window_seconds
        self._tokens = max_tokens
        self._token_rate = self._max_tokens / self._window_seconds
        self._last_refill = time.perf_counter()
        self._lock = asyncio.Lock()

    async def _acquire(self) -> None:
        """Acquires a token from the bucket.

        Checks to see if we have enough tokens to send a request. Then sleeps.
        Tokens determined by taking the min of the current expected number of
        tokens (current tokens + tokens added since last reset) and the max
        tokens. This allows the # of tokens to be dynamic yet always bounded
        by the max tokens.
        """
        while True:
            async with self._lock:
                now = time.perf_counter()
                time_since_last_refill = now - self._last_refill
                self._tokens = min(
                    self._tokens + (time_since_last_refill * self._token_rate),
                    self._max_tokens,
                )
                self._last_refill = now

                # only proceed if we have at least 1 token left in the
                # bucket. If so, we return ("grab a token"). Otherwise,
                # we sleep and wait for the next refill. If the bucket hasn't
                # had enough time to refill, then self._tokens will be < 1,
                # meaning the bucket is empty. This will trigger the sleep.
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                else:
                    await asyncio.sleep(0.05)  # back-off, wait for refill.
