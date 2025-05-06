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
                self._tokens = min(
                    int(self._tokens) + self.get_num_tokens_to_refill(),
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
                    seconds_to_sleep = (int(abs(self._tokens)) * self._token_rate) + 10
                    print(
                        f"Not enough tokens: {self._tokens}\tWaiting for refill for {seconds_to_sleep} seconds..."
                    )
                    await asyncio.sleep(seconds_to_sleep)  # back-off, wait for refill.

                    # NOTE: I know having dupes is bad. This is a hacky workaround
                    # for now.
                    # TODO: next time this service is used, can properly refactor this logic.
                    # Hacky workaround works for now, allows the thread to hand back
                    # control to the main thread when it's trying to regenerate
                    # tokens, as opposed to checking every 0.5 seconds. Useful for
                    # backfill, which can consume up to 100 requests per DID (due to pagination),
                    # so we need to give the bucket some time to regenerate tokens as well
                    # as hand back control of the main thread to other workers.
                    self._tokens = min(
                        int(self._tokens) + self.get_num_tokens_to_refill(),
                        self._max_tokens,
                    )
                    self._last_refill = now
                    print(
                        f"Refilled tokens after {seconds_to_sleep} seconds: {self._tokens} tokens in bucket now."
                    )

    def get_num_tokens_to_refill(self) -> int:
        now = time.perf_counter()
        time_since_last_refill = now - self._last_refill
        return int(time_since_last_refill * self._token_rate)

    def get_tokens(self) -> int:
        """Get the number of tokens in the bucket.

        Returns:
            int, the number of tokens in the bucket.
        """
        return self._tokens

    def set_tokens(self, tokens: int) -> None:
        """Set the number of tokens in the bucket.

        Args:
            tokens: int, the number of tokens to set the bucket to.
        """
        self._tokens = tokens


class TokenBucket:
    """Synchronous implementation of a token bucket rate limiter."""

    def __init__(self, max_tokens: int, window_seconds: int):
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

    def _acquire(self) -> None:
        """Acquires a token from the bucket.

        Blocks until a token is available. Refills tokens based on elapsed time.
        """
        while True:
            now = time.perf_counter()
            time_since_last_refill = now - self._last_refill
            self._tokens = min(
                self._tokens + (time_since_last_refill * self._token_rate),
                self._max_tokens,
            )
            self._last_refill = now

            if self._tokens >= 1:
                self._tokens -= 1
                return
            else:
                # Sleep for a short time before checking again
                time.sleep(0.05)


def calculate_sleep_seconds(reset_unix: int) -> int:
    """Given the unix reset time from a response header, calculate
    how long to sleep for."""
    now = time.time()
    return reset_unix - now
