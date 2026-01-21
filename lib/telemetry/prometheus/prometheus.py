"""Prometheus instrumentation with decorator pattern support."""

import functools
from typing import Any, Callable, TypeVar

from lib.telemetry.prometheus.metrics import (
    REQUEST_ERRORS_TOTAL,
    REQUEST_LATENCY_SECONDS,
    REQUEST_SUCCESS_TOTAL,
    REQUESTS_TOTAL,
)

R = TypeVar("R")


class Prometheus:
    """Prometheus instrumentation class with decorator pattern support.

    Provides decorators for instrumenting HTTP requests and other operations
    with Prometheus metrics. Supports synchronous functions.

    Example:
        >>> prometheus = Prometheus()
        >>> @prometheus.track_http_request(service="llm_service", endpoint="/batch_completion")
        ... def classify_batch(batch):
        ...     return classifier.classify_batch(batch)
    """

    def __init__(self):
        """Initialize Prometheus instrumentation."""
        pass

    def track_http_request(
        self,
        service: str,
        endpoint: str | None = None,
        extract_endpoint: Callable[..., str] | None = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:
        """Decorator to track HTTP requests with Prometheus metrics.

        Tracks:
        - Total request count (REQUESTS_TOTAL)
        - Successful request count (REQUEST_SUCCESS_TOTAL)
        - Error count (REQUEST_ERRORS_TOTAL)
        - Request latency (REQUEST_LATENCY_SECONDS)

        Args:
            service: Service name label (e.g., "llm_service", "api", "database")
            endpoint: Static endpoint label (e.g., "/batch_completion"). If None and
                extract_endpoint is provided, endpoint will be extracted from
                function arguments.
            extract_endpoint: Optional function to extract endpoint from
                function arguments. Takes the same args/kwargs as the decorated
                function and returns an endpoint string.

        Returns:
            Decorator function that wraps the original function with metrics.

        Example:
            >>> @prometheus.track_http_request(
            ...     service="llm_service",
            ...     endpoint="/batch_completion"
            ... )
            ... def classify_batch(batch):
            ...     return classifier.classify_batch(batch)

            >>> @prometheus.track_http_request(
            ...     service="api",
            ...     extract_endpoint=lambda url, **kwargs: urlparse(url).path
            ... )
            ... def make_request(url: str, **kwargs):
            ...     return requests.get(url, **kwargs)
        """

        def decorator(func: Callable[..., R]) -> Callable[..., R]:
            return self._track_sync_http_request(
                func, service, endpoint, extract_endpoint
            )

        return decorator

    def _track_sync_http_request(
        self,
        func: Callable[..., R],
        service: str,
        endpoint: str | None,
        extract_endpoint: Callable[..., str] | None,
    ) -> Callable[..., R]:
        """Track synchronous HTTP requests."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            # Determine endpoint label
            if extract_endpoint:
                endpoint_label = extract_endpoint(*args, **kwargs)
            elif endpoint:
                endpoint_label = endpoint
            else:
                endpoint_label = func.__name__

            # Track latency and request
            with REQUEST_LATENCY_SECONDS.labels(
                service=service, endpoint=endpoint_label
            ).time():
                try:
                    result = func(*args, **kwargs)

                    # Increment total and success counters
                    REQUESTS_TOTAL.labels(
                        service=service, endpoint=endpoint_label
                    ).inc()
                    REQUEST_SUCCESS_TOTAL.labels(
                        service=service, endpoint=endpoint_label
                    ).inc()

                    return result
                except Exception:
                    # Increment total and error counters
                    REQUESTS_TOTAL.labels(
                        service=service, endpoint=endpoint_label
                    ).inc()
                    REQUEST_ERRORS_TOTAL.labels(
                        service=service, endpoint=endpoint_label
                    ).inc()
                    raise

        return wrapper


class PrometheusLitellmAdapter:
    """Adapter interface for connecting LiteLLM to Prometheus.

    LiteLLM already supports Prometheus, so this is a wrapper to simplify that
    integration: https://docs.litellm.ai/docs/proxy/prometheus
    """

    pass
