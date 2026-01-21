"""Prometheus metrics."""

from prometheus_client import Counter, Gauge, Histogram

REQUESTS_TOTAL = Counter(
    "service_requests_total",
    "Total number of HTTP/API requests attempted",
    ["service", "endpoint"],
)

REQUEST_ERRORS_TOTAL = Counter(
    "service_request_errors_total",
    "Total number of HTTP/API requests failed",
    ["service", "endpoint"],
)

REQUEST_SUCCESS_TOTAL = Counter(
    "service_request_success_total",
    "Total number of HTTP/API requests that succeeded",
    ["service", "endpoint"],
)

REQUEST_LATENCY_SECONDS = Histogram(
    "service_request_latency_seconds",
    "Request latency histogram",
    ["service", "endpoint"],
    buckets=[0.05, 0.1, 0.2, 0.5, 1, 2, 5],
)

EVENT_LOOP_LAG_SECONDS = Gauge(
    "service_event_loop_lag_seconds", "Detected event loop latency lag in seconds"
)
