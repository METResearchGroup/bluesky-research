from prometheus_client import Counter, Gauge, Summary

# Customize and namespace specifically for backfill
BACKFILL_REQUESTS = Counter(
    "backfill_endpoint_requests_total",
    "Total number of requests attempted per endpoint",
    ["endpoint"],
)

BACKFILL_ERRORS = Counter(
    "backfill_endpoint_request_errors_total",
    "Total number of failed requests per endpoint",
    ["endpoint"],
)

BACKFILL_LATENCY_SECONDS = Summary(
    "backfill_endpoint_request_latency_seconds",
    "Request latency summary per endpoint",
    ["endpoint"],
)

BACKFILL_INFLIGHT = Gauge(
    "backfill_endpoint_inflight_requests",
    "In-flight concurrent requests per endpoint",
    ["endpoint"],
)

BACKFILL_QUEUE_SIZE = Gauge(
    "backfill_endpoint_queue_size",
    "Number of pending items in the decode queue",
    ["endpoint"],
)

BACKFILL_TOKENS_LEFT = Gauge(
    "backfill_endpoint_tokens_remaining",
    "Remaining token bucket tokens per endpoint",
    ["endpoint"],
)

BACKFILL_DB_FLUSH_SECONDS = Summary(
    "backfill_endpoint_db_flush_seconds",
    "Duration of SQLite flush operations per endpoint",
    ["endpoint"],
)
