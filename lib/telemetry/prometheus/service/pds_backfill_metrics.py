from prometheus_client import Counter, Gauge, Summary, Histogram

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

BACKFILL_NETWORK_ERRORS = Counter(
    "backfill_endpoint_network_errors_total",
    "Total number of network-related errors (timeouts, disconnects) per endpoint",
    ["endpoint", "error_type"],
)

BACKFILL_RETRIES = Counter(
    "backfill_endpoint_retries_total",
    "Total number of retry attempts per endpoint",
    ["endpoint"],
)

BACKFILL_SUCCESS_RATIO = Gauge(
    "backfill_endpoint_success_ratio",
    "Ratio of successful requests to total requests",
    ["endpoint"],
)

BACKFILL_LATENCY_SECONDS = Summary(
    "backfill_endpoint_request_latency_seconds",
    "Request latency summary per endpoint",
    ["endpoint"],
)

BACKFILL_LATENCY_HISTOGRAM = Histogram(
    "backfill_endpoint_request_latency_histogram",
    "Request latency histogram per endpoint",
    ["endpoint"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

BACKFILL_INFLIGHT = Gauge(
    "backfill_endpoint_inflight_requests",
    "In-flight concurrent requests per endpoint",
    ["endpoint"],
)

BACKFILL_QUEUE_SIZE = Gauge(
    "backfill_endpoint_queue_size",
    "Number of pending items in the output results queue",
    ["endpoint"],
)

BACKFILL_QUEUE_SIZES = Gauge(
    "backfill_endpoint_queue_sizes",
    "Number of pending items in various queues",
    ["endpoint", "queue_type"],
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

BACKFILL_PROCESSING_SECONDS = Summary(
    "backfill_endpoint_processing_seconds",
    "Duration of record processing operations per endpoint",
    ["endpoint", "operation_type"],
)

BACKFILL_DID_STATUS = Counter(
    "backfill_endpoint_did_status_total",
    "Count of DIDs by processing status (processed, deadlettered, etc.)",
    ["endpoint", "status"],
)
