"""Constants for user session log compaction and CloudWatch backfill."""

# S3 layout under the study bucket
USER_SESSION_LOGS_S3_ROOT_PREFIX = "user_session_logs/"
USER_SESSION_LOGS_S3_SEGMENT = "user_session_logs"

# Glue
USER_SESSION_LOGS_GLUE_CRAWLER_NAME = "user_session_logs_glue_crawler"

# Athena
USER_SESSION_LOGS_ATHENA_QUERY = "SELECT * FROM user_session_logs"

# Compaction
PARTITION_DATE_COLUMN = "partition_date"
USER_SESSION_LOGS_DEDUPE_COLUMNS = ["user_did", "timestamp", "cursor"]
COMPACTED_FILENAME_PREFIX = "compacted_"
COMPACTED_FILENAME_SUFFIX = ".jsonl"

# CloudWatch Logs Insights (backfill path)
CLOUDWATCH_LOG_GROUP_NAME = "ec2-feed-api-logs"
CLOUDWATCH_LOG_STREAM_NAME = "i-0917ffe080d3d91f1/bsky-logs"
CLOUDWATCH_INSIGHTS_QUERY_TEMPLATE = (
    'fields @timestamp, @message | filter @logStream = "{stream}" '
    "| filter @message like /User DID not in the study/"
)
BACKFILL_LOOKBACK_DAYS = 1
DID_PLC_REGEX = r"did:plc:[a-z0-9]+"
CLOUDWATCH_TIMESTAMP_PARSE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

# Backfill row defaults (match user_session_logs / Athena schema expectations)
BACKFILL_CURSOR = "backfill"
BACKFILL_LIMIT = 50
BACKFILL_FEED_LENGTH = 50
BACKFILL_FEED_LABEL = "default (backfill)"
BACKFILL_FILENAME_PREFIX = "backfill_user_session_logs_"
BACKFILL_FILENAME_SUFFIX = ".jsonl"
INTERNAL_BACKFILL_DATE_COLUMN = "date"
