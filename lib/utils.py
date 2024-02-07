from datetime import datetime, timezone
from dateutil import parser


def parse_datetime_string(datetime_str: str) -> datetime:
    """Parses the different types of datetime strings in Bluesky (for some 
    reason, there are different datetime string formats used)."""
    return parser.parse(datetime_str).replace(tzinfo=timezone.utc)
