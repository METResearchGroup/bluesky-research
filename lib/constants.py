"""Constants for general use."""
from datetime import datetime, timezone

BLOCKED_AUTHORS = []  # likely that we will want to filter out some authors (e.g., spam accounts)
NUM_DAYS_POST_RECENCY = 3  # we only want recent posts (Bluesky docs recommend 3 days, see )
current_datetime = datetime.now(timezone.utc)
INAPPROPRIATE_TERMS = ["porn", "furry"]
