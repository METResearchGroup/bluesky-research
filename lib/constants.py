"""Constants for general use."""
from datetime import datetime, timezone

# likely that we will want to filter out some authors (e.g., spam accounts)
BLOCKED_AUTHORS = []
# we only want recent posts (Bluesky docs recommend 3 days, see )
NUM_DAYS_POST_RECENCY = 3
current_datetime = datetime.now(timezone.utc)
current_datetime_str = current_datetime.strftime("%Y-%m-%d-%H:%M:%S")
