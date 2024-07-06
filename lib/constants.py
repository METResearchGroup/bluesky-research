"""Constants for general use."""
from datetime import datetime, timezone
import os

# likely that we will want to filter out some authors (e.g., spam accounts)
BLOCKED_AUTHORS = []
# we only want recent posts (Bluesky docs recommend 3 days, see )
NUM_DAYS_POST_RECENCY = 3
current_datetime = datetime.now(timezone.utc)
current_datetime_str = current_datetime.strftime("%Y-%m-%d-%H:%M:%S")


current_file_directory = os.path.dirname(os.path.abspath(__file__))
# level above git directory.
root_directory = os.path.abspath(os.path.join(current_file_directory, '../..'))
root_data_dirname = "bluesky_research_data"
root_local_data_directory = os.path.join(root_directory, root_data_dirname)
