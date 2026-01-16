import os

endpoint = "com.atproto.sync.getRepo"

# default backfill range to be during the study period.
default_start_timestamp = "2024-09-27-00:00:00"
default_end_timestamp = "2024-12-02-00:00:00"

valid_types = [
    "block",
    "follow",
    # "generator", # no reason to have it ig.
    "like",
    "post",
    # "profile", # no reason to have it ig.
    "reply",
    "repost",
]

valid_generic_bluesky_types = [
    "app.bsky.graph.block",
    "app.bsky.graph.follow",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.feed.post",  # both 'post' and 'reply' are of type 'app.bsky.feed.post'
]

service_name = "backfill_sync"
base_queue_name = "output_backfill_sync"

input_queue_name = "input_backfill_sync"

default_batch_size = 100

current_dir = os.path.dirname(os.path.abspath(__file__))

# default_write_batch_size = 100
# default_write_batch_size = 50
default_write_batch_size = 25
# default_write_batch_size = 10  # for experimental purposes.
default_pds_endpoint = "https://bsky.social"

# constants for rate limiting the APIs.

default_qps = 10  # assume 10 QPS max = 600/minute = 3000/5 minutes
# default_qps = 2  # 2 QPS max = 120/minute = 600/5 minutes
# default_qps = 1 # simplest implementation.

GLOBAL_RATE_LIMIT = (
    3000  # rate limit, I think, is 3000/5 minutes, we can put our own cap.
)
MANUAL_RATE_LIMIT = (
    0.9 * GLOBAL_RATE_LIMIT
)  # we can put conservatively to avoid max rate limit.
