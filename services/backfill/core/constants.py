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
