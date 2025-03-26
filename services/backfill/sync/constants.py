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

service_name = "backfill_sync"
queue_name = "output_backfill_sync"

default_batch_size = 100
