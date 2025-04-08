"""Constants for the Jetstream connector module."""

# Valid collection types for the Bluesky firehose
VALID_COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
    # "app.bsky.graph.mute_actor",
    # "app.bsky.graph.mute_actors",
    # "app.bsky.graph.mute_thread",
    # "app.bsky.graph.unmute_actor",
    # "app.bsky.graph.unmute_actors",
    # "app.bsky.graph.unmute_thread"
]
