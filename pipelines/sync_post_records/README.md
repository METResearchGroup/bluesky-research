# Sync Post Records

This pipeline syncs post records from the Bluesky API to our local data directory. It has two key sources:

- Firehose: This is the primary source of data. It is a stream of post records from the Bluesky API.
- Most Liked Posts: This queries several feeds from the Bluesky API (e.g., "What's Hot") in order to get the "most popular" posts on Bluesky. This is a workaround for our end since other people have services that sync all the posts on Bluesky and they aggregate it into a single feed. Instead of us manually trying to figure out which posts have the most likes, we can just use those feeds instead.

The pipeline is run via `orchestration/sync_post_records_pipeline.py`.
