# Backfill

Generic backfill service. Will eventually be used as a catch-all for all backfill-related tasks.

Encompasses two types of backfill:

- Backfilling the PDS: this involves syncing with the PDS endpoints from Bluesky
and then getting the raw data for users. This is in the `core`, `storage`, and `scripts` logic.
- Backfilling existing records: this involves taking existing records that are missing
something (i.e., they aren't preprocessed, they are missing labels from integrations, etc.)
and then backfilling them. This is in the `posts` and `posts_used_in_feeds` logic.
