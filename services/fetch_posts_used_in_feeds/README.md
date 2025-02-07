# Fetch posts used in feeds

The purpose of this service is to fetch and track the posts that are used in feeds. In practice, this will be triggered after feeds are generated and will write to a DB that will indicate which posts are used in the feeds.

As of 2025-02-02, the feeds are stored in a local directory and are not yet migrated to the new format. We need to migrate the feeds to the new format + location before we can start using this service (see "migrate_feeds_to_db.py").

Steps:

1. Migrate feeds to the new format + location (see "migrate_feeds_to_db.py")
2. Run this service to fetch the posts used in the feeds and write to the DB
3. Run the `backfill_records_coordination` pipeline to add the posts to the queue for ML inference.
