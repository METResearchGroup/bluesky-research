# Fetch posts used in feeds

The purpose of this service is to fetch and track the posts that are used in feeds. In practice, this will be triggered after feeds are generated and will write to a DB that will indicate which posts are used in the feeds.

As of 2025-02-02, the feeds are stored in a local directory and are not yet migrated to the new format. We need to migrate the feeds to the new format + location before we can start using this service (see "calculate_analytics/study_analytics/consolidate_data/migrate_feeds_to_db.py").
