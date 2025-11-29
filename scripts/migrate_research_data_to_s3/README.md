# Migrate research data to S3

Our research data from our Bluesky Nature paper is, as of the time of writing (November 2025), 1 year old. We want to repurpose this pipeline to be more generalized, so to support refactoring and possibly breaking API changes, we're TTLing the study data to S3. This code will support that.

Steps:

1. Run `initialize_migration_tracker_db.py` to initialize the migration tracking database.

2. Run `main.py` to start the migration. By default, this does one prefix at a time (so we can make sure that it's working correctly).
