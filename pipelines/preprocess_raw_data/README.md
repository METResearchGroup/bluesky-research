# Preprocess raw data

This is a pipeline that will, on a cron schedule, take the posts from the latest syncs in `sync_post_records` (or, to be exact, any posts that haven't been preprocessed yet) and run them through preprocessing.

This will do the following:
1. Load in the raw data from the source (either from SQLite or MongoDB)
2. Consolidate the post formats. The posts from the firehose and the posts from the feed views have different formats, so we need a consolidated representation of a "post".
3. Run the posts through a series of filtering and preprocessing steps.
4. Save the posts to the database.

