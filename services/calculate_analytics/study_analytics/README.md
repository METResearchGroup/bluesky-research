# Study Analytics

This service calculates the analytics for the current Bluesky study.

It runs the following steps:

1. Loads the feeds and user session logs from S3 into local storage (load_data/load_data.py)
2. Consolidates the daily feeds and daily user session logs into a single file (consolidate_data/consolidate_data.py).
3. Preprocesses the data. This consists of the following steps (in preprocess_data/preprocess_data.py   ):
    - a. Join the posts used in feeds (denoted by URI) with preprocessed posts as well as the individual integrations. This fully consolidates and hydrates the posts used in feeds.
    - b. Link consolidated posts to the feeds. Should export feeds where the rows, instead of having dicts with only the URI, have the full post data.
    - c. Link feeds and user session logs. Maps user session logs to which feed they would've seen (by feed ID). Also connects multiple user session logs if they're paginated requests.
4. Runs analysis.
    - One per Excel sheet:
        - condition_aggregated
        - user_behaviors
        - condition_raw_feed
