# Study Analytics

This service calculates the analytics for the current Bluesky study.

It runs the following steps:
1. Loads the feeds and user session logs from S3 into local storage (load_data/load_data.py)
2. Consolidates the daily feeds and daily user session logs into a single file (consolidate_data/consolidate_data.py).
3. ...
4. ...
