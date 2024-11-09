"""Processes previous feeds and:

1. Adds a 'feed_id' to each feed.
2. Dumps the 'feed' field as JSON-dumped files to S3 (instead of the complex
struct that it currently is).
    - Requires that the existing 'cached_custom_feeds' table is changed to
    have the 'feed' field be a string and not a complex struct.

This needs to be done on the "cached" feeds in order to prove that it works,
and then can be done on the "active" feeds once that's verified.
"""

pass
