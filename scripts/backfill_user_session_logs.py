"""Backfills user session logs to add feed IDs.

Migration plan:
1. Load the user session logs that don't have feed IDs (some of them have feed
IDs already since I've begun to roll this out).
2. Load in the previously generated feeds (load in the day's feeds and the feeds
from the day before that).
3. Create a map of users to feeds.
4. For each user, get their user session logs and their feeds and then
map their feeds to the user session logs.
"""

pass
