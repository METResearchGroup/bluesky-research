"""Loads feeds from S3.

Does it on a per-day basis, to avoid loading in too much data at once.
"""

# load all the partition dates
# for each partition date, load the feeds
# dump to parquet in (current dir)/feeds/(partition date).parquet

# NOTE: I actually wonder if I can do the entire data transformation in Athena
# instead of doing it locally??

