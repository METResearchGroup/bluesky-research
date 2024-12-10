# Snapshot data

This service creates a snapshot of the current state of the data. It is intended to be run as a batch job, taking the latest data from the active directory and saving it to the cache directory.

It is run with `orchestration/compaction_pipeline.py`.
