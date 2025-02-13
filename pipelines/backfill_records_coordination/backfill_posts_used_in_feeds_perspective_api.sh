#!/bin/bash

# Build python command with integration flag
PYTHON_CMD="/projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py \
    --record-type posts_used_in_feeds \
    --add-to-queue \
    --start-date $PARTITION_DATE \
    --end-date $PARTITION_DATE \
    --integration ml_inference_perspective_api" 