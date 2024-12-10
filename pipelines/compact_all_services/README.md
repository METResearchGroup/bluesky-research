# Compact All Services

Pipeline for compacting data files across multiple services into more efficient formats.

## Overview

This pipeline consolidates data files from various services into more compact formats to improve query performance and reduce storage costs. It handles both local storage and S3 data.

The main functionalities are:

1. Compacting local service data:
   - Loads data from local storage for each service
   - Exports to consolidated files by date
   - Optionally deletes old files after compaction
   - Handles special cases for certain services (e.g. preprocessed posts, ML inference)

2. Compacting S3 data:
   - Queries data from Athena
   - Deduplicates records based on primary keys where needed
   - Exports compacted data to S3 with timestamps
   - Deletes original files after successful compaction

## Services Handled

The pipeline compacts data for these services:

- Preprocessed posts
- User activity data (in-network, study users)
- Social network data
- User interactions (likes, replies)
- Most liked posts
- Daily superposters
- User session logs
- Feed analytics
- Post scores
- Consolidated post records
- ML inference results (Perspective API, sociopolitical)

## Usage

The pipeline can be triggered via Lambda or run locally. It's normally run via `orchestration/compaction_pipeline.py` which executes it on a cron schedule.
