# Pipelines

This directory contains the individual pipeline components that are run in production. The pipelines are orchestrated in `orchestration/`.

At a high level, the pipelines are:

- **Sync Pipeline**: syncs records from Bluesky. This is split into two pipelines:
    - **Firehose pipeline**: runs the Bluesky firehose stream and the associated writer servier in order to ingest the real-time data stream.
    - **Integrations pipeline**: syncs data from external sources. Currently queries the Bluesky API to get the most liked posts from the previous day.
- **Data Pipeline**: ingests the synced records, preprocesses them, runs the integrations, and then consolidates the integration results to get enriched records.
- **Recommendation Pipeline**: generates feeds for users.
- **Compaction Pipeline**: compacts the data and migrates records from the "active" to the "cache" directories.
- **Analytics Pipeline**: runs analytics on the data.
