# Backfill Sync Pipeline

This pipeline provides a command-line interface for backfilling data from the Bluesky firehose via Jetstream, with options for filtering by DIDs, collections, and timestamps.

## Overview

The Backfill Sync Pipeline allows you to:

1. Connect to the Bluesky firehose via Jetstream
2. Filter data based on DIDs, collection types, and timestamps
3. Store the data in a queue for further processing by other components
4. Configure various parameters like batch size, max run time, and more

This pipeline is ideal for:
- Backfilling historical data from a specific date
- Collecting data for specific users (DIDs)
- Targeting specific types of activities (posts, likes, follows, etc.)
- Setting up initial data loads for analytics projects

## Usage

### Command Line Interface

```bash
# Basic usage - backfill 10000 posts
python -m pipelines.backfill_sync.app

# Backfill 5000 likes and follows from a specific date
python -m pipelines.backfill_sync.app -c app.bsky.feed.like -c app.bsky.graph.follow -n 5000 -s 2024-01-01

# Backfill posts from specific DIDs
python -m pipelines.backfill_sync.app -d did:plc:abcdef123456,did:plc:xyz789

# Backfill with custom queue and batch size
python -m pipelines.backfill_sync.app --queue-name my_custom_queue --batch-size 50 --max-time 1800
```

### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| --wanted-collections | -c | Collection types to include | app.bsky.feed.post |
| --wanted-dids | -d | Comma-separated list of DIDs to filter for | None |
| --start-timestamp | -s | Start timestamp (YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS) | None |
| --num-records | -n | Number of records to collect | 10000 |
| --instance | -i | Jetstream instance to connect to | First public instance |
| --max-time | -t | Maximum time to run in seconds | 900 (15 min) |
| --queue-name |  | Name for the queue | jetstream_sync |
| --batch-size |  | Records to batch for queue insertion | 100 |
| --compress |  | Use zstd compression for the stream | False |
| --bluesky-user-handles | -u | Bluesky user handles (not yet implemented) | None |

## Processing Flow

1. **Connection**: The pipeline establishes a WebSocket connection to a Jetstream instance
2. **Filtering**: It applies filters for DIDs, collections, and timestamps
3. **Ingestion**: Records are received and processed from the firehose
4. **Storage**: Processed records are batched and stored in a queue
5. **Reporting**: When complete, the pipeline reports statistics about the ingestion

## Queue Integration

The records are stored in a queue (default name: "jetstream_sync") that other components can consume. 

To process the data:

```python
from lib.db.queue import Queue

# Connect to the queue populated by the backfill sync
queue = Queue(queue_name="jetstream_sync", create_new_queue=False)

# Process the queue items
items = queue.batch_remove_items_from_queue(limit=100)
for item in items:
    # Process each item
    print(item.payload)
```

## Examples

### Backfilling Posts for Research

```bash
# Collect 50,000 posts for research purposes, with a longer timeout
python -m pipelines.backfill_sync.app -c app.bsky.feed.post -n 50000 --max-time 3600
```

### Monitoring Specific Users

```bash
# Monitor specific DIDs for all supported activity types
python -m pipelines.backfill_sync.app -d did:plc:user1,did:plc:user2 -c app.bsky.feed.post -c app.bsky.feed.like -c app.bsky.feed.repost -c app.bsky.graph.follow
```

### Historical Data Analysis

```bash
# Collect data starting from January 1, 2024
python -m pipelines.backfill_sync.app -s 2024-01-01 -c app.bsky.feed.post -c app.bsky.feed.like -n 20000
```

## Related Modules

- `services.backfill.backfill_sync`: The underlying module that powers this pipeline
- `services.sync.jetstream`: The core Jetstream connector functionality
- `lib.db.queue`: Queue management for storing and retrieving records 