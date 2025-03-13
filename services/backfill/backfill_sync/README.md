# Backfill Sync Module

This module provides functionality to backfill data from the Bluesky firehose via Jetstream, supporting filtering by DIDs and timestamps.

## Purpose

The Backfill Sync module allows you to:
1. Connect to the Bluesky firehose via Jetstream
2. Filter events by specific DIDs, collections, and timestamps
3. Process and store the data in a queue for further processing
4. Backfill historical data or sync from a specific point in time

## Usage

### Basic Usage

```python
from services.backfill.backfill_sync import run_backfill_sync

# Backfill data for specific DIDs
stats = run_backfill_sync(
    wanted_dids=["did:plc:abc123", "did:plc:def456"],
    wanted_collections=["app.bsky.feed.post"],
    num_records=10000
)

# Print the results
print(f"Processed {stats['records_stored']} records")
```

### With Timestamp

```python
from services.backfill.backfill_sync import run_backfill_sync

# Backfill data from a specific timestamp
stats = run_backfill_sync(
    start_timestamp="2024-01-01",  # or "2024-01-01-12:30:00" for more precision
    wanted_collections=["app.bsky.feed.post", "app.bsky.feed.like"],
    num_records=5000
)
```

## Configuration Options

The `run_backfill_sync` function accepts the following parameters:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| wanted_dids | list[str] | List of DIDs to filter for | None |
| start_timestamp | str | Start timestamp (YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS) | None |
| wanted_collections | list[str] | List of collection types to include | ["app.bsky.feed.post"] |
| num_records | int | Number of records to collect | 10000 |
| instance | str | Jetstream instance to connect to | First public instance |
| max_time | int | Maximum time to run in seconds | 900 (15 min) |
| queue_name | str | Name for the queue | "jetstream_sync" |
| batch_size | int | Number of records to batch for queue insertion | 100 |
| compress | bool | Use zstd compression for the stream | False |

## Valid Collections

The following collection types are supported:
- app.bsky.feed.post
- app.bsky.feed.like
- app.bsky.feed.repost
- app.bsky.graph.follow
- app.bsky.graph.block 