# Jetstream Connector Service

## Overview

The Jetstream Connector Service provides a high-performance interface for subscribing to the Bluesky Firehose via the Jetstream service. It allows you to:

1. Connect to Bluesky's Jetstream service
2. Filter events by collection types and DIDs
3. Efficiently process and store firehose events in a queue
4. Resume processing from specific points in time

This service is designed for scalable, reliable ingestion of Bluesky social data for analytics, research, and application development.

## How It Works

The Jetstream connector uses WebSockets to establish a connection to one of Bluesky's public Jetstream instances. It streams events from the AT Protocol firehose in a simplified JSON format and stores them in a queue for further processing by other components.

### Key Components

- **JetstreamConnector**: Core class that manages the connection to Jetstream and processes events
- **JetstreamRecord**: Pydantic model for representing Jetstream events (commits, identity updates, account status changes)
- **Queue**: Storage mechanism for efficiently storing processed records

### Data Flow

1. The connector establishes a WebSocket connection to a Jetstream instance
2. Events are received and transformed into JetstreamRecord objects
3. Records are batched and stored in a queue
4. Other services can consume the queue to process the records

## Usage

### Command Line Interface

The service provides a command-line interface (`jetstream_cli.py`) for easy connection to the Jetstream service:

```bash
# Stream 1000 posts from the firehose
python -m services.sync.jetstream.jetstream_cli

# Stream likes and follows from a specific date
python -m services.sync.jetstream.jetstream_cli -c app.bsky.feed.like -c app.bsky.graph.follow -s 2024-01-01

# Stream posts from specific DIDs
python -m services.sync.jetstream.jetstream_cli -d did:plc:abcdef123456 -d did:plc:xyz789

# Stream posts to a custom queue with a specific batch size
python -m services.sync.jetstream.jetstream_cli --queue-name custom_queue --batch-size 50
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `-c, --wanted-collections` | Collection types to include (can be specified multiple times) |
| `-d, --wanted-dids` | Specific DIDs to include (can be specified multiple times) |
| `-s, --start-timestamp` | Start timestamp in YYYY-MM-DD format |
| `-n, --num-records` | Number of records to collect (default: 1000) |
| `-i, --instance` | Jetstream instance to connect to |
| `-t, --max-time` | Maximum time to run in seconds (default: 300) |
| `--queue-name` | Name for the queue (default: jetstream_sync) |
| `--batch-size` | Number of records to batch together for queue insertion (default: 100) |
| `--compress` | Use zstd compression for the stream |
| `-u, --bluesky-user-handles` | List of Bluesky user handles to get data for (comma-separated) |

### Programmatic API

You can also use the JetstreamConnector programmatically in your own code:

```python
from services.sync.jetstream.jetstream_connector import JetstreamConnector
import asyncio

# Initialize connector
connector = JetstreamConnector(queue_name="custom_queue", batch_size=100)

# Run the connector
async def run_connector():
    stats = await connector.listen_until_count(
        instance="jetstream2.us-east.bsky.network",
        wanted_collections=["app.bsky.feed.post", "app.bsky.feed.like"],
        target_count=1000,
        max_time=300,
        cursor=None,  # Optional cursor for resuming from a specific point
    )
    print(f"Processed {stats['records_stored']} records")

# Run the async function
asyncio.run(run_connector())
```

## Event Types

The service processes three main types of events from the Jetstream:

1. **Commit Events**: Create, update, or delete operations on records in repositories
2. **Identity Events**: Updates to a user's DID document or handle
3. **Account Events**: Changes to account status (active, deactivated, or taken down)

Each event includes a timestamp, DID, and type-specific data.

## Consuming the Queue

Other services can consume records from the queue for further processing:

```python
from lib.db.queue import Queue

# Connect to the queue
queue = Queue(queue_name="jetstream_sync", create_new_queue=False)

# Get items from the queue
items = queue.batch_remove_items_from_queue(limit=100)

# Process items
for item in items:
    # Each item is a QueueItem object with payload and metadata
    record_data = json.loads(item.payload)
    # Process the record...
```

## Public Jetstream Instances

The service can connect to any of Bluesky's public Jetstream instances:

- jetstream1.us-east.bsky.network
- jetstream2.us-east.bsky.network
- jetstream1.us-west.bsky.network
- jetstream2.us-west.bsky.network

## Code Structure

- `jetstream_connector.py`: Core connector implementation
- `models.py`: Pydantic model definitions
- `constants.py`: Constants used by the connector
- `jetstream_cli.py`: Command-line interface

## Dependencies

- websockets: For WebSocket connections to Jetstream
- pydantic: For data modeling and validation
- lib.db.queue: For queue storage

## Examples

### Collecting Posts for Analysis

```bash
# Collect 10,000 posts for analysis
python -m services.sync.jetstream.jetstream_cli -c app.bsky.feed.post -n 10000
```

### Monitoring User Activity

```bash
# Monitor specific user activity
python -m services.sync.jetstream.jetstream_cli -c app.bsky.feed.post -c app.bsky.feed.like -d did:plc:abcdef123456
```

### Resuming from a Specific Date

```bash
# Resume collection from January 1, 2024
python -m services.sync.jetstream.jetstream_cli -s 2024-01-01
``` 