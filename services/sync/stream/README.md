# Firehose sync service

Interacts with Bluesky firehose to get posts and exports them to persistent storage.

## Architecture Overview

The sync stream system implements a two-phase data export architecture:

1. **Real-time Cache Writes**: As records arrive from the Bluesky firehose, they are immediately written to a local JSON cache organized by operation type, record type, and user relationships.

2. **Batch Exports**: A separate cron job periodically reads from the cache and exports all accumulated data to permanent Parquet storage, then optionally clears the cache.

This separation allows the firehose stream to maintain high throughput (writing lightweight JSON files) while batch exports handle the heavier DataFrame processing and Parquet conversion asynchronously.

## Directory Structure

```
stream/
├── README.md                 # This file
├── app.py                    # Flask app entry point for firehose stream
├── main.py                   # Debug entry point (development only)
├── __init__.py               # Public API exports
│
├── core/                     # Foundation layer (types, protocols, setup)
│   ├── types.py              # Type definitions (Operation, RecordType, etc.)
│   ├── record_types.py       # Record type registry and mappings
│   ├── protocols.py          # Protocol definitions for dependency injection
│   ├── context.py            # Context objects (CacheWriteContext, BatchExportContext)
│   ├── constants.py          # Constants
│   └── setup.py              # Dependency injection and system setup
│
├── streaming/                # Firehose stream management layer
│   ├── firehose.py          # Firehose client and stream management
│   ├── cursor.py            # Cursor state management (S3/DynamoDB)
│   └── operations.py        # Operations callback orchestrator
│
├── cache_management/         # Cache directory and file management
│   ├── paths.py             # Path construction
│   ├── directory_lifecycle.py # Directory creation/deletion
│   └── files.py             # File I/O utilities
│
├── handlers/                 # Record handlers (cache write layer)
│   ├── generic.py           # Generic handler implementation
│   ├── registry.py          # Handler registry
│   ├── factories.py         # Handler factory functions
│   ├── config.py            # Handler configuration types
│   └── configs.py           # Handler configurations
│
├── record_processors/        # Record transformation and routing
│   ├── processors/          # Processor implementations
│   │   ├── post_processor.py
│   │   ├── like_processor.py
│   │   └── follow_processor.py
│   ├── transformation/      # Transformation logic
│   ├── registry.py          # Processor registry
│   ├── router.py            # Routing logic
│   ├── factories.py         # Processor factories
│   ├── protocol.py          # Processor protocol definition
│   └── types.py             # Processor types
│
├── exporters/               # Batch export logic
│   ├── batch_exporter.py    # Batch export orchestrator
│   ├── base.py              # Base exporter class
│   ├── study_user_exporter.py # Study user activity exporter
│   └── in_network_exporter.py # In-network user activity exporter
│
├── storage/                 # Storage abstraction layer
│   ├── repository.py        # Storage repository
│   └── adapters.py          # Storage adapter implementations
│
├── experiments/             # Experimental code and prototypes
│   ├── bloom_filter_experiments.py
│   └── README.md
│
└── tests/                   # Test suite
```

## System Components

### Core Layer (`core/`)

**Types & Protocols**: Foundational type definitions and protocol interfaces used throughout the system.

- `types.py`: Enums and type definitions (Operation, RecordType, HandlerKey, etc.)
- `record_types.py`: Record type registry mapping Bluesky NSIDs to internal types
- `protocols.py`: Protocol definitions for dependency injection (PathManagerProtocol, FileUtilitiesProtocol, etc.)
- `context.py`: Context objects that bundle dependencies (CacheWriteContext, BatchExportContext)
- `setup.py`: Dependency injection and system setup functions

### Streaming Layer (`streaming/`)

**Firehose Management**: All code related to connecting to and managing the Bluesky firehose stream.

- `firehose.py`: Firehose client that connects to Bluesky's firehose API
- `cursor.py`: Cursor state management for resuming streams (S3/DynamoDB)
- `operations.py`: Operations callback orchestrator that processes incoming records

### Cache Management Layer (`cache_management/`)

**File System Operations**: Manages the local JSON cache directory structure and file I/O.

- `CachePathManager`: Constructs all file paths for the cache directory structure
- `CacheDirectoryManager`: Manages directory lifecycle (creation and deletion)
- `FileUtilities`: Generic file I/O operations for JSON records

### Handler Layer (`handlers/`)

**Cache Write Layer**: Config-driven handlers that write records to the cache.

- `GenericRecordHandler`: Config-driven handler for all record types
- `HandlerConfig`: Configuration dataclass defining handler behavior
- `RecordHandlerRegistry`: Instance-based registry for handler lookup

### Record Processors (`record_processors/`)

**Transformation & Routing**: Transforms raw firehose records and determines routing decisions.

- Processors: Transform raw firehose data to domain models
- Transformation: Record transformation logic
- Router: Executes routing decisions via handlers

### Exporters (`exporters/`)

**Batch Export Logic**: Reads from cache and exports to persistent storage.

- `BatchExporter`: Orchestrates batch export of all cached data
- `StudyUserActivityExporter`: Exports study user activity data
- `InNetworkUserActivityExporter`: Exports in-network user activity data

### Storage Layer (`storage/`)

**Storage Abstraction**: Repository pattern for different storage backends.

- `StorageRepository`: Repository interface for storage operations
- `LocalStorageAdapter`: Local file system adapter implementation

## Usage

### Running the Firehose Stream

To run the firehose stream:

```python
python app.py
```

This will write posts to the local cache. Expect a 1:5 ratio between firehose events and new post writes.

### Cache Structure

We output to local storage in the following format:

```markdown
__local_cache__/
├── study_user_activity/
│   ├── create/
│   │   ├── post/
│   │   ├── like/
│   │   │   └── {post_uri_suffix}/  # Nested by liked post
│   │   ├── follow/
│   │   │   ├── follower/
│   │   │   └── followee/
│   │   ├── like_on_user_post/
│   │   │   └── {post_uri_suffix}/  # Nested by liked post
│   │   └── reply_to_user_post/
│   │       └── {parent_post_uri_suffix}/  # Nested by parent post
│   └── delete/
└── in_network_user_activity/
    └── create/
        └── post/
            └── {author_did}/
```

### Batch Export

The batch export system reads from the cache and exports data to persistent Parquet storage. Use the `export_batch()` function or `BatchExporter` class:

```python
from services.sync.stream.exporters.batch_exporter import export_batch

# Export with default settings (clears cache after export)
result = export_batch()

# Export with custom settings
result = export_batch(clear_filepaths=True, clear_cache=False)
```

## Expected Throughput

We expect to handle something on the order of ~1M posts/day.

## References

Inspired by:
- https://github.com/MarshalX/bluesky-feed-generator/tree/main/server
- https://github.com/bluesky-astronomy/astronomy-feeds/tree/main/server

Main Bluesky team feed overview: https://github.com/bluesky-social/feed-generator#overview
