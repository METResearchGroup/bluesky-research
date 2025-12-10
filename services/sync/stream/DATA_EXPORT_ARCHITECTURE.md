# Data Export Architecture

## Overview

The sync stream system implements a two-phase data export architecture:

1. **Real-time Cache Writes**: As records arrive from the Bluesky firehose, they are immediately written to a local JSON cache organized by operation type, record type, and user relationships.

2. **Batch Exports**: A separate cron job periodically reads from the cache and exports all accumulated data to permanent Parquet storage, then optionally clears the cache.

This separation allows the firehose stream to maintain high throughput (writing lightweight JSON files) while batch exports handle the heavier DataFrame processing and Parquet conversion asynchronously.

## System Components

### 1. Cache Management Layer

#### `CachePathManager`
- **Purpose**: Constructs all file paths for the cache directory structure
- **Key Methods**:
  - `get_study_user_activity_path()`: Paths for study user records (posts, likes, follows)
  - `get_in_network_activity_path()`: Paths for in-network user posts (organized by author_did)
  - `get_local_cache_path()`: Generic firehose record paths (legacy, mostly unused)
- **Path Structure**: 
  ```
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

#### `CacheDirectoryManager`
- **Purpose**: Manages directory lifecycle (creation and deletion)
- **Key Methods**:
  - `ensure_directory_exists(path)`: Ensures directory exists, creating if necessary
  - `rebuild_all()`: Creates entire cache directory structure
  - `delete_all()`: Removes all cache directories

#### `FileUtilities`
- **Purpose**: Generic file I/O operations for JSON records
- **Operations**: 
  - Write: `write_json()`, `write_jsonl()` (creates directories as needed)
  - Read: `read_json()`, `read_all_json_in_directory()`
  - Directory: `list_files()`, `list_directories()`, `is_directory()`
  - Delete: `delete_files()` (removes specific files after export)

### 2. Handler Layer

#### `GenericRecordHandler`
- **Purpose**: Config-driven handler for all record types
- **Configuration**: Uses `HandlerConfig` to define behavior:
  - `path_strategy`: How to construct the base path
  - `nested_path_extractor`: Optional function to extract nested path components (e.g., post URI suffix)
  - `read_strategy`: How to read records from cache (default vs nested)
  - `requires_follow_status`: Whether follow_status parameter is required
- **Key Methods**:
  - `write_record()`: Writes a record to cache using configured path strategy
  - `read_records()`: Reads all records from cache using configured read strategy

#### `HandlerConfig`
- **Purpose**: Dataclass defining handler behavior
- **Type Safety**: Uses Protocol types for strategies:
  - `PathStrategyProtocol`: Path construction functions
  - `NestedPathExtractorProtocol`: Nested path extraction functions
  - `ReadStrategyProtocol`: Record reading functions

#### `RecordHandlerRegistry`
- **Purpose**: Instance-based registry for handler lookup
- **Keys**: Uses `HandlerKey` enum for type-safe registry keys
- **Usage**: Each system instance has its own registry (no global state)

### 3. Exporter Layer

#### `BaseActivityExporter`
- **Purpose**: Abstract base class with shared DataFrame export logic
- **Key Methods**:
  - `_export_dataframe()`: Converts list of dicts to DataFrame, adds metadata (synctimestamp, partition_date), exports via StorageRepository

#### `StudyUserActivityExporter`
- **Purpose**: Exports study user activity data (posts, likes, follows, etc.)
- **Process**:
  1. Iterates over operations (CREATE, DELETE)
  2. For each record type, uses handlers to read from cache
  3. Aggregates records by type
  4. Exports each type to appropriate service:
     - Posts, likes, like_on_user_post, reply_to_user_post → `study_user_activity`
     - Follows → `scraped_user_social_network`

#### `InNetworkUserActivityExporter`
- **Purpose**: Exports in-network user posts
- **Process**:
  1. Iterates over author_did directories
  2. Reads all JSON files for each author
  3. Exports to `in_network_user_activity` service

### 4. Storage Layer

#### `StorageRepository`
- **Purpose**: Abstract interface for storage operations
- **Implementation**: Uses adapter pattern to support different backends
- **Key Methods**:
  - `export_dataframe()`: Exports DataFrame to storage with service-specific metadata

#### `LocalStorageAdapter`
- **Purpose**: Concrete implementation for local Parquet storage
- **Process**:
  1. Converts DataFrame to Parquet
  2. Writes to service-specific directory structure
  3. Uses partition keys based on timestamp

### 5. Context & Setup

The system uses separate context objects for cache write and batch export phases to maintain clear separation of concerns.

#### `CacheWriteContext`
- **Purpose**: Context for real-time cache write operations during firehose processing
- **Components**:
  - `path_manager`: Constructs cache file paths
  - `directory_manager`: Manages directory lifecycle
  - `file_utilities`: Handles file I/O operations
  - `handler_registry`: Registry for record type handlers
  - `study_user_manager`: Manages study user identification
- **Usage**: Used by `data_filter.py` functions during firehose stream processing
- **Benefits**: Contains only what's needed for cache writes, no unnecessary dependencies

#### `BatchExportContext`
- **Purpose**: Context for batch export operations
- **Components**:
  - `path_manager`: Constructs cache file paths
  - `directory_manager`: Manages directory lifecycle and cleanup
  - `file_utilities`: Handles file I/O operations
  - `storage_repository`: Exports data to persistent storage
  - `study_user_exporter`: Exports study user activity data
  - `in_network_exporter`: Exports in-network user activity data
- **Usage**: Used by `BatchExporter` during batch export phase
- **Benefits**: Contains only what's needed for batch exports

#### `setup_cache_write_system()`
- **Purpose**: Factory function that wires dependencies for cache write phase
- **Process**:
  1. Creates shared infrastructure (path_manager, directory_manager, file_utilities)
  2. Creates study user manager
  3. Creates and registers all handlers
  4. Returns configured `CacheWriteContext`

#### `setup_batch_export_system()`
- **Purpose**: Factory function that wires dependencies for batch export phase
- **Process**:
  1. Creates shared infrastructure (path_manager, directory_manager, file_utilities)
  2. Creates storage adapter and repository
  3. Creates and registers handlers (needed for exporters)
  4. Creates exporters (study_user_exporter, in_network_exporter)
  5. Creates and returns configured `BatchExporter`

## Design Patterns

### Protocol Pattern
- **Usage**: Defines interfaces for path strategies, read strategies, and storage operations
- **Benefits**: Type safety without inheritance, supports duck typing
- **Examples**: `PathStrategyProtocol`, `ReadStrategyProtocol`, `StorageRepositoryProtocol`

### Strategy Pattern
- **Usage**: Configurable behavior via strategy functions
- **Examples**: 
  - Path strategies: `_default_path_strategy`, `_in_network_path_strategy`
  - Read strategies: `_nested_read_strategy`, `_follow_read_strategy`

### Repository Pattern
- **Usage**: Abstracts storage operations behind `StorageRepository` interface
- **Benefits**: Easy to swap storage backends (local, S3, etc.)

### Factory Pattern
- **Usage**: `create_handlers_for_all_types()` creates all handlers at once
- **Registry**: `HANDLER_CONFIG_REGISTRY` maps `HandlerKey` to `HandlerConfig`

### Registry Pattern
- **Usage**: `RecordHandlerRegistry` provides lookup for handlers by key
- **Benefits**: Decouples handler creation from usage, enables dynamic handler selection

## Runtime Flows

### Happy Path: Study User Post

1. **Firehose receives post** → `firehose.py` parses commit, extracts post record
2. **Operations callback** → `data_filter.operations_callback()` called with operations dict
3. **Post management** → `manage_post()` called with post dict and `CacheWriteContext`
4. **Study user check** → `StudyUserManager.is_study_user()` checks if author is study user
5. **Handler lookup** → `handler_registry.get_handler(RecordType.POST.value)` retrieves handler
6. **Path construction** → Handler calls `path_strategy()` to get base path:
   ```
   study_user_activity/create/post/
   ```
7. **File writing** → `FileUtilities.write_json()` writes JSON file:
   ```
   study_user_activity/create/post/author_did={did}_post_uri_suffix={suffix}.json
   ```
8. **Study user manager update** → `insert_study_user_post()` tracks post for future lookups

**Later: Batch Export**

9. **Batch export triggered** → `BatchExporter.export_batch()` called
10. **Exporter reads cache** → `StudyUserActivityExporter.export_activity_data()` iterates operations
11. **Handler reads records** → Handler's `read_strategy()` reads all JSON files from cache
12. **DataFrame conversion** → `BaseActivityExporter._export_dataframe()` converts to DataFrame
13. **Storage export** → `StorageRepository.export_dataframe()` writes Parquet file
14. **Cache cleanup** → Optionally deletes processed files and rebuilds cache structure

### Happy Path: Like on Study User Post

1. **Firehose receives like** → Like record extracted
2. **Like management** → `manage_like()` called
3. **Post lookup** → `StudyUserManager.is_study_user_post()` checks if liked post is by study user
4. **Handler lookup** → `handler_registry.get_handler(RecordType.LIKE_ON_USER_POST.value)`
5. **Path construction** → Handler uses nested path extractor:
   - Base path: `study_user_activity/create/like_on_user_post/`
   - Nested path: Extracts `post_uri_suffix` from like record
   - Full path: `study_user_activity/create/like_on_user_post/{post_uri_suffix}/`
6. **File writing** → Writes JSON file in nested directory
7. **Batch export** → Uses `_nested_read_strategy()` to read from nested directories

### Happy Path: In-Network Post

1. **Firehose receives post** → Post extracted
2. **Post management** → `manage_post()` called
3. **In-network check** → `StudyUserManager.is_in_network_user()` checks if author is in-network
4. **Handler lookup** → `handler_registry.get_handler(HandlerKey.IN_NETWORK_POST.value)`
5. **Path construction** → Uses `_in_network_path_strategy()`:
   - Requires `author_did` parameter
   - Path: `in_network_user_activity/create/post/{author_did}/`
6. **File writing** → Writes JSON file in author-specific directory
7. **Batch export** → `InNetworkUserActivityExporter` iterates author directories

### Happy Path: Follow Record

1. **Firehose receives follow** → Follow record extracted
2. **Follow management** → `manage_follow()` called
3. **Relationship check** → Checks if follower OR followee is study user
4. **Handler lookup** → `handler_registry.get_handler(RecordType.FOLLOW.value)`
5. **Path construction** → Uses `follow_status` parameter:
   - If user is follower: `study_user_activity/create/follow/follower/`
   - If user is followee: `study_user_activity/create/follow/followee/`
6. **File writing** → Writes JSON file in appropriate subdirectory
7. **Batch export** → Uses `_follow_read_strategy()` to read from both subdirectories

### Batch Export Flow

1. **Batch export initiated** → `export_batch()` or `BatchExporter.export_batch()` called
2. **Study user export** → `StudyUserActivityExporter.export_activity_data()`:
   - Iterates operations (CREATE, DELETE)
   - For each record type, gets handler from registry
   - Handler's `read_strategy()` collects all records
   - Aggregates by record type
   - Exports each type to appropriate service
3. **In-network export** → `InNetworkUserActivityExporter.export_activity_data()`:
   - Iterates author_did directories
   - Reads all JSON files
   - Exports to `in_network_user_activity` service
4. **File cleanup** → If `clear_filepaths=True`, deletes processed files
5. **Cache cleanup** → If `clear_cache=True`, deletes and rebuilds cache structure

## Type System

### Enums

- **`Operation`**: `CREATE`, `DELETE`
- **`RecordType`**: `POST`, `LIKE`, `FOLLOW`, `LIKE_ON_USER_POST`, `REPLY_TO_USER_POST`
- **`HandlerKey`**: Registry keys including `IN_NETWORK_POST`
- **`FollowStatus`**: `FOLLOWER`, `FOLLOWEE`

### Protocols

- **`PathManagerProtocol`**: Interface for path construction
- **`FileUtilitiesProtocol`**: File I/O interface (consolidates read/write/delete operations)
- **`StorageRepositoryProtocol`**: Storage abstraction
- **`RecordHandlerProtocol`**: Handler interface
- **`PathStrategyProtocol`**: Path construction strategy
- **`ReadStrategyProtocol`**: Record reading strategy

## Extension Points

### Adding a New Record Type

1. Add enum value to `RecordType` (if needed)
2. Add `HandlerKey` enum value
3. Create `HandlerConfig` in `configs.py`:
   - Define path strategy
   - Define nested path extractor (if needed)
   - Define read strategy (if custom)
4. Add config to `HANDLER_CONFIG_REGISTRY` in `factories.py`
5. Update exporter logic if needed (e.g., service mapping)

### Adding a New Storage Backend

1. Implement `StorageRepositoryProtocol` in new adapter class
2. Update `StorageRepository` to use new adapter
3. No changes needed to exporters (they use repository interface)

### Adding a New Path Strategy

1. Create function matching `PathStrategyProtocol` signature
2. Use in `HandlerConfig.path_strategy`
3. No changes needed to handler (it calls strategy function)

## Key Design Decisions

1. **Two-Phase Architecture**: Separates high-throughput writes (JSON cache) from heavy processing (Parquet export)
2. **Generic Handler Pattern**: Single handler class with configuration reduces code duplication
3. **Instance-Based Registry**: No global state, enables multiple independent systems
4. **Protocol-Based Types**: Type safety without inheritance overhead
5. **Context Object**: Explicit dependency injection replaces global state
6. **Strategy Functions**: Configurable behavior via functions rather than class hierarchies

