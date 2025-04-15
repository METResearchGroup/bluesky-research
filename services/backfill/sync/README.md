# Bluesky Backfill Sync Service

## Overview

This service enables retrospective data collection from Bluesky social network users by accessing and processing their historical records. It retrieves user data through Bluesky's repository synchronization endpoints, validates and transforms the data, and exports it to a queue for further processing. The service is designed to efficiently handle batched operations with rate limiting to respect API constraints.

## Detailed Explanation

The backfill sync service operates through the following process:

### User Repository Access
- The service accesses user repositories by their DIDs (Decentralized Identifiers) using the `com.atproto.sync.getRepo` endpoint
- It resolves the user's PDS (Personal Data Server) endpoint by querying the PLC directory
- Data is retrieved in CAR format (Content-Addressable Storage) and parsed into individual records

### Data Filtering and Transformation
- Records are filtered by type (post, like, follow, reply, repost, block)
- Timestamps are validated against configurable time ranges (with defaults set for the study period)
- Special handling is provided for records outside the study period by assigning standardized timestamps (15th or 1st of the month)
- Records are enriched with metadata including record type and normalized timestamps

### Batch Processing
- Users can be processed in configurable batches to avoid memory issues
- Rate limiting is applied to respect Bluesky API constraints
- Garbage collection is performed between batches for resource optimization

### Export Pipeline
- Processed records are grouped by record type (post, like, follow, etc.)
- Data is exported to type-specific queues for further processing
- Metadata about processed records per user is tracked and returned

### Core Components:
1. **backfill.py**: Contains the main logic for fetching, validating, and transforming user records
2. **export_data.py**: Manages exporting transformed records to queue storage
3. **constants.py**: Defines key constants like timestamp formats, valid record types, and default parameters
4. **main.py**: Entry point with handler functions for executing the backfill process
5. **helper.py**: Utility functions for the backfill process

## Additional Components

### Determine DIDs to Backfill
- The `determine_dids_to_backfill.py` script is responsible for identifying which DIDs need to be backfilled. It analyzes existing data to determine the DIDs of posts that were reposted, responded to, or liked, and prepares a list of DIDs that require backfilling.

### Experimental Scripts
- **Serial vs Parallel Comparison**: The `experiment_compare_serial_parallel.py` script compares the performance of serial and parallel implementations of the backfill process. It measures execution time, memory usage, and CPU utilization to determine the most efficient approach.
- **Parallelization Experiments**: The `parallelization_experiments.py` script explores different parallelization strategies, such as threading and multiprocessing, to optimize the backfill process. It evaluates the I/O and compute-bound nature of the process and identifies the optimal number of workers for each strategy.

## Functions in Detail

The code is structured in a layered, composable way:

1. **Low-level Functions**: 
   - `get_plc_directory_doc()`: Resolves PDS endpoints from DIDs
   - `identify_post_type()` & `identify_record_type()`: Classify record types
   - `validate_record_timestamp()` & `validate_record_type()`: Filter valid records

2. **Mid-level Functions**:
   - `transform_backfilled_record()`: Enrich records with metadata
   - `get_bsky_records_for_user()`: Fetch and parse user repositories
   - `assign_default_backfill_synctimestamp()`: Normalize timestamps

3. **High-level Functions**:
   - `do_backfill_for_user()`: Process a single user's records
   - `do_backfill_for_users()`: Process multiple users with rate limiting
   - `run_batched_backfill()`: Orchestrate batch operations with resource management

## Usage

The service can be run either as a standalone script or imported and used as a module:

```python
# As a script
python -m services.backfill.sync.main --dids did1,did2,did3 --batch-size 50

# As a module
from services.backfill.sync.backfill import run_batched_backfill

dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3"]
result = run_batched_backfill(
    dids=dids,
    batch_size=50,
    start_timestamp="2024-09-27-00:00:00",
    end_timestamp="2024-12-02-00:00:00"
)
```

## Testing Details

Tests are located in the `tests/` directory.
