# Backfill All Users Script

This script simplifies the process of backfilling data for all users in the system by:
1. Loading all users from the participant data service
2. Extracting their DIDs
3. Processing them in configurable chunks
4. Running the backfill sync process for each chunk
5. Logging all commands and results to a CSV file

## Purpose

The main purpose of this script is to efficiently backfill Bluesky firehose data for all known users in the study. Rather than manually specifying DIDs or running multiple commands, this script automates the process by:

- Breaking users into manageable chunks to prevent overwhelming the system
- Providing detailed logging in a CSV format for analysis
- Supporting resumable operations through the CSV record
- Offering a dry-run mode for validation before execution

## Usage

### Basic Usage

```bash
# Backfill posts for all users in the system with default settings
# (starts from September 28th, 2024, at 12am)
python -m pipelines.backfill_sync.backfill_all_users

# Backfill with a specific start date
python -m pipelines.backfill_sync.backfill_all_users --start-timestamp 2024-01-01

# Backfill multiple collection types
python -m pipelines.backfill_sync.backfill_all_users --collections app.bsky.feed.post,app.bsky.feed.like
```

### Advanced Usage

```bash
# Process users in larger chunks (500 DIDs per chunk)
python -m pipelines.backfill_sync.backfill_all_users --chunk-size 500

# Collect more records per chunk and increase timeout
python -m pipelines.backfill_sync.backfill_all_users --num-records 50000 --max-time 1800

# Dry run to validate configuration without processing
python -m pipelines.backfill_sync.backfill_all_users --dry-run

# Save CSV output to a specific directory
python -m pipelines.backfill_sync.backfill_all_users --output-dir /path/to/output
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--chunk-size` | Number of DIDs to process in each backfill run | 100 |
| `--start-timestamp` | Start timestamp (YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS) | 2024-09-28-00:00:00 |
| `--collections` | Comma-separated list of collections to include | app.bsky.feed.post |
| `--num-records` | Number of records to collect per backfill run | 10000 |
| `--max-time` | Maximum time to run each backfill in seconds | 900 |
| `--queue-name` | Name for the queue | jetstream_sync |
| `--batch-size` | Number of records to batch for queue insertion | 100 |
| `--compress` | Use zstd compression for the stream | False |
| `--output-dir` | Directory to save the output CSV file | Current directory |
| `--dry-run` | Don't actually run backfill, just print commands | False |

## Output

The script creates a CSV file named `backfill_sync_commands_TIMESTAMP.csv` with the following columns:

- `chunk_id`: Identifies each processed chunk
- `start_time`: When processing of the chunk started
- `end_time`: When processing of the chunk completed
- `duration_seconds`: How long the chunk took to process
- `did_count`: Number of DIDs in the chunk
- `dids`: Sample of DIDs in the chunk (up to 5)
- `collections`: Collection types being processed
- `start_timestamp`: Starting timestamp for backfill
- `num_records`: Target number of records
- `max_time`: Maximum run time per chunk
- `queue_name`: Queue used for storage
- `records_stored`: Number of records successfully stored
- `status`: SUCCESS, ERROR, or DRY_RUN with error details if applicable

## Examples

### Backfilling Recent Posts

```bash
# Backfill posts from the last month for all users
python -m pipelines.backfill_sync.backfill_all_users --start-timestamp 2024-05-01
```

### Full User Activity Backfill

```bash
# Backfill all types of user activity with larger chunks and longer timeouts
python -m pipelines.backfill_sync.backfill_all_users \
  --collections app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow \
  --chunk-size 200 \
  --num-records 20000 \
  --max-time 1800
```

### Testing Setup

```bash
# Validate configuration with a dry run
python -m pipelines.backfill_sync.backfill_all_users --dry-run

# Then run for real once everything looks good
python -m pipelines.backfill_sync.backfill_all_users
```

## Related Modules

- `services.participant_data.helper`: Provides user data access
- `services.backfill.backfill_sync`: Underlying sync functionality
- `pipelines.backfill_sync.app`: Base backfill sync application 