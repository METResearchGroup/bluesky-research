# Migrate Research Data to S3

This module migrates research data from our Bluesky Nature paper (2024) to S3 for long-term archival storage. The data is approximately 1 year old as of November 2025, and we're archiving it to support pipeline refactoring and potential breaking API changes.

## Overview

The migration system uses a SQLite database to track migration progress, allowing for resumable operations, error tracking, and detailed status reporting. Files are uploaded to S3 using boto3's multipart upload capabilities for efficient handling of large files.

## Files

### Core Files

- **`constants.py`**: Configuration file containing:
  - `PREFIXES_TO_MIGRATE`: List of local directory prefixes to migrate
  - `DEFAULT_S3_ROOT_PREFIX`: S3 prefix where migrated files will be stored

- **`migration_tracker.py`**: Core tracking module providing:
  - `MigrationTracker` class: SQLite-based progress tracking
  - Status management (pending, in_progress, completed, failed, skipped)
  - Methods for querying files by status and prefix
  - Summary statistics and checklist generation

- **`initialize_migration_tracker_db.py`**: Initializes the migration tracking database:
  - Scans local directories for files to migrate
  - Generates S3 keys for each file
  - Registers all files in the SQLite tracking database with `pending` status
  - Uses tqdm for progress tracking during initialization

- **`run_migration.py`**: Executes the migration process:
  - Uploads files to S3 using boto3's `upload_file` (multipart upload support)
  - Updates tracker status as files are processed
  - Handles errors and retries
  - Supports migrating single prefix or all prefixes

- **`view_migration_tracker_db.py`**: View migration status:
  - Displays per-prefix status tables with counts for each status
  - Shows progress percentages per prefix
  - Prints overall migration summary checklist

### Supporting Files

- **`requirements.in` / `requirements.txt`**: Python dependencies
- **`tests/`**: Unit tests for the migration system

## How It Works

1. **Database Initialization**: The SQLite database (`migration_tracker.db`) tracks each file's:
   - Local file path
   - S3 destination key
   - File size
   - Migration status
   - Timestamps (started, completed)
   - Error messages (if failed)
   - Retry count

2. **Status Lifecycle**: Files progress through statuses:
   - `pending` → `in_progress` → `completed` or `failed`
   - Failed files can be retried (status resets to `pending`)

3. **S3 Upload**: Files are uploaded using boto3's `upload_file` method, which:
   - Automatically uses multipart uploads for large files (>8MB)
   - Handles retries on transient failures
   - Streams files (doesn't load entire file into memory)
   - Supports progress callbacks

4. **Prefix-Based Organization**: Files are organized by local directory prefixes, allowing:
   - Selective migration of specific data types
   - Per-prefix status tracking
   - Independent processing of different data categories

## Usage

### Step 1: Initialize the Migration Database

Scan local directories and register all files to be migrated:

```bash
python initialize_migration_tracker_db.py
```

This will:

- Walk through all directories listed in `PREFIXES_TO_MIGRATE`
- Generate S3 keys for each file
- Register files in the SQLite database with `pending` status
- Display progress bars for each prefix

**Note**: This step is idempotent - re-running will skip files already registered.

### Step 2: View Migration Status (Optional)

Check the current status of the migration:

```bash
python view_migration_tracker_db.py
```

This displays:

- Per-prefix status tables showing counts for pending, in_progress, completed, failed, and skipped
- Progress percentages for each prefix
- Overall migration summary

### Step 3: Run the Migration

Execute the migration process:

```bash
python run_migration.py
```

By default, this processes all prefixes sequentially. The script will:

- Query the database for pending and failed files
- Upload files to S3 using multipart uploads
- Update tracker status as files are processed
- Log progress and errors

**Resumable**: If the migration is interrupted, re-running will resume from where it left off (files in `in_progress` or `failed` status will be retried).

## Database Location

The SQLite database is stored at:

```text
scripts/migrate_research_data_to_s3/migration_tracker.db
```

## S3 Destination

Files are uploaded to:

```text
s3://bluesky-research/{DEFAULT_S3_ROOT_PREFIX}/{local_path}
```

Where `DEFAULT_S3_ROOT_PREFIX` is defined in `constants.py` (default: `bluesky_research/2024_nature_paper_study_data`).

## Error Handling

- Failed uploads are logged with error messages
- Files can be retried by re-running the migration script
- The tracker maintains error messages for debugging
- View failed files using `view_migration_tracker_db.py`

## Requirements

- Python 3.12+
- boto3 (AWS SDK)
- tqdm (progress bars)
- Access to AWS S3 with appropriate credentials configured
