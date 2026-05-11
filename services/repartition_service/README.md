# Repartition Service

This service is used for repartitioning a service's data. For a given partition date, it moves the old records to a new location and reorganizes them based on a new partition key.

## Purpose

The service provides a way to:

- Safely migrate data from one partition structure to another
- Maintain a backup of the original data during migration
- Verify data integrity during the migration process
- Update the partition key for a service's data

## Usage

The service can be invoked with a payload containing:

```python
payload = {
    "start_date": "2024-01-01",  # Start date for processing (YYYY-MM-DD)
    "end_date": "2024-01-31",    # End date for processing (YYYY-MM-DD)
    "service": "service_name",    # MAP_SERVICE_TO_METADATA key for the target service
    "new_service_partition_key": "preprocessing_timestamp",  # Field used as parquet partition key metadata (default: "preprocessing_timestamp")
    "exclude_partition_dates": ["2024-01-15"],  # Optional dates to exclude
    "use_parallel": False,  # Optional: True to process dates via ProcessPoolExecutor (same per-date semantics)
}
```

**Runbook (operators):** [`docs/runbooks/services/repartition_service.md`](../../docs/runbooks/services/repartition_service.md)

## Process Flow

1. Load data for each partition date
2. Create backup copy under `old_{basename(local_prefix)}` (see `get_service_paths` in `helper.py`)
3. Verify data integrity of backup
4. Create temporary copy under `tmp_{basename(local_prefix)}`
5. Verify temporary copy integrity
6. Delete original data after verification
7. Repartition data using new partition key
8. Export repartitioned data
9. Report record counts for verification

## Data Paths

Paths are computed from [`get_service_paths`](helper.py):

- **Original**: `{local_prefix}/cache/partition_date=<date>` (`local_prefix` from `MAP_SERVICE_TO_METADATA`)
- **Backup**: `{root_local_data_directory}/old_{basename(local_prefix)}/cache/partition_date=<date>`
- **Temporary**: `{root_local_data_directory}/tmp_{basename(local_prefix)}/cache/partition_date=<date>`
