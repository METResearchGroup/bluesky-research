# Calculate Analytics Service

This service provides analytics tools for tracking record counts across different integrations and time periods. It helps monitor data volume changes, particularly useful for tracking changes before and after cache operations like DuckDB writes.

## Purpose

The service provides a way to:
- Track record counts over time for any integration
- Monitor data volume changes across partition dates
- Verify data completeness after operations
- Generate historical record count reports

## Usage

The service can be invoked via command line or with a payload containing:

```python
payload = {
    "integration": "ml_inference_perspective_api",  # Name of the integration to analyze
    "partition_date": "2024-10-15",                # End date for analysis (YYYY-MM-DD)
    "num_days_lookback": 7,                        # Optional: Number of days to look back (default: 7)
    "min_lookback_date": "2024-09-28"             # Optional: Minimum date to look back to
}
```

### Command Line Usage

```bash
python count_records_for_integration.py \
    --integration ml_inference_perspective_api \
    --partition_date 2024-10-15 \
    --num_days_lookback 7 \
    --min_lookback_date 2024-09-28
```

## Process Flow

1. Validate input parameters (integration name, dates)
2. Calculate date range using lookback window
3. For each date in range:
   - Load data from local storage
   - Count records
   - Handle any missing data or errors
4. Display results in chronological order

## Example Output

```
Number of records for the "ml_inference_perspective_api" integration for the range "2024-10-08" to "2024-10-15":

2024-10-08 1234
2024-10-09 1256
2024-10-10 1198
2024-10-11 1302
2024-10-12 1245
2024-10-13 1189
2024-10-14 1276
2024-10-15 1301
```

## Data Paths

The service reads data from the standard local storage paths for each integration:
`{root_local_data_directory}/{integration}/cache/partition_date=<date>`
