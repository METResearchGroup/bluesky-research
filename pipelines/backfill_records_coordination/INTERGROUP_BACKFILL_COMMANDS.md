# Commands to run for intergroup backfill

This runbook chunks the backfill into weekly windows. For each week:
- Enqueue posts for `ml_inference_intergroup` from S3 into the input queue.
- Run the intergroup integration to process the queue.
- Write the output queue (“cache buffer”) to permanent storage.

Notes:
- `--source-data-location s3` applies to the enqueue step (reading posts + label history).
- The “write cache to storage” step uses the existing `CacheBufferWriterService` behavior.


## Week 1: 2024-09-28 to 2024-10-04

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-09-28 --end-date 2024-10-04 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 2: 2024-10-05 to 2024-10-11

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-10-05 --end-date 2024-10-11 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 3: 2024-10-12 to 2024-10-18

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-10-12 --end-date 2024-10-18 --source-data-location s3`

Logs:
```plaintext
(base) ➜  bluesky-research git:(run-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-10-12 --end-date 2024-10-18 --source-data-location s3
2026-01-20 17:18:08,134 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-20 17:18:08,251 INFO [logger.py]: Not clearing any queues.
2026-01-20 17:18:08,273 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
2026-01-20 17:18:08,325 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-20 17:18:08,331 INFO [logger.py]: [Progress: 1/1] Enqueuing records for integration: ml_inference_intergroup
2026-01-20 17:18:08,332 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=[<StorageTier.CACHE: 'cache'>], n_days=7.
2026-01-20 17:18:08,657 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-12] Found n_files=2 parquet objects.
2026-01-20 17:18:08,716 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-13] Found n_files=2 parquet objects.
2026-01-20 17:18:08,756 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-14] Found n_files=2 parquet objects.
2026-01-20 17:18:08,799 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-15] Found n_files=2 parquet objects.
2026-01-20 17:18:08,839 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-16] Found n_files=2 parquet objects.
2026-01-20 17:18:08,879 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-17] Found n_files=2 parquet objects.
2026-01-20 17:18:08,918 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-18] Found n_files=2 parquet objects.
2026-01-20 17:18:08,918 INFO [logger.py]: Listed total_parquet_files=14 for dataset=preprocessed_posts.
2026-01-20 17:20:42,249 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1085511, 'columns': 3}, 'result_memory_usage_mb': np.float64(442.3930501937866)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-20 17:20:45,660 INFO [logger.py]: Listing S3 parquet URIs for dataset=ml_inference_intergroup, storage_tiers=[<StorageTier.CACHE: 'cache'>], n_days=7.
2026-01-20 17:20:46,361 INFO [logger.py]: Listed total_parquet_files=0 for dataset=ml_inference_intergroup.
2026-01-20 17:20:46,362 WARNING [logger.py]: 
                    filepaths must be provided when mode='parquet.
                    There are scenarios where data is missing (e.g., in the "active"
                    path, there might not be any up-to-date records). In these cases,
                    it's assumed that the filepaths are not provided.
                    
2026-01-20 17:20:46,364 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-20 17:20:46,364 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-20 17:20:46,451 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-20 17:20:48,010 INFO [logger.py]: Writing 1085511 items as 1086 minibatches to DB.
2026-01-20 17:20:48,010 INFO [logger.py]: Writing 1086 minibatches to DB as 44 batches...
2026-01-20 17:20:48,010 INFO [logger.py]: Processing batch 1/44...
2026-01-20 17:20:48,289 INFO [logger.py]: Processing batch 11/44...
2026-01-20 17:20:48,565 INFO [logger.py]: Processing batch 21/44...
2026-01-20 17:20:48,859 INFO [logger.py]: Processing batch 31/44...
2026-01-20 17:20:49,126 INFO [logger.py]: Processing batch 41/44...
2026-01-20 17:20:49,237 INFO [logger.py]: Inserted 1085511 posts into queue for integration: ml_inference_intergroup
2026-01-20 17:20:49,437 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-20 17:20:49,438 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 4: 2024-10-19 to 2024-10-25

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-10-19 --end-date 2024-10-25 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 5: 2024-10-26 to 2024-11-01

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 6: 2024-11-02 to 2024-11-08

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 7: 2024-11-09 to 2024-11-15

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-11-09 --end-date 2024-11-15 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 8: 2024-11-16 to 2024-11-22

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-11-16 --end-date 2024-11-22 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 9: 2024-11-23 to 2024-11-29

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-11-23 --end-date 2024-11-29 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

## Week 10: 2024-11-30 to 2024-12-01

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts --integrations g --start-date 2024-11-30 --end-date 2024-12-01 --source-data-location s3`
- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g`
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`


