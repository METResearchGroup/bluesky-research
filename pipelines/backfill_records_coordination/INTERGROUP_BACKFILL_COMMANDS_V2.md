# Commands to run for intergroup backfill (V2)

This runbook chunks the backfill into weekly windows. For each week:
- Enqueue posts for `ml_inference_intergroup` from S3 into the input queue.
- Run the intergroup integration to process the queue.
- Write the output queue ("cache buffer") to permanent storage.

**Sample proportion for this runbook: 0.10** (weeks that use `--sample-records`).

Notes:
- `--source-data-location s3` applies to the enqueue step (reading posts + label history).
- The "write cache to storage" step uses the existing `CacheBufferWriterService` behavior.

## Week 3: 2024-10-12 to 2024-10-18

We only need starting at 2024-10-16, as it looks like everything before that looks good.

### Pt. III: 2024-10-16 to 2024-10-18

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-16 --end-date 2024-10-18 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-28 15:13:44,768 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-28 15:13:44,776 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 15:13:44,810 INFO [logger.py]: Writing 10661 items as 11 minibatches to DB.
2026-01-28 15:13:44,810 INFO [logger.py]: Writing 11 minibatches to DB as 1 batches...
2026-01-28 15:13:44,810 INFO [logger.py]: Processing batch 1/1...
2026-01-28 15:13:44,826 INFO [logger.py]: Inserted 10661 posts into queue for integration: ml_inference_intergroup
2026-01-28 15:13:44,827 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-28 15:13:44,827 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

Iteration 1: I'll do 10,000 first.  I used a breakpoint to check the prompts and it looks good. Once these are done, I'll look at the ChatGPT console:

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000
2026-01-28 15:18:00,577 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 15:18:00,673 INFO [logger.py]: Not clearing any queues.
2026-01-28 15:18:00,673 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-28 15:18:00,673 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-28 15:18:07,383 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 15:18:07,387 INFO [logger.py]: Current queue size: 11 items
2026-01-28 15:18:07,430 INFO [logger.py]: Loaded 10661 posts to classify.
2026-01-28 15:18:07,430 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-28 15:18:07,430 INFO [logger.py]: Latest inference timestamp: None
2026-01-28 15:18:07,435 INFO [logger.py]: After dropping duplicates, 10661 posts remain.
2026-01-28 15:18:07,445 INFO [logger.py]: After filtering, 10513 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 27.90625 MB
2026-01-28 15:18:08,604 INFO [logger.py]: Limited posts from 10513 to 9858 (max_records_per_run=10000, included 10 complete batches)
2026-01-28 15:18:08,605 INFO [logger.py]: Classifying 9858 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/20 [00:00<?, ?batch/s]2026-01-28 15:18:09,666 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/20...
...
2026-01-28 15:35:17,660 INFO [logger.py]: Processing batch 1/1...
2026-01-28 15:35:17,662 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-28 15:35:17,663 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 20/20 [17:07<00:00, 51.40s/batch, successful=9858, failed=0]
Execution time for run_batch_classification: 17 minutes, 9 seconds
Memory usage for run_batch_classification: -28.578125 MB
Execution time for orchestrate_classification: 17 minutes, 13 seconds
Memory usage for orchestrate_classification: -7.84375 MB
Execution time for classify_latest_posts: 17 minutes, 15 seconds
Memory usage for classify_latest_posts: -7.859375 MB
2026-01-28 15:35:20,777 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-28 15:35:20,779 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Let's take a look:

```bash
You are a helpful assistant. Your job is to analyze a single social media post and answer a binary classification question ## Task Decide whether the post involves intergroup discussion. In social psychology, intergroup refers to interactions or situations that involve two or more groups that define themselves—or are defined by others—as distinct based on characteristics such as identity, beliefs, status, affiliation, or other boundaries. - If you judge that the post describes, reports, or implies intergroup discussion, respond with: "1" - If the post is unrelated, speaks only about individuals, is ambiguous, or describes within-group matters, respond with: "0" Only output your label. ONLY output 0 or 1. ## Examples Post: "Customers are upset because the management changed the return policy." Answer: 1 Post: "She was frustrated after missing her bus." Answer: 0 Post: "People in City A say City B always cheats during football tournaments." Answer: 1 Post: "Members of my hiking club disagreed on where to set up camp." Answer: 0 Post: "Why do older employees ignore what the younger staff suggest?" Answer: 1 Post: "A new bakery opened across from the old one." Answer: 0 Post: "Several men argued loudly outside the bar." Answer: 0 Post: An angel 🥹 Answer:

{
  "label": 0
}
```

```bash
You are a helpful assistant. Your job is to analyze a single social media post and answer a binary classification question ## Task Decide whether the post involves intergroup discussion. In social psychology, intergroup refers to interactions or situations that involve two or more groups that define themselves—or are defined by others—as distinct based on characteristics such as identity, beliefs, status, affiliation, or other boundaries. - If you judge that the post describes, reports, or implies intergroup discussion, respond with: "1" - If the post is unrelated, speaks only about individuals, is ambiguous, or describes within-group matters, respond with: "0" Only output your label. ONLY output 0 or 1. ## Examples Post: "Customers are upset because the management changed the return policy." Answer: 1 Post: "She was frustrated after missing her bus." Answer: 0 Post: "People in City A say City B always cheats during football tournaments." Answer: 1 Post: "Members of my hiking club disagreed on where to set up camp." Answer: 0 Post: "Why do older employees ignore what the younger staff suggest?" Answer: 1 Post: "A new bakery opened across from the old one." Answer: 0 Post: "Several men argued loudly outside the bar." Answer: 0 Post: Reuters's Suleiman al-Khalidi and Maya Gebeily: Israel has cleared mines and established new fortifications along line separating occupied Syrian Golan from demilitarized zone, potentially to attack Hizbullah from Golan and encircle the group from the east: www.reuters.com/world/middle... Answer:

{
  "label": 1
}
```

- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
2026-01-28 15:49:22,347 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-17] Exporting n=407 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 15:49:22,350 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 15:49:22,351 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-18] Exporting n=667 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 15:49:22,355 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 15:49:22,356 INFO [logger.py]: Finished exporting 9858 records to local storage for integration ml_inference_intergroup...
2026-01-28 15:49:22,356 INFO [logger.py]: Successfully wrote 9858 records to storage for integration ml_inference_intergroup
```

- [ ] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-28 15:49:37,050 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 15:49:37,114 INFO [logger.py]: Not clearing any queues.
2026-01-28 15:49:37,136 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-28 15:49:37,140 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-28 15:49:37,140 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-28 15:49:37,148 INFO [logger.py]: Registering 98 files for migration                                                           
2026-01-28 15:49:37,153 INFO [logger.py]: Registered 98 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (98 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 151.91it/s]
Finished initializing migration tracker db
2026-01-28 15:49:37,158 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-28 15:49:37,204 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/162cfcf7d3a44cfaa8661676ecee672f-0.parquet
2026-01-28 15:49:37,204 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/162cfcf7d3a44cfaa8661676ecee672f-0.parquet to S3 (0.05 MB)
2026-01-28 15:49:37,466 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/162cfcf7d3a44cfaa8661676ecee672f-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-17/162cfcf7d3a44cfaa8661676ecee672f-0.parquet
2026-01-28 15:49:37,468 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/162cfcf7d3a44cfaa8661676ecee672f-0.parquet
                                                                                                                                     2026-01-28 15:49:37,470 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/1400dacaa66f4806abef7fe7c554c63d-0.parquet
2026-01-28 15:49:37,471 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/1400dacaa66f4806abef7fe7c554c63d-0.parquet to S3 (0.55 MB)
2026-01-28 15:49:37,711 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/1400dacaa66f4806abef7fe7c554c63d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-16/1400dacaa66f4806abef7fe7c554c63d-0.parquet
2026-01-28 15:49:37,713 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/1400dacaa66f4806abef7fe7c554c63d-0.parquet
                                                                                                                                     2026-01-28 15:49:37,714 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/d01ad9b79b7448399bdfac55a503d289-0.parquet
2026-01-28 15:49:37,714 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/d01ad9b79b7448399bdfac55a503d289-0.parquet to S3 (0.08 MB)
2026-01-28 15:49:37,769 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/d01ad9b79b7448399bdfac55a503d289-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-18/d01ad9b79b7448399bdfac55a503d289-0.parquet
2026-01-28 15:49:37,770 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/d01ad9b79b7448399bdfac55a503d289-0.parquet
2026-01-28 15:49:37,771 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/aa876e4f1d4149b191ba63650cd1cbec-0.parquet
2026-01-28 15:49:37,771 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/aa876e4f1d4149b191ba63650cd1cbec-0.parquet to S3 (0.18 MB)
2026-01-28 15:49:37,849 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/aa876e4f1d4149b191ba63650cd1cbec-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-14/aa876e4f1d4149b191ba63650cd1cbec-0.parquet
2026-01-28 15:49:37,850 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/aa876e4f1d4149b191ba63650cd1cbec-0.parquet
                                                                                                                                     2026-01-28 15:49:37,852 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/5d253d19c583472b851f53d8d78d508e-0.parquet
2026-01-28 15:49:37,852 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/5d253d19c583472b851f53d8d78d508e-0.parquet to S3 (0.35 MB)
2026-01-28 15:49:37,977 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/5d253d19c583472b851f53d8d78d508e-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-15/5d253d19c583472b851f53d8d78d508e-0.parquet
2026-01-28 15:49:37,980 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/5d253d19c583472b851f53d8d78d508e-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 5/5 [00:00<00:00,  6.43it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00,  2.57it/s]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

OK GREAT. This looks more correct.
```bash
#	label	_col1
1	-1	2
2	1	28834
3	0	278905
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	903
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	27844
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	46752
16	2024-10-16	4502
17	2024-10-17	414
18	2024-10-18	671
```

I also checked the number with label = 1

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
  WHERE label = 1
) t
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-09-29	128
2	2024-09-30	408
3	2024-10-01	361
4	2024-10-02	190
5	2024-10-03	307
6	2024-10-04	111
7	2024-10-05	20
8	2024-10-06	40
9	2024-10-07	10
10	2024-10-10	386
11	2024-10-11	2024
12	2024-10-12	5947
13	2024-10-13	6769
14	2024-10-14	7375
15	2024-10-15	4219
16	2024-10-16	463
17	2024-10-17	38
18	2024-10-18	39
```

- [X] Inspect input and output queues

```sql
-- Check input queue
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"

-- Check output queue
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

- [X] Delete input and output queues

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

## Week 4: 2024-10-19 to 2024-10-25

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-19 --end-date 2024-10-25 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-28 16:30:37,323 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-28 16:30:37,323 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-28 16:30:37,352 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 16:30:37,459 INFO [logger.py]: Writing 48437 items as 49 minibatches to DB.
2026-01-28 16:30:37,459 INFO [logger.py]: Writing 49 minibatches to DB as 2 batches...
2026-01-28 16:30:37,459 INFO [logger.py]: Processing batch 1/2...
2026-01-28 16:30:37,504 INFO [logger.py]: Inserted 48437 posts into queue for integration: ml_inference_intergroup
2026-01-28 16:30:37,507 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-28 16:30:37,507 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000
2026-01-28 16:31:30,437 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 16:31:30,553 INFO [logger.py]: Not clearing any queues.
2026-01-28 16:31:30,554 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-28 16:31:30,554 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-28 16:31:37,571 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 16:31:37,576 INFO [logger.py]: Current queue size: 49 items
2026-01-28 16:31:37,756 INFO [logger.py]: Loaded 48437 posts to classify.
2026-01-28 16:31:37,757 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-28 16:31:37,757 INFO [logger.py]: Latest inference timestamp: None
2026-01-28 16:31:37,782 INFO [logger.py]: After dropping duplicates, 48437 posts remain.
2026-01-28 16:31:37,821 INFO [logger.py]: After filtering, 47674 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 109.421875 MB
2026-01-28 16:31:39,068 INFO [logger.py]: Classifying 47674 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/96 [00:00<?, ?batch/s]2026-01-28 16:31:40,150 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/96...

...

2026-01-28 17:15:53,751 INFO [logger.py]: Processing batch 1/1...
2026-01-28 17:15:53,752 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-28 17:15:53,753 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 96/96 [44:13<00:00, 27.64s/batch, successful=47674, failed=0]
Execution time for run_batch_classification: 44 minutes, 15 seconds
Memory usage for run_batch_classification: 88.03125 MB
Execution time for orchestrate_classification: 44 minutes, 19 seconds
Memory usage for orchestrate_classification: 150.453125 MB
Execution time for classify_latest_posts: 44 minutes, 21 seconds
Memory usage for classify_latest_posts: 150.4375 MB
2026-01-28 17:15:56,988 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-28 17:15:56,989 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-28 18:22:34,538 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 18:22:34,648 INFO [logger.py]: Not clearing any queues.
2026-01-28 18:22:34,649 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-28 18:22:34,649 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-28 18:22:34,658 INFO [logger.py]: Current queue size: 96 items
2026-01-28 18:22:34,890 INFO [logger.py]: Exporting 47674 records to local storage for integration ml_inference_intergroup...
2026-01-28 18:22:34,970 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-18] Exporting n=550 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,060 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,064 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-19] Exporting n=4118 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,072 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,077 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-20] Exporting n=7199 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,088 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,094 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-21] Exporting n=7149 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,105 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,111 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-22] Exporting n=7230 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,122 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,129 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-23] Exporting n=8391 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,142 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,148 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-24] Exporting n=7234 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,159 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,164 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-25] Exporting n=5803 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 18:22:35,174 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 18:22:35,175 INFO [logger.py]: Finished exporting 47674 records to local storage for integration ml_inference_intergroup...
2026-01-28 18:22:35,176 INFO [logger.py]: Successfully wrote 47674 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-28 18:25:11,394 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 18:25:11,491 INFO [logger.py]: Not clearing any queues.
2026-01-28 18:25:11,515 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-28 18:25:11,519 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-28 18:25:11,520 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-28 18:25:11,528 INFO [logger.py]: Registering 111 files for migration                                                          
2026-01-28 18:25:11,533 INFO [logger.py]: Registered 111 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (111 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 141.61it/s]
Finished initializing migration tracker db
2026-01-28 18:25:11,537 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-28 18:25:11,607 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/9964287ba9ed43a3b2f1a34129522fc9-0.parquet
2026-01-28 18:25:11,609 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/9964287ba9ed43a3b2f1a34129522fc9-0.parquet to S3 (0.83 MB)
2026-01-28 18:25:12,064 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/9964287ba9ed43a3b2f1a34129522fc9-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-21/9964287ba9ed43a3b2f1a34129522fc9-0.parquet
2026-01-28 18:25:12,066 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/9964287ba9ed43a3b2f1a34129522fc9-0.parquet
                                                                                                                                     2026-01-28 18:25:12,067 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/0c45544612b74e37bc3a2e58f1da4f68-0.parquet
2026-01-28 18:25:12,067 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/0c45544612b74e37bc3a2e58f1da4f68-0.parquet to S3 (0.47 MB)
2026-01-28 18:25:12,216 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/0c45544612b74e37bc3a2e58f1da4f68-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-19/0c45544612b74e37bc3a2e58f1da4f68-0.parquet
2026-01-28 18:25:12,219 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/0c45544612b74e37bc3a2e58f1da4f68-0.parquet
                                                                                                                                     2026-01-28 18:25:12,221 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c3cea6c41ed14df582f9db319b4a1f66-0.parquet
2026-01-28 18:25:12,221 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c3cea6c41ed14df582f9db319b4a1f66-0.parquet to S3 (0.07 MB)
2026-01-28 18:25:12,280 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c3cea6c41ed14df582f9db319b4a1f66-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c3cea6c41ed14df582f9db319b4a1f66-0.parquet
2026-01-28 18:25:12,284 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c3cea6c41ed14df582f9db319b4a1f66-0.parquet
2026-01-28 18:25:12,286 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/8fff4fd1850d4f0380ac1ff6c43d490a-0.parquet
2026-01-28 18:25:12,287 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/8fff4fd1850d4f0380ac1ff6c43d490a-0.parquet to S3 (0.81 MB)
2026-01-28 18:25:12,530 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/8fff4fd1850d4f0380ac1ff6c43d490a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-20/8fff4fd1850d4f0380ac1ff6c43d490a-0.parquet
2026-01-28 18:25:12,535 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/8fff4fd1850d4f0380ac1ff6c43d490a-0.parquet
                                                                                                                                     2026-01-28 18:25:12,539 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/134c1b0d25834b84b4f0bcdd5b4402d4-0.parquet
2026-01-28 18:25:12,540 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/134c1b0d25834b84b4f0bcdd5b4402d4-0.parquet to S3 (0.67 MB)
2026-01-28 18:25:12,714 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/134c1b0d25834b84b4f0bcdd5b4402d4-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-25/134c1b0d25834b84b4f0bcdd5b4402d4-0.parquet
2026-01-28 18:25:12,715 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/134c1b0d25834b84b4f0bcdd5b4402d4-0.parquet
                                                                                                                                     2026-01-28 18:25:12,716 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/314018413fb64a5798c6bf9394d66efe-0.parquet
2026-01-28 18:25:12,717 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/314018413fb64a5798c6bf9394d66efe-0.parquet to S3 (0.84 MB)
2026-01-28 18:25:12,925 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/314018413fb64a5798c6bf9394d66efe-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-22/314018413fb64a5798c6bf9394d66efe-0.parquet
2026-01-28 18:25:12,926 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/314018413fb64a5798c6bf9394d66efe-0.parquet
                                                                                                                                     2026-01-28 18:25:12,927 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/d6b4beb8b52c42c38582797d8946ae6f-0.parquet
2026-01-28 18:25:12,927 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/d6b4beb8b52c42c38582797d8946ae6f-0.parquet to S3 (0.97 MB)
2026-01-28 18:25:13,161 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/d6b4beb8b52c42c38582797d8946ae6f-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-23/d6b4beb8b52c42c38582797d8946ae6f-0.parquet
2026-01-28 18:25:13,163 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/d6b4beb8b52c42c38582797d8946ae6f-0.parquet
                                                                                                                                     2026-01-28 18:25:13,164 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/601d4125ec284ca296b6057857c3971b-0.parquet
2026-01-28 18:25:13,164 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/601d4125ec284ca296b6057857c3971b-0.parquet to S3 (0.84 MB)
2026-01-28 18:25:13,368 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/601d4125ec284ca296b6057857c3971b-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-24/601d4125ec284ca296b6057857c3971b-0.parquet
2026-01-28 18:25:13,370 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/601d4125ec284ca296b6057857c3971b-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 8/8 [00:01<00:00,  4.54it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:01<00:00,  1.13it/s]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

```bash
#	label	_col1
1	1	32814
2	-1	2
3	0	322590
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	903
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	27844
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	46752
16	2024-10-16	4502
17	2024-10-17	414
18	2024-10-18	1212
19	2024-10-19	4118
20	2024-10-20	7199
21	2024-10-21	7149
22	2024-10-22	7230
23	2024-10-23	8391
24	2024-10-24	7234
25	2024-10-25	5803
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
  WHERE label = 1
) t
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-09-29	128
2	2024-09-30	408
3	2024-10-01	361
4	2024-10-02	190
5	2024-10-03	307
6	2024-10-04	111
7	2024-10-05	20
8	2024-10-06	40
9	2024-10-07	10
10	2024-10-10	386
11	2024-10-11	2024
12	2024-10-12	5947
13	2024-10-13	6769
14	2024-10-14	7375
15	2024-10-15	4219
16	2024-10-16	463
17	2024-10-17	38
18	2024-10-18	76
19	2024-10-19	317
20	2024-10-20	530
21	2024-10-21	578
22	2024-10-22	603
23	2024-10-23	738
24	2024-10-24	614
25	2024-10-25	563
```

- [X] Inspect input and output queues

```sql
-- Check input queue
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"

-- Check output queue
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
0
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
96
```

- [X] Delete input and output queues

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

## Weeks 5, 6, 7: 2024-10-26 to 2024-11-15

- [X] Enqueueing:
  - [X] Week 5: `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-28 18:53:36,785 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-28 18:53:36,811 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 18:53:36,920 INFO [logger.py]: Writing 46263 items as 47 minibatches to DB.
2026-01-28 18:53:36,920 INFO [logger.py]: Writing 47 minibatches to DB as 2 batches...
2026-01-28 18:53:36,920 INFO [logger.py]: Processing batch 1/2...
2026-01-28 18:53:36,963 INFO [logger.py]: Inserted 46263 posts into queue for integration: ml_inference_intergroup
2026-01-28 18:53:36,966 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-28 18:53:36,967 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

  - [X] Week 6: `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-28 19:26:06,061 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-28 19:26:06,061 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-28 19:26:06,091 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-28 19:26:06,091 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 19:26:06,096 INFO [logger.py]: Current queue size: 47 items
2026-01-28 19:26:06,203 INFO [logger.py]: Writing 46880 items as 47 minibatches to DB.
2026-01-28 19:26:06,203 INFO [logger.py]: Writing 47 minibatches to DB as 2 batches...
2026-01-28 19:26:06,203 INFO [logger.py]: Processing batch 1/2...
2026-01-28 19:26:06,238 INFO [logger.py]: Inserted 46880 posts into queue for integration: ml_inference_intergroup
2026-01-28 19:26:06,243 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-28 19:26:06,243 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

  - [X] Week 7: `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-09 --end-date 2024-11-15 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-28 20:11:25,973 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 20:11:25,983 INFO [logger.py]: Current queue size: 94 items
2026-01-28 20:11:26,109 INFO [logger.py]: Writing 55493 items as 56 minibatches to DB.
2026-01-28 20:11:26,109 INFO [logger.py]: Writing 56 minibatches to DB as 3 batches...
2026-01-28 20:11:26,109 INFO [logger.py]: Processing batch 1/3...
2026-01-28 20:11:26,161 INFO [logger.py]: Inserted 55493 posts into queue for integration: ml_inference_intergroup
2026-01-28 20:11:26,166 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-28 20:11:26,166 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 170000`

We'll do it in 1 iteration.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 170000
2026-01-28 20:13:22,445 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 20:13:22,539 INFO [logger.py]: Not clearing any queues.
2026-01-28 20:13:22,540 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-28 20:13:22,540 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-28 20:13:30,400 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 20:13:30,405 INFO [logger.py]: Current queue size: 150 items
2026-01-28 20:13:30,873 INFO [logger.py]: Loaded 148636 posts to classify.
2026-01-28 20:13:30,874 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-28 20:13:30,874 INFO [logger.py]: Latest inference timestamp: None
2026-01-28 20:13:30,950 INFO [logger.py]: After dropping duplicates, 148516 posts remain.
2026-01-28 20:13:31,073 INFO [logger.py]: After filtering, 146278 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 329.53125 MB
2026-01-28 20:13:32,617 INFO [logger.py]: Classifying 146278 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/293 [00:00<?, ?batch/s]2026-01-28 20:13:33,698 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/293...

...

2026-01-28 22:04:36,861 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-28 22:04:36,862 INFO [logger.py]: Current queue size: 292 items
2026-01-28 22:04:36,863 INFO [logger.py]: Adding 278 posts to the output queue.
2026-01-28 22:04:36,864 INFO [logger.py]: Writing 278 items as 1 minibatches to DB.
2026-01-28 22:04:36,864 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-28 22:04:36,864 INFO [logger.py]: Processing batch 1/1...
2026-01-28 22:04:36,866 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-28 22:04:36,867 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████| 293/293 [1:51:03<00:00, 22.74s/batch, successful=146278, failed=0]
Execution time for run_batch_classification: 111 minutes, 4 seconds
Memory usage for run_batch_classification: -80.1875 MB
Execution time for orchestrate_classification: 111 minutes, 10 seconds
Memory usage for orchestrate_classification: 115.484375 MB
Execution time for classify_latest_posts: 111 minutes, 12 seconds
Memory usage for classify_latest_posts: 115.484375 MB
2026-01-28 22:04:40,129 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-28 22:04:40,132 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-29 05:06:07,642 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-29 05:06:07,749 INFO [logger.py]: Not clearing any queues.
2026-01-29 05:06:07,749 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-29 05:06:07,749 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-29 05:06:07,776 INFO [logger.py]: Current queue size: 293 items
2026-01-29 05:06:08,508 INFO [logger.py]: Exporting 146278 records to local storage for integration ml_inference_intergroup...
2026-01-29 05:06:08,735 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-25] Exporting n=1972 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,835 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,841 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-26] Exporting n=6271 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,851 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,856 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-27] Exporting n=6034 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,866 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,870 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-28] Exporting n=4789 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,878 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,884 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-29] Exporting n=6737 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,894 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,900 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-30] Exporting n=6336 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,910 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,916 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-31] Exporting n=7447 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,927 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,933 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-01] Exporting n=7093 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,944 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,949 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-02] Exporting n=5342 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,959 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,965 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-03] Exporting n=6209 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,974 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,980 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-04] Exporting n=6275 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:08,989 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:08,994 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-05] Exporting n=6652 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,005 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,011 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-06] Exporting n=8343 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,023 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,029 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-07] Exporting n=5761 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,038 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,046 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-08] Exporting n=8719 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,059 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,065 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-09] Exporting n=7117 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,076 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,082 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-10] Exporting n=7024 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,092 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,099 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-11] Exporting n=7983 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,110 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,117 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-12] Exporting n=7768 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,128 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,135 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-13] Exporting n=8837 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,148 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,155 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-14] Exporting n=7393 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,167 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,172 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-15] Exporting n=6176 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-29 05:06:09,182 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-29 05:06:09,187 INFO [logger.py]: Finished exporting 146278 records to local storage for integration ml_inference_intergroup...
2026-01-29 05:06:09,192 INFO [logger.py]: Successfully wrote 146278 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v2) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-29 05:06:47,073 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-29 05:06:47,135 INFO [logger.py]: Not clearing any queues.
2026-01-29 05:06:47,157 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-29 05:06:47,161 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-29 05:06:47,161 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-29 05:06:47,169 INFO [logger.py]: Registering 133 files for migration                                                          
2026-01-29 05:06:47,176 INFO [logger.py]: Registered 133 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (133 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 131.51it/s]
Finished initializing migration tracker db
2026-01-29 05:06:47,179 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-29 05:06:47,239 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/f341567e25d84030a8a9b727ed8e9313-0.parquet
2026-01-29 05:06:47,239 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/f341567e25d84030a8a9b727ed8e9313-0.parquet to S3 (0.70 MB)
2026-01-29 05:06:47,747 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/f341567e25d84030a8a9b727ed8e9313-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-26/f341567e25d84030a8a9b727ed8e9313-0.parquet
2026-01-29 05:06:47,749 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/f341567e25d84030a8a9b727ed8e9313-0.parquet
                                                                                                                                     2026-01-29 05:06:47,750 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/7cb04bd92ce54568b4e9be3f4c1d8c50-0.parquet
2026-01-29 05:06:47,750 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/7cb04bd92ce54568b4e9be3f4c1d8c50-0.parquet to S3 (0.56 MB)
2026-01-29 05:06:47,899 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/7cb04bd92ce54568b4e9be3f4c1d8c50-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-28/7cb04bd92ce54568b4e9be3f4c1d8c50-0.parquet
2026-01-29 05:06:47,901 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/7cb04bd92ce54568b4e9be3f4c1d8c50-0.parquet
                                                                                                                                     2026-01-29 05:06:47,902 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/e55748d3d84343329e4b57d1da9f40ba-0.parquet
2026-01-29 05:06:47,902 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/e55748d3d84343329e4b57d1da9f40ba-0.parquet to S3 (0.79 MB)
2026-01-29 05:06:48,109 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/e55748d3d84343329e4b57d1da9f40ba-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-29/e55748d3d84343329e4b57d1da9f40ba-0.parquet
2026-01-29 05:06:48,113 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/e55748d3d84343329e4b57d1da9f40ba-0.parquet
                                                                                                                                     2026-01-29 05:06:48,115 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/7380d045c9ad4c6e98c65633d1f0f8ba-0.parquet
2026-01-29 05:06:48,115 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/7380d045c9ad4c6e98c65633d1f0f8ba-0.parquet to S3 (0.68 MB)
2026-01-29 05:06:48,289 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/7380d045c9ad4c6e98c65633d1f0f8ba-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-27/7380d045c9ad4c6e98c65633d1f0f8ba-0.parquet
2026-01-29 05:06:48,291 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/7380d045c9ad4c6e98c65633d1f0f8ba-0.parquet
                                                                                                                                     2026-01-29 05:06:48,292 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/1d14e722b6cf45e79e15df6b8a5df341-0.parquet
2026-01-29 05:06:48,293 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/1d14e722b6cf45e79e15df6b8a5df341-0.parquet to S3 (0.92 MB)
2026-01-29 05:06:48,773 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/1d14e722b6cf45e79e15df6b8a5df341-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-11/1d14e722b6cf45e79e15df6b8a5df341-0.parquet
2026-01-29 05:06:48,775 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/1d14e722b6cf45e79e15df6b8a5df341-0.parquet
                                                                                                                                     2026-01-29 05:06:48,776 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/a6f50d68934c43ca83989e995005f97e-0.parquet
2026-01-29 05:06:48,776 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/a6f50d68934c43ca83989e995005f97e-0.parquet to S3 (0.80 MB)
2026-01-29 05:06:48,986 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/a6f50d68934c43ca83989e995005f97e-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-10/a6f50d68934c43ca83989e995005f97e-0.parquet
2026-01-29 05:06:48,989 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/a6f50d68934c43ca83989e995005f97e-0.parquet
                                                                                                                                     2026-01-29 05:06:48,990 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/3886384c02174d28afa2345ce129ccb7-0.parquet
2026-01-29 05:06:48,990 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/3886384c02174d28afa2345ce129ccb7-0.parquet to S3 (0.73 MB)
2026-01-29 05:06:49,172 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/3886384c02174d28afa2345ce129ccb7-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-04/3886384c02174d28afa2345ce129ccb7-0.parquet
2026-01-29 05:06:49,174 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/3886384c02174d28afa2345ce129ccb7-0.parquet
                                                                                                                                     2026-01-29 05:06:49,176 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/c427cdacb52a40c590caa1bd1e94ff57-0.parquet
2026-01-29 05:06:49,176 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/c427cdacb52a40c590caa1bd1e94ff57-0.parquet to S3 (0.71 MB)
2026-01-29 05:06:49,355 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/c427cdacb52a40c590caa1bd1e94ff57-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-03/c427cdacb52a40c590caa1bd1e94ff57-0.parquet
2026-01-29 05:06:49,358 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/c427cdacb52a40c590caa1bd1e94ff57-0.parquet
                                                                                                                                     2026-01-29 05:06:49,360 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/cb64d5c73f0b405da188ad4e27b693bf-0.parquet
2026-01-29 05:06:49,360 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/cb64d5c73f0b405da188ad4e27b693bf-0.parquet to S3 (0.61 MB)
2026-01-29 05:06:49,536 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/cb64d5c73f0b405da188ad4e27b693bf-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-02/cb64d5c73f0b405da188ad4e27b693bf-0.parquet
2026-01-29 05:06:49,537 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/cb64d5c73f0b405da188ad4e27b693bf-0.parquet
                                                                                                                                     2026-01-29 05:06:49,538 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/d28041a29265479b9742bfbed61f7c0d-0.parquet
2026-01-29 05:06:49,538 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/d28041a29265479b9742bfbed61f7c0d-0.parquet to S3 (0.75 MB)
2026-01-29 05:06:49,731 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/d28041a29265479b9742bfbed61f7c0d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-05/d28041a29265479b9742bfbed61f7c0d-0.parquet
2026-01-29 05:06:49,736 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/d28041a29265479b9742bfbed61f7c0d-0.parquet
                                                                                                                                     2026-01-29 05:06:49,740 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/a4c160ff2b3346c48106db85f42f44c9-0.parquet
2026-01-29 05:06:49,740 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/a4c160ff2b3346c48106db85f42f44c9-0.parquet to S3 (0.23 MB)
2026-01-29 05:06:49,853 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/a4c160ff2b3346c48106db85f42f44c9-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-25/a4c160ff2b3346c48106db85f42f44c9-0.parquet
2026-01-29 05:06:49,855 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/a4c160ff2b3346c48106db85f42f44c9-0.parquet
                                                                                                                                     2026-01-29 05:06:49,856 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/85aa840a167a403bb3e0be049063e48c-0.parquet
2026-01-29 05:06:49,857 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/85aa840a167a403bb3e0be049063e48c-0.parquet to S3 (0.87 MB)
2026-01-29 05:06:50,079 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/85aa840a167a403bb3e0be049063e48c-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-12/85aa840a167a403bb3e0be049063e48c-0.parquet
2026-01-29 05:06:50,084 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/85aa840a167a403bb3e0be049063e48c-0.parquet
                                                                                                                                     2026-01-29 05:06:50,087 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/94d32a2365a44f71a04be563f7271f53-0.parquet
2026-01-29 05:06:50,087 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/94d32a2365a44f71a04be563f7271f53-0.parquet to S3 (0.67 MB)
2026-01-29 05:06:50,301 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/94d32a2365a44f71a04be563f7271f53-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-15/94d32a2365a44f71a04be563f7271f53-0.parquet
2026-01-29 05:06:50,304 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/94d32a2365a44f71a04be563f7271f53-0.parquet
                                                                                                                                     2026-01-29 05:06:50,306 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/cdd4ddc298ee4b5aa265a8671b6a1d41-0.parquet
2026-01-29 05:06:50,306 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/cdd4ddc298ee4b5aa265a8671b6a1d41-0.parquet to S3 (0.81 MB)
2026-01-29 05:06:50,508 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/cdd4ddc298ee4b5aa265a8671b6a1d41-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-14/cdd4ddc298ee4b5aa265a8671b6a1d41-0.parquet
2026-01-29 05:06:50,511 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/cdd4ddc298ee4b5aa265a8671b6a1d41-0.parquet
                                                                                                                                     2026-01-29 05:06:50,513 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/b787b24af4914cd2847f1584155951f9-0.parquet
2026-01-29 05:06:50,513 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/b787b24af4914cd2847f1584155951f9-0.parquet to S3 (0.99 MB)
2026-01-29 05:06:50,781 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/b787b24af4914cd2847f1584155951f9-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-13/b787b24af4914cd2847f1584155951f9-0.parquet
2026-01-29 05:06:50,785 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/b787b24af4914cd2847f1584155951f9-0.parquet
                                                                                                                                     2026-01-29 05:06:50,788 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/653b63af499e485c86f64bda28b9c659-0.parquet
2026-01-29 05:06:50,788 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/653b63af499e485c86f64bda28b9c659-0.parquet to S3 (0.81 MB)
2026-01-29 05:06:50,993 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/653b63af499e485c86f64bda28b9c659-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-09/653b63af499e485c86f64bda28b9c659-0.parquet
2026-01-29 05:06:50,994 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/653b63af499e485c86f64bda28b9c659-0.parquet
                                                                                                                                     2026-01-29 05:06:50,995 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/84efaefddaa647dabab23eb9404085c6-0.parquet
2026-01-29 05:06:50,995 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/84efaefddaa647dabab23eb9404085c6-0.parquet to S3 (0.68 MB)
2026-01-29 05:06:51,203 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/84efaefddaa647dabab23eb9404085c6-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-07/84efaefddaa647dabab23eb9404085c6-0.parquet
2026-01-29 05:06:51,206 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/84efaefddaa647dabab23eb9404085c6-0.parquet
                                                                                                                                     2026-01-29 05:06:51,208 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/86038b57243b4753b7362838d3031eb7-0.parquet
2026-01-29 05:06:51,208 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/86038b57243b4753b7362838d3031eb7-0.parquet to S3 (0.95 MB)
2026-01-29 05:06:51,443 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/86038b57243b4753b7362838d3031eb7-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-06/86038b57243b4753b7362838d3031eb7-0.parquet
2026-01-29 05:06:51,445 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/86038b57243b4753b7362838d3031eb7-0.parquet
                                                                                                                                     2026-01-29 05:06:51,446 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/59416193d8414ab0aef68671c4664a1d-0.parquet
2026-01-29 05:06:51,446 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/59416193d8414ab0aef68671c4664a1d-0.parquet to S3 (0.81 MB)
2026-01-29 05:06:51,894 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/59416193d8414ab0aef68671c4664a1d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-01/59416193d8414ab0aef68671c4664a1d-0.parquet
2026-01-29 05:06:51,896 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/59416193d8414ab0aef68671c4664a1d-0.parquet
                                                                                                                                     2026-01-29 05:06:51,897 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/f9ed672654a74418b5e9a227ac0a6f68-0.parquet
2026-01-29 05:06:51,897 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/f9ed672654a74418b5e9a227ac0a6f68-0.parquet to S3 (1.01 MB)
2026-01-29 05:06:52,147 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/f9ed672654a74418b5e9a227ac0a6f68-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-08/f9ed672654a74418b5e9a227ac0a6f68-0.parquet
2026-01-29 05:06:52,152 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/f9ed672654a74418b5e9a227ac0a6f68-0.parquet
                                                                                                                                     2026-01-29 05:06:52,155 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/0451d6d041c3434d86241ef4cdf11a24-0.parquet
2026-01-29 05:06:52,156 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/0451d6d041c3434d86241ef4cdf11a24-0.parquet to S3 (0.74 MB)
2026-01-29 05:06:53,489 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/0451d6d041c3434d86241ef4cdf11a24-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-30/0451d6d041c3434d86241ef4cdf11a24-0.parquet
2026-01-29 05:06:53,491 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/0451d6d041c3434d86241ef4cdf11a24-0.parquet
                                                                                                                                     2026-01-29 05:06:53,492 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/f8568974952d445ba1e771b79e3260e8-0.parquet
2026-01-29 05:06:53,492 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/f8568974952d445ba1e771b79e3260e8-0.parquet to S3 (0.85 MB)
2026-01-29 05:06:53,720 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/f8568974952d445ba1e771b79e3260e8-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-31/f8568974952d445ba1e771b79e3260e8-0.parquet
2026-01-29 05:06:53,722 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/f8568974952d445ba1e771b79e3260e8-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████| 22/22 [00:06<00:00,  3.39it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:06<00:00,  3.24s/it]
```

- [ ] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

```bash
#	label	_col1
1	0	455760
2	-1	2
3	1	45867
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	903
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	27844
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	46752
16	2024-10-16	4502
17	2024-10-17	414
18	2024-10-18	1212
19	2024-10-19	4118
20	2024-10-20	7199
21	2024-10-21	7149
22	2024-10-22	7230
23	2024-10-23	8391
24	2024-10-24	7234
25	2024-10-25	7720
26	2024-10-26	6271
27	2024-10-27	6034
28	2024-10-28	4789
29	2024-10-29	6737
30	2024-10-30	6336
31	2024-10-31	7447
32	2024-11-01	7093
33	2024-11-02	5342
34	2024-11-03	6209
35	2024-11-04	6275
36	2024-11-05	6652
37	2024-11-06	8343
38	2024-11-07	5761
39	2024-11-08	8719
40	2024-11-09	7117
41	2024-11-10	7024
42	2024-11-11	7983
43	2024-11-12	7768
44	2024-11-13	8837
45	2024-11-14	7393
46	2024-11-15	6176
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
  WHERE label = 1
) t
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-09-29	128
2	2024-09-30	408
3	2024-10-01	361
4	2024-10-02	190
5	2024-10-03	307
6	2024-10-04	111
7	2024-10-05	20
8	2024-10-06	40
9	2024-10-07	10
10	2024-10-10	386
11	2024-10-11	2024
12	2024-10-12	5947
13	2024-10-13	6769
14	2024-10-14	7375
15	2024-10-15	4219
16	2024-10-16	463
17	2024-10-17	38
18	2024-10-18	76
19	2024-10-19	317
20	2024-10-20	530
21	2024-10-21	578
22	2024-10-22	603
23	2024-10-23	738
24	2024-10-24	614
25	2024-10-25	746
26	2024-10-26	494
27	2024-10-27	532
28	2024-10-28	504
29	2024-10-29	639
30	2024-10-30	607
31	2024-10-31	589
32	2024-11-01	556
33	2024-11-02	425
34	2024-11-03	482
35	2024-11-04	599
36	2024-11-05	551
37	2024-11-06	1068
38	2024-11-07	675
39	2024-11-08	892
40	2024-11-09	616
41	2024-11-10	578
42	2024-11-11	678
43	2024-11-12	626
44	2024-11-13	748
45	2024-11-14	532
46	2024-11-15	479
```

- [X] Inspect input and output queues

```sql
-- Check input queue
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"

-- Check output queue
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;" 
0
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
293
```

- [X] Delete input and output queues

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

## Weeks 8, 9, 10: 2024-11-16 to 2024-12-01

- [X] Enqueueing:
  - [X] Week 8: `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-16 --end-date 2024-11-22 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-29 05:34:52,084 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-29 05:34:52,109 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-29 05:34:52,211 INFO [logger.py]: Writing 41422 items as 42 minibatches to DB.
2026-01-29 05:34:52,211 INFO [logger.py]: Writing 42 minibatches to DB as 2 batches...
2026-01-29 05:34:52,211 INFO [logger.py]: Processing batch 1/2...
2026-01-29 05:34:52,251 INFO [logger.py]: Inserted 41422 posts into queue for integration: ml_inference_intergroup
2026-01-29 05:34:52,254 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-29 05:34:52,254 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

  - [X] Week 9: `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-23 --end-date 2024-11-29 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-29 05:50:18,282 INFO [logger.py]: Loaded 421273 base posts (scope=posts_used_in_feeds).
2026-01-29 05:50:18,296 INFO [logger.py]: Sampled base posts (proportion=0.1): 421273 -> 42127
2026-01-29 05:50:18,296 INFO [logger.py]: [Progress: 1/1] Enqueuing records for integration: ml_inference_intergroup
2026-01-29 05:50:18,296 INFO [logger.py]: Listing S3 parquet URIs for dataset=ml_inference_intergroup, storage_tiers=['cache'], n_days=7.
2026-01-29 05:50:18,660 INFO [logger.py]: Listed total_parquet_files=0 for dataset=ml_inference_intergroup.
2026-01-29 05:50:18,661 WARNING [logger.py]: 
                    filepaths must be provided when mode='parquet.
                    There are scenarios where data is missing (e.g., in the "active"
                    path, there might not be any up-to-date records). In these cases,
                    it's assumed that the filepaths are not provided.
                    
2026-01-29 05:50:18,668 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-29 05:50:18,668 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-29 05:50:18,695 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-29 05:50:18,695 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-29 05:50:18,701 INFO [logger.py]: Current queue size: 42 items
2026-01-29 05:50:18,794 INFO [logger.py]: Writing 42127 items as 43 minibatches to DB.
2026-01-29 05:50:18,794 INFO [logger.py]: Writing 43 minibatches to DB as 2 batches...
2026-01-29 05:50:18,794 INFO [logger.py]: Processing batch 1/2...
2026-01-29 05:50:18,830 INFO [logger.py]: Inserted 42127 posts into queue for integration: ml_inference_intergroup
2026-01-29 05:50:18,833 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-29 05:50:18,834 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
2026-01-29 05:50:19,375 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-29 05:50:19,474 INFO [logger.py]: Not clearing any queues.
```

  - [X] Week 10: `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-30 --end-date 2024-12-01 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash
2026-01-29 05:57:06,527 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-29 05:57:06,532 INFO [logger.py]: Current queue size: 85 items
2026-01-29 05:57:06,571 INFO [logger.py]: Writing 18225 items as 19 minibatches to DB.
2026-01-29 05:57:06,571 INFO [logger.py]: Writing 19 minibatches to DB as 1 batches...
2026-01-29 05:57:06,571 INFO [logger.py]: Processing batch 1/1...
2026-01-29 05:57:06,586 INFO [logger.py]: Inserted 18225 posts into queue for integration: ml_inference_intergroup
2026-01-29 05:57:06,587 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-29 05:57:06,588 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

- [ ] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

- [ ] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
  WHERE label = 1
) t
GROUP BY 1
ORDER BY 1 ASC
```

- [ ] Inspect input and output queues

```sql
-- Check input queue
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"

-- Check output queue
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

- [ ] Delete input and output queues

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```
