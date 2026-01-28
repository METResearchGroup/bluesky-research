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

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-19 --end-date 2024-10-25 --source-data-location s3 --sample-records --sample-proportion 0.10`

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


## Week 5: 2024-10-26 to 2024-11-01

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3 --sample-records --sample-proportion 0.10`

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


## Week 6: 2024-11-02 to 2024-11-08

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3 --sample-records --sample-proportion 0.10`

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 20000`

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


## Week 7: 2024-11-09 to 2024-11-15

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-09 --end-date 2024-11-15 --source-data-location s3 --sample-records --sample-proportion 0.10`

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


## Week 8: 2024-11-16 to 2024-11-22

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-16 --end-date 2024-11-22 --source-data-location s3 --sample-records --sample-proportion 0.10`

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


## Week 9: 2024-11-23 to 2024-11-29

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-23 --end-date 2024-11-29 --source-data-location s3 --sample-records --sample-proportion 0.10`

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


## Week 10: 2024-11-30 to 2024-12-01

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-30 --end-date 2024-12-01 --source-data-location s3 --sample-records --sample-proportion 0.10`

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
