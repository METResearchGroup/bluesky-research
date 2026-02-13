# Commands to run for intergroup backfill (V3)

This runbook chunks the backfill into weekly windows. For each week:

- Enqueue posts for `ml_inference_intergroup` from S3 into the input queue.
- Run the intergroup integration to process the queue.
- Write the output queue ("cache buffer") to permanent storage.
- Migrate cache to S3, validate in Athena, then inspect/clear queues.

**Sample proportion for this runbook: 0.10** (weeks that use `--sample-records`).

Notes:

- `--source-data-location s3` applies to the enqueue step (reading posts + label history).
- The "write cache to storage" step uses the existing `CacheBufferWriterService` behavior.
- Run commands from repo root; use `uv run` if your environment uses it.
- Adjust `--max-records-per-run` as needed (e.g. 10000, 50000, or higher for large weeks).

---

## Week 1: 2024-09-28 to 2024-10-04

Previously completed this already.

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-09-28 --end-date 2024-10-04 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash

```

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

```bash

```

- [ ] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash

```

- [ ] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash

```

- [ ] **Validate in Athena**

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
ORDER BY 1 ASC;
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [ ] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash

```

- [ ] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 2: 2024-10-05 to 2024-10-11

- [X] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-05 --end-date 2024-10-11 --source-data-location s3`

```bash
2026-02-10 14:18:33,569 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 14:18:33,576 INFO [logger.py]: Writing 581 items as 1 minibatches to DB.
2026-02-10 14:18:33,576 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-10 14:18:33,576 INFO [logger.py]: Processing batch 1/1...
2026-02-10 14:18:33,578 INFO [logger.py]: Inserted 581 posts into queue for integration: ml_inference_intergroup
2026-02-10 14:18:33,578 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-02-10 14:18:33,578 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

I want to make sure that this does indeed filter out previously labeled posts.

- NOTE: I added logging to confirm this, see [this PR](https://github.com/METResearchGroup/bluesky-research/pull/382).

```bash
(base) ➜  bluesky-research git:(add-logging-to-filtered-posts) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 2000                                                                            
2026-02-10 15:13:13,600 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 15:13:13,703 INFO [logger.py]: Not clearing any queues.
2026-02-10 15:13:13,703 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-10 15:13:13,703 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-10 15:13:21,192 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 15:13:21,195 INFO [logger.py]: Current queue size: 1 items
2026-02-10 15:13:21,199 INFO [logger.py]: Loaded 581 posts to classify.
2026-02-10 15:13:21,200 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-10 15:13:21,200 INFO [logger.py]: Latest inference timestamp: None
2026-02-10 15:13:21,205 INFO [logger.py]: After dropping duplicates, 581 posts remain.
2026-02-10 15:13:21,211 INFO [logger.py]: After filtering, 351 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 2.84375 MB
2026-02-10 15:13:22,254 INFO [logger.py]: Classifying 351 posts with intergroup classifier...
Classifying batches:   0%|                                                                                   | 0/1 [00:00<?, ?batch/s]2026-02-10 15:13:23,346 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/1...
2026-02-10 15:13:45,673 INFO [logger.py]: Successfully labeled 351 posts.
2026-02-10 15:13:45,674 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-10 15:13:45,674 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 15:13:45,675 INFO [logger.py]: Current queue size: 1 items
2026-02-10 15:13:45,675 INFO [logger.py]: Creating new SQLite DB for queue output_ml_inference_intergroup...
2026-02-10 15:13:45,677 INFO [logger.py]: Adding 351 posts to the output queue.
2026-02-10 15:13:45,678 INFO [logger.py]: Writing 351 items as 1 minibatches to DB.
2026-02-10 15:13:45,678 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-10 15:13:45,678 INFO [logger.py]: Processing batch 1/1...
2026-02-10 15:13:45,680 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-10 15:13:45,681 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████████| 1/1 [00:22<00:00, 22.34s/batch, successful=351, failed=0]
Execution time for run_batch_classification: 0 minutes, 23 seconds
Memory usage for run_batch_classification: 37.546875 MB
Execution time for orchestrate_classification: 0 minutes, 28 seconds
Memory usage for orchestrate_classification: 40.40625 MB
Execution time for classify_latest_posts: 0 minutes, 30 seconds
Memory usage for classify_latest_posts: 40.40625 MB
2026-02-10 15:13:48,808 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-10 15:13:48,809 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

I suppose in this range, I had already classified most posts? I thought I classified a lot already, but I guess it ended up being most of it?

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(add-logging-to-filtered-posts) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-10 15:16:57,556 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 15:16:57,684 INFO [logger.py]: Not clearing any queues.
2026-02-10 15:16:57,685 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-10 15:16:57,685 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-10 15:16:57,686 INFO [logger.py]: Current queue size: 1 items
2026-02-10 15:16:57,690 INFO [logger.py]: Exporting 351 records to local storage for integration ml_inference_intergroup...
2026-02-10 15:16:57,706 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-04] Exporting n=351 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-10 15:16:57,966 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-10 15:16:57,967 INFO [logger.py]: Finished exporting 351 records to local storage for integration ml_inference_intergroup...
```

- [ ] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash

```

- [ ] **Validate in Athena**

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
1	0	547207
2	-1	2
3	1	54206
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC;
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	915
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
45	2024-11-14	7622
46	2024-11-15	10848
47	2024-11-16	7673
48	2024-11-17	4113
49	2024-11-18	1152
50	2024-11-19	8199
51	2024-11-20	431
52	2024-11-21	6851
53	2024-11-22	10422
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
0
(.venv) (base) ➜  queue sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
1
```

- [X] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 3: 2024-10-12 to 2024-10-18

- [X] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-12 --end-date 2024-10-18 --source-data-location s3`

```bash
2026-02-10 15:45:41,240 INFO [logger.py]: Loaded 482951 post URI rows from S3 for service ml_inference_intergroup (257650 unique URIs).
2026-02-10 15:45:41,287 INFO [logger.py]: Enqueuing 80299 posts for integration ml_inference_intergroup (after dedup and filtering previously labeled).
2026-02-10 15:45:41,287 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 15:45:41,405 INFO [logger.py]: Writing 80299 items as 81 minibatches to DB.
2026-02-10 15:45:41,405 INFO [logger.py]: Writing 81 minibatches to DB as 4 batches...
2026-02-10 15:45:41,405 INFO [logger.py]: Processing batch 1/4...
2026-02-10 15:45:41,496 INFO [logger.py]: Inserted 80299 posts into queue for integration: ml_inference_intergroup
2026-02-10 15:45:41,501 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-02-10 15:45:41,501 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

I'm starting with a smaller run first, just to see how it does.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000
2026-02-10 15:48:36,258 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 15:48:36,367 INFO [logger.py]: Not clearing any queues.
2026-02-10 15:48:36,368 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-10 15:48:36,368 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-10 15:48:43,480 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 15:48:43,492 INFO [logger.py]: Current queue size: 81 items
2026-02-10 15:48:43,760 INFO [logger.py]: Loaded 80299 posts to classify.
2026-02-10 15:48:43,761 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-10 15:48:43,761 INFO [logger.py]: Latest inference timestamp: None
2026-02-10 15:48:43,802 INFO [logger.py]: After dropping duplicates, 80299 posts remain.
2026-02-10 15:48:43,866 INFO [logger.py]: After filtering, 75324 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 165.5625 MB
2026-02-10 15:48:45,279 INFO [logger.py]: Limited posts from 75324 to 9825 (max_records_per_run=10000, included 10 complete batches)
2026-02-10 15:48:45,299 INFO [logger.py]: Classifying 9825 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/20 [00:00<?, ?batch/s]2026-02-10 15:48:46,365 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/20...

...

2026-02-10 15:57:24,028 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-10 15:57:24,029 INFO [logger.py]: Current queue size: 19 items
2026-02-10 15:57:24,030 INFO [logger.py]: Adding 325 posts to the output queue.
2026-02-10 15:57:24,031 INFO [logger.py]: Writing 325 items as 1 minibatches to DB.
2026-02-10 15:57:24,031 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-10 15:57:24,031 INFO [logger.py]: Processing batch 1/1...
2026-02-10 15:57:24,033 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-10 15:57:24,034 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 20/20 [08:37<00:00, 25.88s/batch, successful=9825, failed=0]
Execution time for run_batch_classification: 8 minutes, 39 seconds
Memory usage for run_batch_classification: -81.46875 MB
Execution time for orchestrate_classification: 8 minutes, 44 seconds
Memory usage for orchestrate_classification: -24.71875 MB
Execution time for classify_latest_posts: 8 minutes, 46 seconds
Memory usage for classify_latest_posts: -24.703125 MB
2026-02-10 15:57:27,143 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-10 15:57:27,145 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

OK, looks to work well enough. I can let it run on the remaining set then.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 80000
2026-02-10 15:59:30,095 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 15:59:30,197 INFO [logger.py]: Not clearing any queues.
2026-02-10 15:59:30,197 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-10 15:59:30,197 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-10 15:59:37,198 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 15:59:37,223 INFO [logger.py]: Current queue size: 71 items
2026-02-10 15:59:37,506 INFO [logger.py]: Loaded 70299 posts to classify.
2026-02-10 15:59:37,507 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-10 15:59:37,507 INFO [logger.py]: Latest inference timestamp: None
2026-02-10 15:59:37,542 INFO [logger.py]: After dropping duplicates, 70299 posts remain.
2026-02-10 15:59:37,598 INFO [logger.py]: After filtering, 65499 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 147.21875 MB
2026-02-10 15:59:38,868 INFO [logger.py]: Classifying 65499 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/131 [00:00<?, ?batch/s]2026-02-10 15:59:39,928 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/131...

...

Classifying batches: 100%|███████████████████████████████████████████| 131/131 [50:15<00:00, 23.02s/batch, successful=65499, failed=0]
Execution time for run_batch_classification: 50 minutes, 17 seconds
Memory usage for run_batch_classification: -57.3125 MB
Execution time for orchestrate_classification: 50 minutes, 22 seconds
Memory usage for orchestrate_classification: 34.09375 MB
Execution time for classify_latest_posts: 50 minutes, 24 seconds
Memory usage for classify_latest_posts: 34.09375 MB
2026-02-10 16:49:58,855 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-10 16:49:58,858 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-10 18:34:52,220 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 18:34:52,337 INFO [logger.py]: Not clearing any queues.
2026-02-10 18:34:52,338 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-10 18:34:52,338 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-10 18:34:52,352 INFO [logger.py]: Current queue size: 151 items
2026-02-10 18:34:52,752 INFO [logger.py]: Exporting 75324 records to local storage for integration ml_inference_intergroup...
2026-02-10 18:34:52,881 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-11] Exporting n=21896 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-10 18:34:52,946 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-10 18:34:52,948 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-15] Exporting n=322 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-10 18:34:52,951 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-10 18:34:52,980 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-16] Exporting n=43039 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-10 18:34:53,030 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-10 18:34:53,035 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-17] Exporting n=3746 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-10 18:34:53,043 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-10 18:34:53,048 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-18] Exporting n=6321 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-10 18:34:53,059 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-10 18:34:53,061 INFO [logger.py]: Finished exporting 75324 records to local storage for integration ml_inference_intergroup...
2026-02-10 18:34:53,064 INFO [logger.py]: Successfully wrote 75324 records to storage for integration ml_inference_intergroup
```

- [X] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-02-10 18:35:27,269 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 18:35:27,331 INFO [logger.py]: Not clearing any queues.
2026-02-10 18:35:27,388 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-02-10 18:35:27,391 INFO [logger.py]: Registered 0 new files for migration (0 already in tracker).                                
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-02-10 18:35:27,406 INFO [logger.py]: Registered 5 new files for migration (152 already in tracker).                               
Initialized migration tracker db for ml_inference_intergroup/cache (157 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 132.36it/s]
Finished initializing migration tracker db
2026-02-10 18:35:27,409 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]2026-02-10 18:35:27,459 INFO [logger.py]: Migrating 0 file(s) for ml_inference_intergroup/active
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]
2026-02-10 18:35:27,459 INFO [logger.py]: Migrating 5 file(s) for ml_inference_intergroup/cache
                                                                                                                                     2026-02-10 18:35:27,460 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/f2eeb871fcf34b78b696ea11f86071c7-0.parquet
2026-02-10 18:35:27,460 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/f2eeb871fcf34b78b696ea11f86071c7-0.parquet to S3 (0.39 MB)
2026-02-10 18:35:27,994 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/f2eeb871fcf34b78b696ea11f86071c7-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-17/f2eeb871fcf34b78b696ea11f86071c7-0.parquet
2026-02-10 18:35:27,999 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/f2eeb871fcf34b78b696ea11f86071c7-0.parquet
                                                                                                                                     2026-02-10 18:35:28,008 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/e368ef16df1649d98233335a9d1f7edb-0.parquet
2026-02-10 18:35:28,010 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/e368ef16df1649d98233335a9d1f7edb-0.parquet to S3 (4.67 MB)
2026-02-10 18:35:30,281 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/e368ef16df1649d98233335a9d1f7edb-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-16/e368ef16df1649d98233335a9d1f7edb-0.parquet
2026-02-10 18:35:30,282 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/e368ef16df1649d98233335a9d1f7edb-0.parquet
                                                                                                                                     2026-02-10 18:35:30,284 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/4f8aefc344ed420cbca466e579efa992-0.parquet
2026-02-10 18:35:30,284 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/4f8aefc344ed420cbca466e579efa992-0.parquet to S3 (2.42 MB)
2026-02-10 18:35:30,878 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/4f8aefc344ed420cbca466e579efa992-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-11/4f8aefc344ed420cbca466e579efa992-0.parquet
2026-02-10 18:35:30,882 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/4f8aefc344ed420cbca466e579efa992-0.parquet
                                                                                                                                     2026-02-10 18:35:30,885 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/6acde623565d42b198ca96bae00a7526-0.parquet
2026-02-10 18:35:30,885 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/6acde623565d42b198ca96bae00a7526-0.parquet to S3 (0.64 MB)
2026-02-10 18:35:31,050 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/6acde623565d42b198ca96bae00a7526-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-18/6acde623565d42b198ca96bae00a7526-0.parquet
2026-02-10 18:35:31,052 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/6acde623565d42b198ca96bae00a7526-0.parquet
                                                                                                                                     2026-02-10 18:35:31,053 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/c10f0dc55de54edbb002b454f8ab3810-0.parquet
2026-02-10 18:35:31,053 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/c10f0dc55de54edbb002b454f8ab3810-0.parquet to S3 (0.04 MB)
2026-02-10 18:35:31,114 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/c10f0dc55de54edbb002b454f8ab3810-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-15/c10f0dc55de54edbb002b454f8ab3810-0.parquet
2026-02-10 18:35:31,115 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/c10f0dc55de54edbb002b454f8ab3810-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 5/5 [00:03<00:00,  1.37it/s]
2026-02-10 18:35:31,115 INFO [logger.py]: Migrated 5 files for ml_inference_intergroup/cache: 5 succeeded, 0 failed.
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:03<00:00,  1.83s/it]
```

- [ ] **Validate in Athena**

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
1	1	59669
2	0	595852
3	-1	2
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC;
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	915
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	28525
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	47074
16	2024-10-16	47541
17	2024-10-17	4160
18	2024-10-18	7533
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
45	2024-11-14	7622
46	2024-11-15	10848
47	2024-11-16	7673
48	2024-11-17	4113
49	2024-11-18	1152
50	2024-11-19	8199
51	2024-11-20	431
52	2024-11-21	6851
53	2024-11-22	10422
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash
#	partition_date	uri_count
1	2024-09-28	4942
2	2024-09-29	443
3	2024-09-30	5747
4	2024-10-01	3701
5	2024-10-02	2744
6	2024-10-03	3112
7	2024-10-04	1073
8	2024-10-05	384
9	2024-10-06	265
10	2024-10-07	75
11	2024-10-09	1642
12	2024-10-10	3095
13	2024-10-11	8796
14	2024-10-12	85840
15	2024-10-13	68307
16	2024-10-14	73409
17	2024-10-15	47419
18	2024-10-16	42751
19	2024-10-17	9885
20	2024-10-18	7423
21	2024-10-19	46643
22	2024-10-20	72486
23	2024-10-21	71910
24	2024-10-22	74198
25	2024-10-23	85739
26	2024-10-24	67695
27	2024-10-25	64976
28	2024-10-26	65311
29	2024-10-27	60869
30	2024-10-28	47453
31	2024-10-29	78105
32	2024-10-30	64774
33	2024-10-31	72610
34	2024-11-01	67320
35	2024-11-02	51846
36	2024-11-03	66765
37	2024-11-04	63159
38	2024-11-05	57798
39	2024-11-06	81440
40	2024-11-07	54929
41	2024-11-08	86701
42	2024-11-09	69188
43	2024-11-10	71586
44	2024-11-11	85696
45	2024-11-12	77974
46	2024-11-13	88728
47	2024-11-14	67242
48	2024-11-15	86133
49	2024-11-16	90724
50	2024-11-17	62421
51	2024-11-18	5919
52	2024-11-19	91998
53	2024-11-20	7713
54	2024-11-21	46852
55	2024-11-22	96462
56	2024-11-23	38276
57	2024-11-24	2700
58	2024-11-25	29645
59	2024-11-26	82472
60	2024-11-27	82807
61	2024-11-28	89032
62	2024-11-29	86822
63	2024-11-30	84690
64	2024-12-01	87378
```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
1
(.venv) (base) ➜  queue sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
151
```

I checked and there's no more posts to run. Just one row that wasn't deleted it seems?

It seems like an edge case and IDK why it wasn't run. I think this may have been the row that ran but my code bugged out, but then a rerun doesn't fetch it? Weird...

Anyways, it's 499 posts that didn't get labeled, but ehhhh. We classified >80,000 other ones correctly.

- [X] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 4: 2024-10-19 to 2024-10-25

- [X] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-19 --end-date 2024-10-25 --source-data-location s3`

```bash
2026-02-10 19:08:51,270 INFO [logger.py]: Enqueuing 436710 posts for integration ml_inference_intergroup (after dedup and filtering previously labeled).
2026-02-10 19:08:51,271 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 19:08:51,839 INFO [logger.py]: Writing 436710 items as 437 minibatches to DB.
2026-02-10 19:08:51,839 INFO [logger.py]: Writing 437 minibatches to DB as 18 batches...
2026-02-10 19:08:51,839 INFO [logger.py]: Processing batch 1/18...
2026-02-10 19:08:52,154 INFO [logger.py]: Processing batch 11/18...
2026-02-10 19:08:52,387 INFO [logger.py]: Inserted 436710 posts into queue for integration: ml_inference_intergroup
2026-02-10 19:08:52,415 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-02-10 19:08:52,415 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

I'll have to run this in batches. I'll start with 50,000 first.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000
2026-02-10 19:18:58,851 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 19:18:58,971 INFO [logger.py]: Not clearing any queues.
2026-02-10 19:18:58,972 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-10 19:18:58,972 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-10 19:19:06,275 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 19:19:06,314 INFO [logger.py]: Current queue size: 437 items
2026-02-10 19:19:07,834 INFO [logger.py]: Loaded 436710 posts to classify.
2026-02-10 19:19:07,834 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-10 19:19:07,834 INFO [logger.py]: Latest inference timestamp: None
2026-02-10 19:19:08,088 INFO [logger.py]: After dropping duplicates, 436710 posts remain.
2026-02-10 19:19:08,488 INFO [logger.py]: After filtering, 429591 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 5 seconds
Memory usage for get_posts_to_classify: 1015.703125 MB
2026-02-10 19:19:11,262 INFO [logger.py]: Limited posts from 429591 to 49127 (max_records_per_run=50000, included 50 complete batches)
2026-02-10 19:19:11,356 INFO [logger.py]: Classifying 49127 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                                | 0/99 [00:00<?, ?batch/s]2026-02-10 19:19:12,431 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/99...

...

2026-02-10 20:08:52,223 INFO [logger.py]: Current queue size: 98 items
2026-02-10 20:08:52,224 INFO [logger.py]: Adding 127 posts to the output queue.
2026-02-10 20:08:52,224 INFO [logger.py]: Writing 127 items as 1 minibatches to DB.
2026-02-10 20:08:52,224 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-10 20:08:52,224 INFO [logger.py]: Processing batch 1/1...
2026-02-10 20:08:52,227 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-10 20:08:52,228 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████████████████████████████████████████████████| 99/99 [49:39<00:00, 30.10s/batch, successful=49127, failed=0]
Execution time for run_batch_classification: 49 minutes, 41 seconds
Memory usage for run_batch_classification: -290.265625 MB
Execution time for orchestrate_classification: 49 minutes, 49 seconds
Memory usage for orchestrate_classification: 23.515625 MB
Execution time for classify_latest_posts: 49 minutes, 51 seconds
Memory usage for classify_latest_posts: 20.84375 MB
2026-02-10 20:08:55,356 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-10 20:08:55,357 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

OK, now time to run the next batch. I'll do 60,000 as that'll probably take me to around 9:30pm or so, at which point I'll do a large overnight batch.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 60000
2026-02-10 20:13:48,145 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 20:13:48,252 INFO [logger.py]: Not clearing any queues.
2026-02-10 20:13:48,252 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-10 20:13:48,252 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-10 20:13:55,433 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 20:13:55,479 INFO [logger.py]: Current queue size: 387 items
2026-02-10 20:13:56,806 INFO [logger.py]: Loaded 386710 posts to classify.
2026-02-10 20:13:56,807 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-10 20:13:56,807 INFO [logger.py]: Latest inference timestamp: None
2026-02-10 20:13:57,023 INFO [logger.py]: After dropping duplicates, 386710 posts remain.
2026-02-10 20:13:57,407 INFO [logger.py]: After filtering, 380464 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 866.265625 MB
2026-02-10 20:13:59,803 INFO [logger.py]: Limited posts from 380464 to 59935 (max_records_per_run=60000, included 61 complete batches)
2026-02-10 20:13:59,872 INFO [logger.py]: Classifying 59935 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/120 [00:00<?, ?batch/s]2026-02-10 20:14:00,953 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/120...

...

2026-02-10 21:09:39,810 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-10 21:09:39,810 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-10 21:09:39,811 INFO [logger.py]: Current queue size: 218 items
2026-02-10 21:09:39,812 INFO [logger.py]: Adding 435 posts to the output queue.
2026-02-10 21:09:39,813 INFO [logger.py]: Writing 435 items as 1 minibatches to DB.
2026-02-10 21:09:39,813 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-10 21:09:39,814 INFO [logger.py]: Processing batch 1/1...
2026-02-10 21:09:39,816 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-10 21:09:39,817 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████████████████████████████████████████████████| 120/120 [55:38<00:00, 27.82s/batch, successful=59935, failed=0]
Execution time for run_batch_classification: 55 minutes, 40 seconds
Memory usage for run_batch_classification: -243.4375 MB
Execution time for orchestrate_classification: 55 minutes, 48 seconds
Memory usage for orchestrate_classification: 22.484375 MB
Execution time for classify_latest_posts: 55 minutes, 50 seconds
Memory usage for classify_latest_posts: 22.484375 MB
2026-02-10 21:09:42,966 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-10 21:09:42,968 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Next I'll just run the remainder of the posts.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 360000
2026-02-10 21:12:47,786 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-10 21:12:47,898 INFO [logger.py]: Not clearing any queues.
2026-02-10 21:12:47,898 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-10 21:12:47,898 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-10 21:12:55,086 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-10 21:12:55,135 INFO [logger.py]: Current queue size: 326 items
2026-02-10 21:12:56,257 INFO [logger.py]: Loaded 325710 posts to classify.
2026-02-10 21:12:56,258 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-10 21:12:56,258 INFO [logger.py]: Latest inference timestamp: None
2026-02-10 21:12:56,430 INFO [logger.py]: After dropping duplicates, 325710 posts remain.
2026-02-10 21:12:56,716 INFO [logger.py]: After filtering, 320529 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 760.640625 MB
2026-02-10 21:12:58,921 INFO [logger.py]: Classifying 320529 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/642 [00:00<?, ?batch/s]2026-02-10 21:13:00,005 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/642...

...

2026-02-11 01:54:15,410 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-11 01:54:15,412 INFO [logger.py]: Current queue size: 860 items
2026-02-11 01:54:15,413 INFO [logger.py]: Adding 29 posts to the output queue.
2026-02-11 01:54:15,413 INFO [logger.py]: Writing 29 items as 1 minibatches to DB.
2026-02-11 01:54:15,413 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 01:54:15,413 INFO [logger.py]: Processing batch 1/1...
2026-02-11 01:54:15,415 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 01:54:15,416 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 642/642 [4:41:15<00:00, 26.29s/batch, successful=320529, failed=0]
Execution time for run_batch_classification: 281 minutes, 16 seconds
Memory usage for run_batch_classification: -290.0625 MB
Execution time for orchestrate_classification: 281 minutes, 24 seconds
Memory usage for orchestrate_classification: 256.5625 MB
Execution time for classify_latest_posts: 281 minutes, 26 seconds
Memory usage for classify_latest_posts: 256.546875 MB
2026-02-11 01:54:18,767 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 01:54:18,770 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-11 08:22:49,788 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 08:22:49,921 INFO [logger.py]: Not clearing any queues.
2026-02-11 08:22:49,921 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 08:22:49,921 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-11 08:22:50,003 INFO [logger.py]: Current queue size: 861 items
2026-02-11 08:22:52,248 INFO [logger.py]: Exporting 429591 records to local storage for integration ml_inference_intergroup...
2026-02-11 08:22:52,869 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-18] Exporting n=5461 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,059 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,090 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-19] Exporting n=37647 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,135 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,177 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-20] Exporting n=63742 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,249 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,293 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-21] Exporting n=63916 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,364 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,411 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-22] Exporting n=65910 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,486 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,538 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-23] Exporting n=76205 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,624 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,669 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-24] Exporting n=64759 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,739 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,775 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-25] Exporting n=51951 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 08:22:53,832 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 08:22:53,848 INFO [logger.py]: Finished exporting 429591 records to local storage for integration ml_inference_intergroup...
2026-02-11 08:22:53,863 INFO [logger.py]: Successfully wrote 429591 records to storage for integration ml_inference_intergroup
```

- [X] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-02-11 08:23:54,625 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 08:23:54,726 INFO [logger.py]: Not clearing any queues.
2026-02-11 08:23:54,792 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-02-11 08:23:54,796 INFO [logger.py]: Registered 0 new files for migration (0 already in tracker).
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-11 08:23:54,814 INFO [logger.py]: Registered 8 new files for migration (157 already in tracker).                                                                             
Initialized migration tracker db for ml_inference_intergroup/cache (165 files)
Processing prefixes: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 103.94it/s]
Finished initializing migration tracker db
2026-02-11 08:23:54,823 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]2026-02-11 08:23:54,891 INFO [logger.py]: Migrating 0 file(s) for ml_inference_intergroup/active
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]
2026-02-11 08:23:54,891 INFO [logger.py]: Migrating 8 file(s) for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-11 08:23:54,892 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/8f82c68dd8c540a1a0a1b0cf8581825d-0.parquet
2026-02-11 08:23:54,892 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/8f82c68dd8c540a1a0a1b0cf8581825d-0.parquet to S3 (6.80 MB)
2026-02-11 08:23:56,773 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/8f82c68dd8c540a1a0a1b0cf8581825d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-21/8f82c68dd8c540a1a0a1b0cf8581825d-0.parquet
2026-02-11 08:23:56,775 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/8f82c68dd8c540a1a0a1b0cf8581825d-0.parquet
                                                                                                                                                                                   2026-02-11 08:23:56,777 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/334e18de4bf544ebba02277df592a630-0.parquet
2026-02-11 08:23:56,777 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/334e18de4bf544ebba02277df592a630-0.parquet to S3 (3.82 MB)
2026-02-11 08:23:57,690 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/334e18de4bf544ebba02277df592a630-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-19/334e18de4bf544ebba02277df592a630-0.parquet
2026-02-11 08:23:57,693 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/334e18de4bf544ebba02277df592a630-0.parquet
                                                                                                                                                                                   2026-02-11 08:23:57,695 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c03edbb6ce904477a89f92aeed1f3ebd-0.parquet
2026-02-11 08:23:57,695 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c03edbb6ce904477a89f92aeed1f3ebd-0.parquet to S3 (0.58 MB)
2026-02-11 08:23:57,849 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c03edbb6ce904477a89f92aeed1f3ebd-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c03edbb6ce904477a89f92aeed1f3ebd-0.parquet
2026-02-11 08:23:57,851 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/c03edbb6ce904477a89f92aeed1f3ebd-0.parquet
                                                                                                                                                                                   2026-02-11 08:23:57,853 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/aaf91d92416b4386a5dee641794a191a-0.parquet
2026-02-11 08:23:57,853 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/aaf91d92416b4386a5dee641794a191a-0.parquet to S3 (6.51 MB)
2026-02-11 08:23:59,403 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/aaf91d92416b4386a5dee641794a191a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-20/aaf91d92416b4386a5dee641794a191a-0.parquet
2026-02-11 08:23:59,405 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/aaf91d92416b4386a5dee641794a191a-0.parquet
                                                                                                                                                                                   2026-02-11 08:23:59,406 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/b7dcbb31879e47b1b8e706db603135b9-0.parquet
2026-02-11 08:23:59,407 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/b7dcbb31879e47b1b8e706db603135b9-0.parquet to S3 (5.51 MB)
2026-02-11 08:24:00,653 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/b7dcbb31879e47b1b8e706db603135b9-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-25/b7dcbb31879e47b1b8e706db603135b9-0.parquet
2026-02-11 08:24:00,655 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/b7dcbb31879e47b1b8e706db603135b9-0.parquet
                                                                                                                                                                                   2026-02-11 08:24:00,656 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/4bbe5ee90c4d4af0ae68e29bd06a7c30-0.parquet
2026-02-11 08:24:00,656 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/4bbe5ee90c4d4af0ae68e29bd06a7c30-0.parquet to S3 (6.94 MB)
2026-02-11 08:24:02,272 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/4bbe5ee90c4d4af0ae68e29bd06a7c30-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-22/4bbe5ee90c4d4af0ae68e29bd06a7c30-0.parquet
2026-02-11 08:24:02,274 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/4bbe5ee90c4d4af0ae68e29bd06a7c30-0.parquet
                                                                                                                                                                                   2026-02-11 08:24:02,275 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/5af9b9ddb51d4d17b568f5387b2df77e-0.parquet
2026-02-11 08:24:02,275 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/5af9b9ddb51d4d17b568f5387b2df77e-0.parquet to S3 (7.95 MB)
2026-02-11 08:24:04,588 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/5af9b9ddb51d4d17b568f5387b2df77e-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-23/5af9b9ddb51d4d17b568f5387b2df77e-0.parquet
2026-02-11 08:24:04,589 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/5af9b9ddb51d4d17b568f5387b2df77e-0.parquet
                                                                                                                                                                                   2026-02-11 08:24:04,590 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/e4c40d002031468190133e47ec4671f2-0.parquet
2026-02-11 08:24:04,590 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/e4c40d002031468190133e47ec4671f2-0.parquet to S3 (6.83 MB)
2026-02-11 08:24:06,196 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/e4c40d002031468190133e47ec4671f2-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-24/e4c40d002031468190133e47ec4671f2-0.parquet
2026-02-11 08:24:06,200 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/e4c40d002031468190133e47ec4671f2-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:11<00:00,  1.41s/it]
2026-02-11 08:24:06,201 INFO [logger.py]: Migrated 8 files for ml_inference_intergroup/cache: 8 succeeded, 0 failed.██████████████████████████████████| 8/8 [00:11<00:00,  1.63s/it]
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:11<00:00,  5.66s/it]
```

- [X] **Validate in Athena**

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
1	1	95725
2	-1	2
3	0	988235
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC;
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	915
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	28525
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	47074
16	2024-10-16	47541
17	2024-10-17	4160
18	2024-10-18	11843
19	2024-10-19	41765
20	2024-10-20	70941
21	2024-10-21	71065
22	2024-10-22	73140
23	2024-10-23	84596
24	2024-10-24	71993
25	2024-10-25	59671
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
45	2024-11-14	7622
46	2024-11-15	10848
47	2024-11-16	7673
48	2024-11-17	4113
49	2024-11-18	1152
50	2024-11-19	8199
51	2024-11-20	431
52	2024-11-21	6851
53	2024-11-22	10422
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash

```

- [X] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 5: 2024-10-26 to 2024-11-01

- [X] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3`

```bash

```

NOTE: I ran two commands simultaneously, since I'll be gone for a bit. I ran enqueueing plus the first 50,000 of the integrations.

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

Batch 1: 50,000

```bash
2026-02-11 09:43:48,099 INFO [logger.py]: Adding 500 posts to the output queue.
2026-02-11 09:43:48,099 INFO [logger.py]: Writing 500 items as 1 minibatches to DB.
2026-02-11 09:43:48,099 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 09:43:48,099 INFO [logger.py]: Processing batch 1/1...
2026-02-11 09:43:48,101 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 09:43:48,101 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches:  99%|██████████████████████████████████████████████████████████████████████████████████████████ | 98/99 [51:14<00:26, 26.04s/batch, successful=49000, failed=0]2026-02-11 09:43:59,915 INFO [logger.py]: Successfully labeled 200 posts.
2026-02-11 09:43:59,916 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 09:43:59,917 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 09:43:59,919 INFO [logger.py]: Current queue size: 369 items
2026-02-11 09:43:59,919 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 09:43:59,919 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-11 09:43:59,922 INFO [logger.py]: Current queue size: 98 items
2026-02-11 09:43:59,922 INFO [logger.py]: Adding 200 posts to the output queue.
2026-02-11 09:43:59,923 INFO [logger.py]: Writing 200 items as 1 minibatches to DB.
2026-02-11 09:43:59,923 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 09:43:59,923 INFO [logger.py]: Processing batch 1/1...
2026-02-11 09:43:59,925 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 09:43:59,926 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████████████████████████████████████████████████| 99/99 [51:26<00:00, 31.18s/batch, successful=49200, failed=0]
Execution time for run_batch_classification: 51 minutes, 28 seconds
Memory usage for run_batch_classification: -269.0625 MB
Execution time for orchestrate_classification: 51 minutes, 36 seconds
Memory usage for orchestrate_classification: 50.59375 MB
Execution time for classify_latest_posts: 51 minutes, 38 seconds
Memory usage for classify_latest_posts: 50.609375 MB
2026-02-11 09:44:03,168 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 09:44:03,171 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Batch 2: 100,000

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 100000
2026-02-11 10:22:07,111 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 10:22:07,212 INFO [logger.py]: Not clearing any queues.
2026-02-11 10:22:07,212 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-11 10:22:07,212 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-11 10:22:14,636 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 10:22:14,678 INFO [logger.py]: Current queue size: 369 items
2026-02-11 10:22:16,066 INFO [logger.py]: Loaded 368463 posts to classify.
2026-02-11 10:22:16,067 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-11 10:22:16,067 INFO [logger.py]: Latest inference timestamp: None
2026-02-11 10:22:16,262 INFO [logger.py]: After dropping duplicates, 368463 posts remain.
2026-02-11 10:22:16,588 INFO [logger.py]: After filtering, 362620 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 771.25 MB
2026-02-11 10:22:18,935 INFO [logger.py]: Limited posts from 362620 to 99399 (max_records_per_run=100000, included 101 complete batches)
2026-02-11 10:22:19,007 INFO [logger.py]: Classifying 99399 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/199 [00:00<?, ?batch/s]2026-02-11 10:22:20,091 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/199...

...

2026-02-11 11:59:55,644 INFO [logger.py]: Current queue size: 297 items
2026-02-11 11:59:55,645 INFO [logger.py]: Adding 399 posts to the output queue.
2026-02-11 11:59:55,646 INFO [logger.py]: Writing 399 items as 1 minibatches to DB.
2026-02-11 11:59:55,647 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 11:59:55,647 INFO [logger.py]: Processing batch 1/1...
2026-02-11 11:59:55,648 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 11:59:55,649 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████████████████████████████████████████████| 199/199 [1:37:35<00:00, 29.42s/batch, successful=99399, failed=0]
Execution time for run_batch_classification: 97 minutes, 37 seconds
Memory usage for run_batch_classification: -214.5625 MB
Execution time for orchestrate_classification: 97 minutes, 44 seconds
Memory usage for orchestrate_classification: 45.734375 MB
Execution time for classify_latest_posts: 97 minutes, 46 seconds
Memory usage for classify_latest_posts: 45.734375 MB
2026-02-11 11:59:58,812 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 11:59:58,814 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Batch 3: 150,000

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 150000
2026-02-11 12:05:06,466 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 12:05:06,595 INFO [logger.py]: Not clearing any queues.
2026-02-11 12:05:06,595 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-11 12:05:06,596 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-11 12:05:15,533 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 12:05:15,570 INFO [logger.py]: Current queue size: 268 items
2026-02-11 12:05:16,485 INFO [logger.py]: Loaded 267463 posts to classify.
2026-02-11 12:05:16,486 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-11 12:05:16,486 INFO [logger.py]: Latest inference timestamp: None
2026-02-11 12:05:16,619 INFO [logger.py]: After dropping duplicates, 267463 posts remain.
2026-02-11 12:05:16,855 INFO [logger.py]: After filtering, 263221 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 3 seconds
Memory usage for get_posts_to_classify: 630.875 MB
2026-02-11 12:05:18,973 INFO [logger.py]: Limited posts from 263221 to 149597 (max_records_per_run=150000, included 152 complete batches)
2026-02-11 12:05:19,001 INFO [logger.py]: Classifying 149597 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/300 [00:00<?, ?batch/s]2026-02-11 12:05:20,066 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/300...
2026-02-11 12:05:46,959 INFO [logger.py]: Successfully labeled 500 posts.
2026-02-11 12:05:46,959 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 12:05:46,960 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 12:05:46,962 INFO [logger.py]: Current queue size: 268 items
2026-02-11 12:05:46,962 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 12:05:46,962 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-11 12:05:46,996 INFO [logger.py]: Current queue size: 298 items
2026-02-11 12:05:46,998 INFO [logger.py]: Adding 500 posts to the output queue.
2026-02-11 12:05:46,999 INFO [logger.py]: Writing 500 items as 1 minibatches to DB.
2026-02-11 12:05:46,999 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 12:05:46,999 INFO [logger.py]: Processing batch 1/1...
2026-02-11 12:05:47,001 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 12:05:47,001 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches:   0%|▎                                                                                          | 1/300 [00:26<2:14:13, 26.94s/batch, successful=500, failed=0]

...

2026-02-11 14:13:10,650 INFO [logger.py]: Current queue size: 597 items
2026-02-11 14:13:10,650 INFO [logger.py]: Adding 97 posts to the output queue.
2026-02-11 14:13:10,650 INFO [logger.py]: Writing 97 items as 1 minibatches to DB.
2026-02-11 14:13:10,650 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 14:13:10,650 INFO [logger.py]: Processing batch 1/1...
2026-02-11 14:13:10,652 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 14:13:10,652 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 300/300 [2:07:50<00:00, 25.57s/batch, successful=149597, failed=0]
Execution time for run_batch_classification: 127 minutes, 52 seconds
Memory usage for run_batch_classification: -258.296875 MB
Execution time for orchestrate_classification: 127 minutes, 58 seconds
Memory usage for orchestrate_classification: 93.296875 MB
Execution time for classify_latest_posts: 128 minutes, 0 seconds
Memory usage for classify_latest_posts: 93.296875 MB
2026-02-11 14:13:13,837 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 14:13:13,838 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

2 hours to train 150,000 posts. Should be ~120,000 left. I'll wrap up the rest in this next batch.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 150000
2026-02-11 14:25:20,532 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 14:25:20,637 INFO [logger.py]: Not clearing any queues.
2026-02-11 14:25:20,637 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-11 14:25:20,637 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-11 14:25:27,331 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 14:25:27,355 INFO [logger.py]: Current queue size: 116 items
2026-02-11 14:25:27,870 INFO [logger.py]: Loaded 115463 posts to classify.
2026-02-11 14:25:27,870 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-11 14:25:27,870 INFO [logger.py]: Latest inference timestamp: None
2026-02-11 14:25:27,928 INFO [logger.py]: After dropping duplicates, 115463 posts remain.
2026-02-11 14:25:28,022 INFO [logger.py]: After filtering, 113624 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 277.09375 MB
2026-02-11 14:25:29,394 INFO [logger.py]: Classifying 113624 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/228 [00:00<?, ?batch/s]2026-02-11 14:25:30,465 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/228...

...

2026-02-11 16:16:12,810 INFO [logger.py]: Adding 124 posts to the output queue.
2026-02-11 16:16:12,810 INFO [logger.py]: Writing 124 items as 1 minibatches to DB.
2026-02-11 16:16:12,810 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 16:16:12,810 INFO [logger.py]: Processing batch 1/1...
2026-02-11 16:16:12,813 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 16:16:12,813 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 228/228 [1:50:42<00:00, 29.13s/batch, successful=113624, failed=0]
Execution time for run_batch_classification: 110 minutes, 43 seconds
Memory usage for run_batch_classification: -101.421875 MB
Execution time for orchestrate_classification: 110 minutes, 49 seconds
Memory usage for orchestrate_classification: 88.109375 MB
Execution time for classify_latest_posts: 110 minutes, 51 seconds
Memory usage for classify_latest_posts: 88.140625 MB
2026-02-11 16:16:16,042 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 16:16:16,044 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-11 18:41:43,659 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 18:41:43,764 INFO [logger.py]: Not clearing any queues.
2026-02-11 18:41:43,765 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 18:41:43,765 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-11 18:41:43,839 INFO [logger.py]: Current queue size: 826 items
2026-02-11 18:41:46,032 INFO [logger.py]: Exporting 411820 records to local storage for integration ml_inference_intergroup...
2026-02-11 18:41:46,647 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-25] Exporting n=20094 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:46,854 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:46,898 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-26] Exporting n=56108 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:46,968 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,011 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-27] Exporting n=53772 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:47,070 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,102 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-28] Exporting n=42694 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:47,150 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,192 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-29] Exporting n=61622 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:47,257 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,299 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-30] Exporting n=57855 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:47,363 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,410 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-31] Exporting n=66011 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:47,479 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,517 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-01] Exporting n=53664 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-11 18:41:47,572 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-11 18:41:47,585 INFO [logger.py]: Finished exporting 411820 records to local storage for integration ml_inference_intergroup...
2026-02-11 18:41:47,600 INFO [logger.py]: Successfully wrote 411820 records to storage for integration ml_inference_intergroup
```

- [X] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-02-11 18:42:04,909 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 18:42:05,017 INFO [logger.py]: Not clearing any queues.
2026-02-11 18:42:05,036 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-02-11 18:42:05,040 INFO [logger.py]: Registered 0 new files for migration (0 already in tracker).
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-11 18:42:05,059 INFO [logger.py]: Registered 8 new files for migration (165 already in tracker).                                                                             
Initialized migration tracker db for ml_inference_intergroup/cache (173 files)
Processing prefixes: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 103.30it/s]
Finished initializing migration tracker db
2026-02-11 18:42:05,062 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]2026-02-11 18:42:05,125 INFO [logger.py]: Migrating 0 file(s) for ml_inference_intergroup/active
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]
2026-02-11 18:42:05,126 INFO [logger.py]: Migrating 8 file(s) for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-11 18:42:05,127 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/0b4d31f2f678435f96d3f8ec9aa9f20c-0.parquet
2026-02-11 18:42:05,127 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/0b4d31f2f678435f96d3f8ec9aa9f20c-0.parquet to S3 (5.71 MB)
2026-02-11 18:42:07,645 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/0b4d31f2f678435f96d3f8ec9aa9f20c-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-26/0b4d31f2f678435f96d3f8ec9aa9f20c-0.parquet
2026-02-11 18:42:07,650 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/0b4d31f2f678435f96d3f8ec9aa9f20c-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:07,655 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/52cba351c52043bc8ab02a01cb01c2ae-0.parquet
2026-02-11 18:42:07,655 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/52cba351c52043bc8ab02a01cb01c2ae-0.parquet to S3 (4.48 MB)
2026-02-11 18:42:09,070 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/52cba351c52043bc8ab02a01cb01c2ae-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-28/52cba351c52043bc8ab02a01cb01c2ae-0.parquet
2026-02-11 18:42:09,074 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/52cba351c52043bc8ab02a01cb01c2ae-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:09,077 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/c50668e16480486181f66cb50d26374f-0.parquet
2026-02-11 18:42:09,078 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/c50668e16480486181f66cb50d26374f-0.parquet to S3 (6.28 MB)
2026-02-11 18:42:10,856 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/c50668e16480486181f66cb50d26374f-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-29/c50668e16480486181f66cb50d26374f-0.parquet
2026-02-11 18:42:10,861 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/c50668e16480486181f66cb50d26374f-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:10,866 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/5b86ee6cf5864938a0d1b03ba50c10d8-0.parquet
2026-02-11 18:42:10,867 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/5b86ee6cf5864938a0d1b03ba50c10d8-0.parquet to S3 (5.56 MB)
2026-02-11 18:42:12,424 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/5b86ee6cf5864938a0d1b03ba50c10d8-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-27/5b86ee6cf5864938a0d1b03ba50c10d8-0.parquet
2026-02-11 18:42:12,426 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/5b86ee6cf5864938a0d1b03ba50c10d8-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:12,426 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/81cab357ba7b4464aeb2c2f0f733a058-0.parquet
2026-02-11 18:42:12,426 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/81cab357ba7b4464aeb2c2f0f733a058-0.parquet to S3 (2.12 MB)
2026-02-11 18:42:12,985 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/81cab357ba7b4464aeb2c2f0f733a058-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-25/81cab357ba7b4464aeb2c2f0f733a058-0.parquet
2026-02-11 18:42:12,988 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/81cab357ba7b4464aeb2c2f0f733a058-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:12,991 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/40cda1e9361f4eb09859b7c7d92e14c4-0.parquet
2026-02-11 18:42:12,991 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/40cda1e9361f4eb09859b7c7d92e14c4-0.parquet to S3 (5.39 MB)
2026-02-11 18:42:14,687 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/40cda1e9361f4eb09859b7c7d92e14c4-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-01/40cda1e9361f4eb09859b7c7d92e14c4-0.parquet
2026-02-11 18:42:14,690 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/40cda1e9361f4eb09859b7c7d92e14c4-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:14,693 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/1df9db5554ad4a28b99f6cc2120d1064-0.parquet
2026-02-11 18:42:14,694 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/1df9db5554ad4a28b99f6cc2120d1064-0.parquet to S3 (5.86 MB)
2026-02-11 18:42:17,804 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/1df9db5554ad4a28b99f6cc2120d1064-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-30/1df9db5554ad4a28b99f6cc2120d1064-0.parquet
2026-02-11 18:42:17,808 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/1df9db5554ad4a28b99f6cc2120d1064-0.parquet
                                                                                                                                                                                   2026-02-11 18:42:17,812 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/302fd7ec231b4d1184c9e65a94a9e71a-0.parquet
2026-02-11 18:42:17,812 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/302fd7ec231b4d1184c9e65a94a9e71a-0.parquet to S3 (6.49 MB)
2026-02-11 18:42:19,604 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/302fd7ec231b4d1184c9e65a94a9e71a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-31/302fd7ec231b4d1184c9e65a94a9e71a-0.parquet
2026-02-11 18:42:19,610 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/302fd7ec231b4d1184c9e65a94a9e71a-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:14<00:00,  1.81s/it]
2026-02-11 18:42:19,612 INFO [logger.py]: Migrated 8 files for ml_inference_intergroup/cache: 8 succeeded, 0 failed.██████████████████████████████████| 8/8 [00:14<00:00,  1.93s/it]
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:14<00:00,  7.24s/it]
```

- [X] **Validate in Athena**

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
1	-1	2
2	0	1356944
3	1	131616
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC;
```

```bash
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	915
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	28525
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	47074
16	2024-10-16	47541
17	2024-10-17	4160
18	2024-10-18	11843
19	2024-10-19	41765
20	2024-10-20	70941
21	2024-10-21	71065
22	2024-10-22	73140
23	2024-10-23	84596
24	2024-10-24	71993
25	2024-10-25	72545
26	2024-10-26	62379
27	2024-10-27	59806
28	2024-10-28	47483
29	2024-10-29	68359
30	2024-10-30	64191
31	2024-10-31	73458
32	2024-11-01	60757
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
45	2024-11-14	7622
46	2024-11-15	10848
47	2024-11-16	7673
48	2024-11-17	4113
49	2024-11-18	1152
50	2024-11-19	8199
51	2024-11-20	431
52	2024-11-21	6851
53	2024-11-22	10422
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash

```

- [X] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 6: 2024-11-02 to 2024-11-08

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3`

```bash

```

Running as one command so I can let this be for 2 hours:

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3 && uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 80000
```

```bash
2026-02-11 19:54:15,628 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-11 19:54:15,628 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-11 19:54:15,629 INFO [logger.py]: Current queue size: 159 items
2026-02-11 19:54:15,630 INFO [logger.py]: Adding 93 posts to the output queue.
2026-02-11 19:54:15,630 INFO [logger.py]: Writing 93 items as 1 minibatches to DB.
2026-02-11 19:54:15,631 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 19:54:15,631 INFO [logger.py]: Processing batch 1/1...
2026-02-11 19:54:15,633 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 19:54:15,634 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████████████████████████████████████████████████| 160/160 [56:55<00:00, 21.35s/batch, successful=79593, failed=0]
Execution time for run_batch_classification: 56 minutes, 57 seconds
Memory usage for run_batch_classification: -341.96875 MB
Execution time for orchestrate_classification: 57 minutes, 5 seconds
Memory usage for orchestrate_classification: 49.4375 MB
Execution time for classify_latest_posts: 57 minutes, 7 seconds
Memory usage for classify_latest_posts: 49.453125 MB
2026-02-11 19:54:18,878 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 19:54:18,881 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

Batch 2: 10,000.

Accidentally did this instead of 1,000 (I wanted to just see how many posts were left). This is OK, and I'll run more overnight.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000 
2026-02-11 20:42:26,248 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 20:42:26,312 INFO [logger.py]: Not clearing any queues.
2026-02-11 20:42:26,312 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-11 20:42:26,313 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-11 20:42:33,365 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 20:42:33,412 INFO [logger.py]: Current queue size: 343 items
2026-02-11 20:42:34,604 INFO [logger.py]: Loaded 342020 posts to classify.
2026-02-11 20:42:34,605 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-11 20:42:34,605 INFO [logger.py]: Latest inference timestamp: None
2026-02-11 20:42:34,778 INFO [logger.py]: After dropping duplicates, 342020 posts remain.
2026-02-11 20:42:35,122 INFO [logger.py]: After filtering, 336368 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 815.0625 MB
2026-02-11 20:42:37,407 INFO [logger.py]: Limited posts from 336368 to 9794 (max_records_per_run=10000, included 10 complete batches)
2026-02-11 20:42:37,488 INFO [logger.py]: Classifying 9794 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                                | 0/20 [00:00<?, ?batch/s]2026-02-11 20:42:38,579 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/20...

...

2026-02-11 20:50:02,899 INFO [logger.py]: Current queue size: 179 items
2026-02-11 20:50:02,904 INFO [logger.py]: Adding 294 posts to the output queue.
2026-02-11 20:50:02,905 INFO [logger.py]: Writing 294 items as 1 minibatches to DB.
2026-02-11 20:50:02,905 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-11 20:50:02,905 INFO [logger.py]: Processing batch 1/1...
2026-02-11 20:50:02,907 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-11 20:50:02,907 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████████████████████████████████████████████████████| 20/20 [07:24<00:00, 22.22s/batch, successful=9794, failed=0]
Execution time for run_batch_classification: 7 minutes, 25 seconds
Memory usage for run_batch_classification: -249.15625 MB
Execution time for orchestrate_classification: 7 minutes, 33 seconds
Memory usage for orchestrate_classification: -12.640625 MB
Execution time for classify_latest_posts: 7 minutes, 35 seconds
Memory usage for classify_latest_posts: -12.65625 MB
2026-02-11 20:50:06,029 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-11 20:50:06,031 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Now I'll do the rest:

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 350000
2026-02-11 20:52:18,450 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-11 20:52:18,551 INFO [logger.py]: Not clearing any queues.
2026-02-11 20:52:18,552 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-11 20:52:18,552 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-11 20:52:25,256 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-11 20:52:25,298 INFO [logger.py]: Current queue size: 333 items
2026-02-11 20:52:26,463 INFO [logger.py]: Loaded 332020 posts to classify.
2026-02-11 20:52:26,464 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-11 20:52:26,464 INFO [logger.py]: Latest inference timestamp: None
2026-02-11 20:52:26,629 INFO [logger.py]: After dropping duplicates, 332020 posts remain.
2026-02-11 20:52:26,954 INFO [logger.py]: After filtering, 326574 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 772.796875 MB
2026-02-11 20:52:29,185 INFO [logger.py]: Classifying 326574 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/654 [00:00<?, ?batch/s]2026-02-11 20:52:30,282 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/654...

...

2026-02-12 01:07:15,101 INFO [logger.py]: Current queue size: 833 items
2026-02-12 01:07:15,101 INFO [logger.py]: Adding 74 posts to the output queue.
2026-02-12 01:07:15,102 INFO [logger.py]: Writing 74 items as 1 minibatches to DB.
2026-02-12 01:07:15,102 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 01:07:15,102 INFO [logger.py]: Processing batch 1/1...
2026-02-12 01:07:15,104 INFO [logger.py]: Deleting 2 batch IDs from the input queue.
2026-02-12 01:07:15,105 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 654/654 [4:14:44<00:00, 23.37s/batch, successful=326574, failed=0]
Execution time for run_batch_classification: 254 minutes, 46 seconds
Memory usage for run_batch_classification: -317.546875 MB
Execution time for orchestrate_classification: 254 minutes, 53 seconds
Memory usage for orchestrate_classification: 276.6875 MB
Execution time for classify_latest_posts: 254 minutes, 55 seconds
Memory usage for classify_latest_posts: 276.671875 MB
2026-02-12 01:07:18,393 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 01:07:18,395 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-12 07:10:20,402 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 07:10:20,511 INFO [logger.py]: Not clearing any queues.
2026-02-12 07:10:20,512 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 07:10:20,512 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 07:10:20,585 INFO [logger.py]: Current queue size: 834 items
2026-02-12 07:10:22,727 INFO [logger.py]: Exporting 415961 records to local storage for integration ml_inference_intergroup...
2026-02-12 07:10:23,371 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-01] Exporting n=11425 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:23,487 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:23,520 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-02] Exporting n=47066 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:23,574 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:23,614 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-03] Exporting n=56718 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:23,672 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:23,714 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-04] Exporting n=56287 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:23,771 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:23,830 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-05] Exporting n=59055 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:23,890 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:23,941 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-06] Exporting n=74999 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:24,021 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:24,057 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-07] Exporting n=51208 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:24,110 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:24,150 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-08] Exporting n=59203 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 07:10:24,214 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 07:10:24,227 INFO [logger.py]: Finished exporting 415961 records to local storage for integration ml_inference_intergroup...
2026-02-12 07:10:24,242 INFO [logger.py]: Successfully wrote 415961 records to storage for integration ml_inference_intergroup
```

- [X] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-02-12 07:10:42,212 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 07:10:42,274 INFO [logger.py]: Not clearing any queues.
2026-02-12 07:10:42,296 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-02-12 07:10:42,300 INFO [logger.py]: Registered 0 new files for migration (0 already in tracker).
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-12 07:10:42,320 INFO [logger.py]: Registered 8 new files for migration (173 already in tracker).                                                                             
Initialized migration tracker db for ml_inference_intergroup/cache (181 files)
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 95.80it/s]
Finished initializing migration tracker db
2026-02-12 07:10:42,323 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]2026-02-12 07:10:42,388 INFO [logger.py]: Migrating 0 file(s) for ml_inference_intergroup/active
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]
2026-02-12 07:10:42,389 INFO [logger.py]: Migrating 8 file(s) for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-12 07:10:42,389 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/1cc3c8460a01402199ee30d9a531ef71-0.parquet
2026-02-12 07:10:42,389 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/1cc3c8460a01402199ee30d9a531ef71-0.parquet to S3 (5.96 MB)
2026-02-12 07:10:45,631 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/1cc3c8460a01402199ee30d9a531ef71-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-04/1cc3c8460a01402199ee30d9a531ef71-0.parquet
2026-02-12 07:10:45,633 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/1cc3c8460a01402199ee30d9a531ef71-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:45,634 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/f8cd723d261f46e8b1a1df53d18b01dd-0.parquet
2026-02-12 07:10:45,634 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/f8cd723d261f46e8b1a1df53d18b01dd-0.parquet to S3 (5.87 MB)
2026-02-12 07:10:47,323 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/f8cd723d261f46e8b1a1df53d18b01dd-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-03/f8cd723d261f46e8b1a1df53d18b01dd-0.parquet
2026-02-12 07:10:47,325 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/f8cd723d261f46e8b1a1df53d18b01dd-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:47,326 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/a9a205dc8837414384546dd4861073da-0.parquet
2026-02-12 07:10:47,326 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/a9a205dc8837414384546dd4861073da-0.parquet to S3 (4.78 MB)
2026-02-12 07:10:48,569 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/a9a205dc8837414384546dd4861073da-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-02/a9a205dc8837414384546dd4861073da-0.parquet
2026-02-12 07:10:48,571 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/a9a205dc8837414384546dd4861073da-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:48,572 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/541a565d4d54481c862138cc2e8f284d-0.parquet
2026-02-12 07:10:48,573 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/541a565d4d54481c862138cc2e8f284d-0.parquet to S3 (5.94 MB)
2026-02-12 07:10:51,110 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/541a565d4d54481c862138cc2e8f284d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-05/541a565d4d54481c862138cc2e8f284d-0.parquet
2026-02-12 07:10:51,114 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/541a565d4d54481c862138cc2e8f284d-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:51,117 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/6280ab200d7b4e5ba414e0ff461d7525-0.parquet
2026-02-12 07:10:51,117 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/6280ab200d7b4e5ba414e0ff461d7525-0.parquet to S3 (5.20 MB)
2026-02-12 07:10:52,745 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/6280ab200d7b4e5ba414e0ff461d7525-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-07/6280ab200d7b4e5ba414e0ff461d7525-0.parquet
2026-02-12 07:10:52,748 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/6280ab200d7b4e5ba414e0ff461d7525-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:52,752 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/ddb85dc2eced499faacbae40a7b88a44-0.parquet
2026-02-12 07:10:52,752 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/ddb85dc2eced499faacbae40a7b88a44-0.parquet to S3 (7.45 MB)
2026-02-12 07:10:54,520 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/ddb85dc2eced499faacbae40a7b88a44-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-06/ddb85dc2eced499faacbae40a7b88a44-0.parquet
2026-02-12 07:10:54,522 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/ddb85dc2eced499faacbae40a7b88a44-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:54,524 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/855ddb1f71574bab97e40d08d101b179-0.parquet
2026-02-12 07:10:54,524 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/855ddb1f71574bab97e40d08d101b179-0.parquet to S3 (1.22 MB)
2026-02-12 07:10:54,810 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/855ddb1f71574bab97e40d08d101b179-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-01/855ddb1f71574bab97e40d08d101b179-0.parquet
2026-02-12 07:10:54,812 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/855ddb1f71574bab97e40d08d101b179-0.parquet
                                                                                                                                                                                   2026-02-12 07:10:54,813 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/251e9f43c4c3489fb18a00d44561ef02-0.parquet
2026-02-12 07:10:54,813 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/251e9f43c4c3489fb18a00d44561ef02-0.parquet to S3 (5.99 MB)
2026-02-12 07:10:56,241 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/251e9f43c4c3489fb18a00d44561ef02-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-08/251e9f43c4c3489fb18a00d44561ef02-0.parquet
2026-02-12 07:10:56,245 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/251e9f43c4c3489fb18a00d44561ef02-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:13<00:00,  1.73s/it]
2026-02-12 07:10:56,246 INFO [logger.py]: Migrated 8 files for ml_inference_intergroup/cache: 8 succeeded, 0 failed.██████████████████████████████████| 8/8 [00:13<00:00,  1.39s/it]
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:13<00:00,  6.93s/it]
```

- [ ] **Validate in Athena**

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
1	0	1725167
2	1	172983
3	-1	2
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
) t
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
6	2024-10-04	915
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	28525
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	47074
16	2024-10-16	47541
17	2024-10-17	4160
18	2024-10-18	11843
19	2024-10-19	41765
20	2024-10-20	70941
21	2024-10-21	71065
22	2024-10-22	73140
23	2024-10-23	84596
24	2024-10-24	71993
25	2024-10-25	72545
26	2024-10-26	62379
27	2024-10-27	59806
28	2024-10-28	47483
29	2024-10-29	68359
30	2024-10-30	64191
31	2024-10-31	73458
32	2024-11-01	65812
33	2024-11-02	52408
34	2024-11-03	62927
35	2024-11-04	62562
36	2024-11-05	65707
37	2024-11-06	83342
38	2024-11-07	56969
39	2024-11-08	67922
40	2024-11-09	7117
41	2024-11-10	7024
42	2024-11-11	7983
43	2024-11-12	7768
44	2024-11-13	8837
45	2024-11-14	7622
46	2024-11-15	10848
47	2024-11-16	7673
48	2024-11-17	4113
49	2024-11-18	1152
50	2024-11-19	8199
51	2024-11-20	431
52	2024-11-21	6851
53	2024-11-22	10422
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
0
(.venv) (base) ➜  queue sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
834
```

- [X] **Delete input and output queues**

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 7: 2024-11-09 to 2024-11-15

- [X] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-09 --end-date 2024-11-15 --source-data-location s3`

```bash
2026-02-12 07:54:00,056 INFO [logger.py]: Listed total_parquet_files=9 for dataset=ml_inference_intergroup.
2026-02-12 07:54:02,638 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 57315, 'columns': 1}, 'result_memory_usage_mb': np.float64(6.941865921020508)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-02-12 07:54:02,651 INFO [logger.py]: Loaded 57315 post URI rows from S3 for service ml_inference_intergroup (57195 unique URIs).
2026-02-12 07:54:02,717 INFO [logger.py]: Enqueuing 501567 posts for integration ml_inference_intergroup (after dedup and filtering previously labeled).
2026-02-12 07:54:02,718 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 07:54:03,430 INFO [logger.py]: Writing 501567 items as 502 minibatches to DB.
2026-02-12 07:54:03,430 INFO [logger.py]: Writing 502 minibatches to DB as 21 batches...
2026-02-12 07:54:03,430 INFO [logger.py]: Processing batch 1/21...
2026-02-12 07:54:03,799 INFO [logger.py]: Processing batch 11/21...
2026-02-12 07:54:04,143 INFO [logger.py]: Processing batch 21/21...
2026-02-12 07:54:04,158 INFO [logger.py]: Inserted 501567 posts into queue for integration: ml_inference_intergroup
2026-02-12 07:54:04,187 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-02-12 07:54:04,187 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

I'll do this in batches. I'll make the first batch 50,000.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000
2026-02-12 08:29:44,993 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 08:29:45,094 INFO [logger.py]: Not clearing any queues.
2026-02-12 08:29:45,094 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-12 08:29:45,094 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-12 08:29:51,902 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 08:29:51,984 INFO [logger.py]: Current queue size: 502 items
2026-02-12 08:29:53,686 INFO [logger.py]: Loaded 501567 posts to classify.
2026-02-12 08:29:53,686 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-12 08:29:53,686 INFO [logger.py]: Latest inference timestamp: None
2026-02-12 08:29:53,926 INFO [logger.py]: After dropping duplicates, 501567 posts remain.
2026-02-12 08:29:54,370 INFO [logger.py]: After filtering, 492699 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 5 seconds
Memory usage for get_posts_to_classify: 1173.828125 MB
2026-02-12 08:29:57,183 INFO [logger.py]: Limited posts from 492699 to 49194 (max_records_per_run=50000, included 50 complete batches)
2026-02-12 08:29:57,307 INFO [logger.py]: Classifying 49194 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                                | 0/99 [00:00<?, ?batch/s]2026-02-12 08:29:58,393 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/99...
2026-02-12 08:30:13,396 INFO [_base_client.py]: Retrying request to /chat/completions in 0.429927 seconds
Retrying request to /chat/completions in 0.429927 seconds
2026-02-12 08:30:22,490 INFO [logger.py]: Successfully labeled 500 posts.
2026-02-12 08:30:22,490 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 08:30:22,491 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 08:30:22,548 INFO [logger.py]: Current queue size: 502 items
2026-02-12 08:30:22,548 INFO [logger.py]: Creating new SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 08:30:22,550 INFO [logger.py]: Adding 500 posts to the output queue.
2026-02-12 08:30:22,551 INFO [logger.py]: Writing 500 items as 1 minibatches to DB.
2026-02-12 08:30:22,551 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 08:30:22,551 INFO [logger.py]: Processing batch 1/1...
2026-02-12 08:30:22,553 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 08:30:22,554 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches:   1%|▉                                                                                             | 1/99 [00:24<39:27, 24.16s/batch, successful=500, failed=0]2026-02-12 08:30:23,167 INFO [_base_client.py]: Retrying request to /chat/completions in 0.477359 seconds
Retrying request to /chat/completions in 0.477359 seconds
...

2026-02-12 09:58:20,537 INFO [logger.py]: Successfully labeled 194 posts.
2026-02-12 09:58:20,538 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 09:58:20,538 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 09:58:20,540 INFO [logger.py]: Current queue size: 452 items
2026-02-12 09:58:20,540 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 09:58:20,540 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 09:58:20,542 INFO [logger.py]: Current queue size: 98 items
2026-02-12 09:58:20,542 INFO [logger.py]: Adding 194 posts to the output queue.
2026-02-12 09:58:20,543 INFO [logger.py]: Writing 194 items as 1 minibatches to DB.
2026-02-12 09:58:20,543 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 09:58:20,543 INFO [logger.py]: Processing batch 1/1...
2026-02-12 09:58:20,544 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 09:58:20,545 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████████████████████████████████████████████████| 99/99 [1:28:22<00:00, 53.56s/batch, successful=49194, failed=0]
Execution time for run_batch_classification: 88 minutes, 23 seconds
Memory usage for run_batch_classification: -316.25 MB
Execution time for orchestrate_classification: 88 minutes, 32 seconds
Memory usage for orchestrate_classification: 41.25 MB
Execution time for classify_latest_posts: 88 minutes, 34 seconds
Memory usage for classify_latest_posts: 41.234375 MB
2026-02-12 09:58:23,751 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 09:58:23,754 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

I'll do 50,000 for the next batch. I want to make sure that rate limits reset.

```bash
2026-02-12 11:04:42,559 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 11:04:42,560 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████████████████████████████████████████████████| 99/99 [45:43<00:00, 27.71s/batch, successful=49048, failed=0]
Execution time for run_batch_classification: 45 minutes, 44 seconds
Memory usage for run_batch_classification: -139.6875 MB
Execution time for orchestrate_classification: 45 minutes, 53 seconds
Memory usage for orchestrate_classification: 27.296875 MB
Execution time for classify_latest_posts: 45 minutes, 55 seconds
Memory usage for classify_latest_posts: 27.328125 MB
2026-02-12 11:04:45,708 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 11:04:45,710 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Next batch: 100,000:

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 100000
2026-02-12 11:17:18,710 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 11:17:18,807 INFO [logger.py]: Not clearing any queues.
2026-02-12 11:17:18,807 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-12 11:17:18,808 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-12 11:17:25,559 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 11:17:25,600 INFO [logger.py]: Current queue size: 402 items
2026-02-12 11:17:27,160 INFO [logger.py]: Loaded 401567 posts to classify.
2026-02-12 11:17:27,161 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-12 11:17:27,161 INFO [logger.py]: Latest inference timestamp: None
2026-02-12 11:17:27,366 INFO [logger.py]: After dropping duplicates, 401567 posts remain.
2026-02-12 11:17:27,726 INFO [logger.py]: After filtering, 394457 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 5 seconds
Memory usage for get_posts_to_classify: 863.59375 MB
2026-02-12 11:17:30,250 INFO [logger.py]: Limited posts from 394457 to 99312 (max_records_per_run=100000, included 101 complete batches)
2026-02-12 11:17:30,353 INFO [logger.py]: Classifying 99312 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/199 [00:00<?, ?batch/s]2026-02-12 11:17:31,422 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/199...

...

Classifying batches:  99%|██████████████████████████████████████████████████████████████████████████████████████▌| 198/199 [1:32:35<00:22, 22.91s/batch, successful=99000, failed=0]2026-02-12 12:50:22,700 INFO [logger.py]: Successfully labeled 312 posts.
2026-02-12 12:50:22,701 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 12:50:22,702 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 12:50:22,704 INFO [logger.py]: Current queue size: 301 items
2026-02-12 12:50:22,704 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 12:50:22,704 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 12:50:22,706 INFO [logger.py]: Current queue size: 396 items
2026-02-12 12:50:22,707 INFO [logger.py]: Adding 312 posts to the output queue.
2026-02-12 12:50:22,708 INFO [logger.py]: Writing 312 items as 1 minibatches to DB.
2026-02-12 12:50:22,708 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 12:50:22,708 INFO [logger.py]: Processing batch 1/1...
2026-02-12 12:50:22,711 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 12:50:22,712 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████████████████████████████████████████████| 199/199 [1:32:51<00:00, 28.00s/batch, successful=99312, failed=0]
Execution time for run_batch_classification: 92 minutes, 52 seconds
Memory usage for run_batch_classification: -225.625 MB
Execution time for orchestrate_classification: 93 minutes, 0 seconds
Memory usage for orchestrate_classification: 40.859375 MB
Execution time for classify_latest_posts: 93 minutes, 2 seconds
Memory usage for classify_latest_posts: 40.84375 MB
2026-02-12 12:50:25,864 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 12:50:25,867 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Next batch: 200,000:

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 200000
2026-02-12 12:53:57,953 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 12:53:58,087 INFO [logger.py]: Not clearing any queues.
2026-02-12 12:53:58,087 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-12 12:53:58,087 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-12 12:54:05,334 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 12:54:05,369 INFO [logger.py]: Current queue size: 301 items
2026-02-12 12:54:06,423 INFO [logger.py]: Loaded 300567 posts to classify.
2026-02-12 12:54:06,424 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-12 12:54:06,424 INFO [logger.py]: Latest inference timestamp: None
2026-02-12 12:54:06,580 INFO [logger.py]: After dropping duplicates, 300567 posts remain.
2026-02-12 12:54:06,854 INFO [logger.py]: After filtering, 295145 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 698.875 MB
2026-02-12 12:54:09,103 INFO [logger.py]: Limited posts from 295145 to 199435 (max_records_per_run=200000, included 203 complete batches)
2026-02-12 12:54:09,130 INFO [logger.py]: Classifying 199435 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/399 [00:00<?, ?batch/s]2026-02-12 12:54:10,206 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/399...
2026-02-12 12:54:30,557 INFO [logger.py]: Successfully labeled 500 posts.
2026-02-12 12:54:30,558 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 12:54:30,558 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 12:54:30,591 INFO [logger.py]: Current queue size: 301 items
2026-02-12 12:54:30,591 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 12:54:30,591 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 12:54:30,625 INFO [logger.py]: Current queue size: 397 items
2026-02-12 12:54:30,626 INFO [logger.py]: Adding 500 posts to the output queue.
2026-02-12 12:54:30,628 INFO [logger.py]: Writing 500 items as 1 minibatches to DB.
2026-02-12 12:54:30,628 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 12:54:30,629 INFO [logger.py]: Processing batch 1/1...
2026-02-12 12:54:30,631 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 12:54:30,632 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches:   0%|▏                                                                                          | 1/399 [00:20<2:15:30, 20.43s/batch, successful=500, failed=0]

...

2026-02-12 15:49:01,139 INFO [logger.py]: Adding 435 posts to the output queue.
2026-02-12 15:49:01,140 INFO [logger.py]: Writing 435 items as 1 minibatches to DB.
2026-02-12 15:49:01,140 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 15:49:01,140 INFO [logger.py]: Processing batch 1/1...
2026-02-12 15:49:01,143 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 15:49:01,143 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 399/399 [2:54:50<00:00, 26.29s/batch, successful=199435, failed=0]
Execution time for run_batch_classification: 174 minutes, 52 seconds
Memory usage for run_batch_classification: -814.609375 MB
Execution time for orchestrate_classification: 174 minutes, 59 seconds
Memory usage for orchestrate_classification: -199.234375 MB
Execution time for classify_latest_posts: 175 minutes, 1 seconds
Memory usage for classify_latest_posts: -199.21875 MB
2026-02-12 15:49:04,391 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 15:49:04,394 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Next batch, 120,000

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 120000
2026-02-12 15:56:43,836 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 15:56:43,943 INFO [logger.py]: Not clearing any queues.
2026-02-12 15:56:43,944 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-12 15:56:43,944 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-12 15:56:50,905 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 15:56:50,920 INFO [logger.py]: Current queue size: 98 items
2026-02-12 15:56:51,265 INFO [logger.py]: Loaded 97567 posts to classify.
2026-02-12 15:56:51,266 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-12 15:56:51,266 INFO [logger.py]: Latest inference timestamp: None
2026-02-12 15:56:51,313 INFO [logger.py]: After dropping duplicates, 97567 posts remain.
2026-02-12 15:56:51,383 INFO [logger.py]: After filtering, 95710 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 228.03125 MB
2026-02-12 15:56:52,829 INFO [logger.py]: Classifying 95710 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/192 [00:00<?, ?batch/s]2026-02-12 15:56:53,898 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/192...

...

2026-02-12 17:11:53,773 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 17:11:53,773 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 17:11:53,775 INFO [logger.py]: Current queue size: 0 items
2026-02-12 17:11:53,775 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 17:11:53,775 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 17:11:53,779 INFO [logger.py]: Current queue size: 987 items
2026-02-12 17:11:53,780 INFO [logger.py]: Adding 210 posts to the output queue.
2026-02-12 17:11:53,780 INFO [logger.py]: Writing 210 items as 1 minibatches to DB.
2026-02-12 17:11:53,780 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 17:11:53,780 INFO [logger.py]: Processing batch 1/1...
2026-02-12 17:11:53,782 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 17:11:53,783 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████████████████████████████████████████████| 192/192 [1:14:59<00:00, 23.44s/batch, successful=95710, failed=0]
Execution time for run_batch_classification: 75 minutes, 1 seconds
Memory usage for run_batch_classification: -125.78125 MB
Execution time for orchestrate_classification: 75 minutes, 6 seconds
Memory usage for orchestrate_classification: 53.421875 MB
Execution time for classify_latest_posts: 75 minutes, 8 seconds
Memory usage for classify_latest_posts: 53.46875 MB
2026-02-12 17:11:56,923 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 17:11:56,926 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Done!

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-12 17:41:28,843 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 17:41:28,956 INFO [logger.py]: Not clearing any queues.
2026-02-12 17:41:28,957 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 17:41:28,957 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 17:41:29,050 INFO [logger.py]: Current queue size: 988 items
2026-02-12 17:41:31,679 INFO [logger.py]: Exporting 492699 records to local storage for integration ml_inference_intergroup...
2026-02-12 17:41:32,454 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-07] Exporting n=1865 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:32,550 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:32,564 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-08] Exporting n=22035 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:32,590 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:32,633 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-09] Exporting n=64206 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:32,701 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:32,746 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-10] Exporting n=62981 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:32,813 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:32,866 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-11] Exporting n=71892 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:32,945 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:32,994 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-12] Exporting n=70104 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:33,072 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:33,127 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-13] Exporting n=78927 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:33,216 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:33,264 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-14] Exporting n=67342 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:33,340 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:33,379 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-15] Exporting n=53347 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-12 17:41:33,439 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-12 17:41:33,456 INFO [logger.py]: Finished exporting 492699 records to local storage for integration ml_inference_intergroup...
2026-02-12 17:41:33,475 INFO [logger.py]: Successfully wrote 492699 records to storage for integration ml_inference_intergroup
```

- [X] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-02-12 17:41:48,962 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 17:41:49,078 INFO [logger.py]: Not clearing any queues.
2026-02-12 17:41:49,099 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-02-12 17:41:49,103 INFO [logger.py]: Registered 0 new files for migration (0 already in tracker).
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-12 17:41:49,123 INFO [logger.py]: Registered 9 new files for migration (181 already in tracker).                                                                             
Initialized migration tracker db for ml_inference_intergroup/cache (190 files)
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 98.03it/s]
Finished initializing migration tracker db
2026-02-12 17:41:49,126 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]2026-02-12 17:41:49,189 INFO [logger.py]: Migrating 0 file(s) for ml_inference_intergroup/active
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]
2026-02-12 17:41:49,192 INFO [logger.py]: Migrating 9 file(s) for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-12 17:41:49,193 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/efd64169c1ff4499bc7d120e316fac29-0.parquet
2026-02-12 17:41:49,193 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/efd64169c1ff4499bc7d120e316fac29-0.parquet to S3 (7.26 MB)
2026-02-12 17:41:52,015 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/efd64169c1ff4499bc7d120e316fac29-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-11/efd64169c1ff4499bc7d120e316fac29-0.parquet
2026-02-12 17:41:52,018 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/efd64169c1ff4499bc7d120e316fac29-0.parquet
                                                                                                                                                                                   2026-02-12 17:41:52,019 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/9efbebc95419440a833a78f9e4f3c512-0.parquet
2026-02-12 17:41:52,019 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/9efbebc95419440a833a78f9e4f3c512-0.parquet to S3 (6.35 MB)
2026-02-12 17:41:54,093 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/9efbebc95419440a833a78f9e4f3c512-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-10/9efbebc95419440a833a78f9e4f3c512-0.parquet
2026-02-12 17:41:54,095 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/9efbebc95419440a833a78f9e4f3c512-0.parquet
                                                                                                                                                                                   2026-02-12 17:41:54,096 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/27dab896d6234272907fa68cd94f0ca2-0.parquet
2026-02-12 17:41:54,096 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/27dab896d6234272907fa68cd94f0ca2-0.parquet to S3 (7.03 MB)
2026-02-12 17:41:59,635 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/27dab896d6234272907fa68cd94f0ca2-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-12/27dab896d6234272907fa68cd94f0ca2-0.parquet
2026-02-12 17:41:59,640 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/27dab896d6234272907fa68cd94f0ca2-0.parquet
                                                                                                                                                                                   2026-02-12 17:41:59,644 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/fd74ee89b3d64289a96ce1bcd366df65-0.parquet
2026-02-12 17:41:59,645 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/fd74ee89b3d64289a96ce1bcd366df65-0.parquet to S3 (5.07 MB)
2026-02-12 17:42:02,198 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/fd74ee89b3d64289a96ce1bcd366df65-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-15/fd74ee89b3d64289a96ce1bcd366df65-0.parquet
2026-02-12 17:42:02,204 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/fd74ee89b3d64289a96ce1bcd366df65-0.parquet
                                                                                                                                                                                   2026-02-12 17:42:02,209 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/f061cb981bff4a3c9a3743366af6f53b-0.parquet
2026-02-12 17:42:02,209 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/f061cb981bff4a3c9a3743366af6f53b-0.parquet to S3 (6.42 MB)
2026-02-12 17:42:04,533 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/f061cb981bff4a3c9a3743366af6f53b-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-14/f061cb981bff4a3c9a3743366af6f53b-0.parquet
2026-02-12 17:42:04,539 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/f061cb981bff4a3c9a3743366af6f53b-0.parquet
                                                                                                                                                                                   2026-02-12 17:42:04,543 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/c865c35178d64d5397d6fe93d94aaeb5-0.parquet
2026-02-12 17:42:04,544 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/c865c35178d64d5397d6fe93d94aaeb5-0.parquet to S3 (7.89 MB)
2026-02-12 17:42:07,686 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/c865c35178d64d5397d6fe93d94aaeb5-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-13/c865c35178d64d5397d6fe93d94aaeb5-0.parquet
2026-02-12 17:42:07,691 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/c865c35178d64d5397d6fe93d94aaeb5-0.parquet
                                                                                                                                                                                   2026-02-12 17:42:07,696 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/0d2e2ae60de64359800eb684124147d4-0.parquet
2026-02-12 17:42:07,696 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/0d2e2ae60de64359800eb684124147d4-0.parquet to S3 (6.28 MB)
2026-02-12 17:42:10,227 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/0d2e2ae60de64359800eb684124147d4-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-09/0d2e2ae60de64359800eb684124147d4-0.parquet
2026-02-12 17:42:10,232 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/0d2e2ae60de64359800eb684124147d4-0.parquet
                                                                                                                                                                                   2026-02-12 17:42:10,237 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/12adcc19adfe451097e0c9cfa7923a71-0.parquet
2026-02-12 17:42:10,238 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/12adcc19adfe451097e0c9cfa7923a71-0.parquet to S3 (0.21 MB)
2026-02-12 17:42:10,371 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/12adcc19adfe451097e0c9cfa7923a71-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-07/12adcc19adfe451097e0c9cfa7923a71-0.parquet
2026-02-12 17:42:10,374 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/12adcc19adfe451097e0c9cfa7923a71-0.parquet
                                                                                                                                                                                   2026-02-12 17:42:10,376 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/8121ce432480440593e48c6ce2db8e5b-0.parquet
2026-02-12 17:42:10,377 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/8121ce432480440593e48c6ce2db8e5b-0.parquet to S3 (2.30 MB)
2026-02-12 17:42:11,371 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/8121ce432480440593e48c6ce2db8e5b-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-08/8121ce432480440593e48c6ce2db8e5b-0.parquet
2026-02-12 17:42:11,376 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/8121ce432480440593e48c6ce2db8e5b-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████| 9/9 [00:22<00:00,  2.47s/it]
2026-02-12 17:42:11,379 INFO [logger.py]: Migrated 9 files for ml_inference_intergroup/cache: 9 succeeded, 0 failed.██████████████████████████████████| 9/9 [00:22<00:00,  1.69s/it]
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:22<00:00, 11.10s/it]
```

- [X] **Validate in Athena**

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

```
#	label	_col1
1	-1	2
2	0	2167807
3	1	213616
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
) t
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
6	2024-10-04	915
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	28525
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	47074
16	2024-10-16	47541
17	2024-10-17	4160
18	2024-10-18	11843
19	2024-10-19	41765
20	2024-10-20	70941
21	2024-10-21	71065
22	2024-10-22	73140
23	2024-10-23	84596
24	2024-10-24	71993
25	2024-10-25	72545
26	2024-10-26	62379
27	2024-10-27	59806
28	2024-10-28	47483
29	2024-10-29	68359
30	2024-10-30	64191
31	2024-10-31	73458
32	2024-11-01	65812
33	2024-11-02	52408
34	2024-11-03	62927
35	2024-11-04	62562
36	2024-11-05	65707
37	2024-11-06	83342
38	2024-11-07	57045
39	2024-11-08	82324
40	2024-11-09	71323
41	2024-11-10	70005
42	2024-11-11	79875
43	2024-11-12	77872
44	2024-11-13	87764
45	2024-11-14	74964
46	2024-11-15	64195
47	2024-11-16	7673
48	2024-11-17	4113
49	2024-11-18	1152
50	2024-11-19	8199
51	2024-11-20	431
52	2024-11-21	6851
53	2024-11-22	10422
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash

```

- [X] **Delete input and output queues**

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 8: 2024-11-16 to 2024-11-22

- [X] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-16 --end-date 2024-11-22 --source-data-location s3`

```bash
2026-02-12 18:20:19,547 INFO [logger.py]: Enqueuing 377643 posts for integration ml_inference_intergroup (after dedup and filtering previously labeled).
2026-02-12 18:20:19,547 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 18:20:20,048 INFO [logger.py]: Writing 377643 items as 378 minibatches to DB.
2026-02-12 18:20:20,048 INFO [logger.py]: Writing 378 minibatches to DB as 16 batches...
2026-02-12 18:20:20,048 INFO [logger.py]: Processing batch 1/16...
2026-02-12 18:20:20,279 INFO [logger.py]: Processing batch 11/16...
2026-02-12 18:20:20,424 INFO [logger.py]: Inserted 377643 posts into queue for integration: ml_inference_intergroup
2026-02-12 18:20:20,445 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-02-12 18:20:20,445 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

I'll do this in batches. I'll start with a batch of 120,000.

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 120000
2026-02-12 18:35:00,921 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 18:35:01,023 INFO [logger.py]: Not clearing any queues.
2026-02-12 18:35:01,023 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-12 18:35:01,023 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-12 18:35:08,040 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 18:35:08,079 INFO [logger.py]: Current queue size: 378 items
2026-02-12 18:35:09,392 INFO [logger.py]: Loaded 377643 posts to classify.
2026-02-12 18:35:09,393 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-12 18:35:09,393 INFO [logger.py]: Latest inference timestamp: None
2026-02-12 18:35:09,584 INFO [logger.py]: After dropping duplicates, 377643 posts remain.
2026-02-12 18:35:09,921 INFO [logger.py]: After filtering, 370707 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 4 seconds
Memory usage for get_posts_to_classify: 891.140625 MB
2026-02-12 18:35:12,310 INFO [logger.py]: Limited posts from 370707 to 119741 (max_records_per_run=120000, included 122 complete batches)
2026-02-12 18:35:12,364 INFO [logger.py]: Classifying 119741 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/240 [00:00<?, ?batch/s]2026-02-12 18:35:13,445 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/240...
2026-02-12 18:35:20,657 INFO [_base_client.py]: Retrying request to /chat/completions in 0.449532 seconds
Retrying request to /chat/completions in 0.449532 seconds
2026-02-12 18:35:36,728 INFO [logger.py]: Successfully labeled 500 posts.
2026-02-12 18:35:36,729 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-12 18:35:36,729 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 18:35:36,730 INFO [logger.py]: Current queue size: 378 items
2026-02-12 18:35:36,730 INFO [logger.py]: Creating new SQLite DB for queue output_ml_inference_intergroup...
2026-02-12 18:35:36,733 INFO [logger.py]: Adding 500 posts to the output queue.
2026-02-12 18:35:36,735 INFO [logger.py]: Writing 500 items as 1 minibatches to DB.
2026-02-12 18:35:36,735 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 18:35:36,736 INFO [logger.py]: Processing batch 1/1...
2026-02-12 18:35:36,737 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 18:35:36,738 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches:   0%|▍                                                                                          | 1/240 [00:23<1:32:47, 23.29s/batch, successful=500, failed=0]

...

2026-02-12 20:22:07,698 INFO [logger.py]: Adding 241 posts to the output queue.
2026-02-12 20:22:07,699 INFO [logger.py]: Writing 241 items as 1 minibatches to DB.
2026-02-12 20:22:07,699 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-12 20:22:07,699 INFO [logger.py]: Processing batch 1/1...
2026-02-12 20:22:07,700 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-12 20:22:07,701 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 240/240 [1:46:54<00:00, 26.73s/batch, successful=119741, failed=0]
Execution time for run_batch_classification: 106 minutes, 55 seconds
Memory usage for run_batch_classification: -226.015625 MB
Execution time for orchestrate_classification: 107 minutes, 3 seconds
Memory usage for orchestrate_classification: 110.734375 MB
Execution time for classify_latest_posts: 107 minutes, 5 seconds
Memory usage for classify_latest_posts: 110.734375 MB
2026-02-12 20:22:10,932 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-12 20:22:10,935 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Now I'll do the remaining 250,000 overnight

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 300000
2026-02-12 21:08:44,948 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-12 21:08:45,052 INFO [logger.py]: Not clearing any queues.
2026-02-12 21:08:45,052 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-02-12 21:08:45,052 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-02-12 21:08:52,219 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-02-12 21:08:52,251 INFO [logger.py]: Current queue size: 256 items
2026-02-12 21:08:53,102 INFO [logger.py]: Loaded 255643 posts to classify.
2026-02-12 21:08:53,103 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-02-12 21:08:53,103 INFO [logger.py]: Latest inference timestamp: None
2026-02-12 21:08:53,228 INFO [logger.py]: After dropping duplicates, 255643 posts remain.
2026-02-12 21:08:53,466 INFO [logger.py]: After filtering, 250966 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 3 seconds
Memory usage for get_posts_to_classify: 582.640625 MB
2026-02-12 21:08:55,368 INFO [logger.py]: Classifying 250966 posts with intergroup classifier...
Classifying batches:   0%|                                                                                                                               | 0/502 [00:00<?, ?batch/s]2026-02-12 21:08:56,437 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/502...

...

2026-02-13 00:13:46,129 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-13 00:13:46,129 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-13 00:13:46,131 INFO [logger.py]: Current queue size: 741 items
2026-02-13 00:13:46,131 INFO [logger.py]: Adding 466 posts to the output queue.
2026-02-13 00:13:46,133 INFO [logger.py]: Writing 466 items as 1 minibatches to DB.
2026-02-13 00:13:46,133 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-02-13 00:13:46,133 INFO [logger.py]: Processing batch 1/1...
2026-02-13 00:13:46,135 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-02-13 00:13:46,136 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████████████████████████████████████████████| 502/502 [3:04:49<00:00, 22.09s/batch, successful=250966, failed=0]
Execution time for run_batch_classification: 184 minutes, 51 seconds
Memory usage for run_batch_classification: -72.4375 MB
Execution time for orchestrate_classification: 184 minutes, 57 seconds
Memory usage for orchestrate_classification: 244.234375 MB
Execution time for classify_latest_posts: 184 minutes, 59 seconds
Memory usage for classify_latest_posts: 244.234375 MB
2026-02-13 00:13:49,382 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-02-13 00:13:49,383 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-02-13 08:00:46,175 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-13 08:00:46,275 INFO [logger.py]: Not clearing any queues.
2026-02-13 08:00:46,276 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-02-13 08:00:46,276 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-02-13 08:00:46,344 INFO [logger.py]: Current queue size: 742 items
2026-02-13 08:00:48,310 INFO [logger.py]: Exporting 370707 records to local storage for integration ml_inference_intergroup...
2026-02-13 08:00:48,894 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-14] Exporting n=2658 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:48,959 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:48,990 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-15] Exporting n=47721 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,045 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,091 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-16] Exporting n=68108 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,163 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,190 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-17] Exporting n=37719 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,230 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,239 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-18] Exporting n=10167 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,253 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,299 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-19] Exporting n=74904 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,377 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,383 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-20] Exporting n=3862 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,391 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,429 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-21] Exporting n=59472 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,492 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,538 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-22] Exporting n=66096 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-02-13 08:00:49,610 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-02-13 08:00:49,624 INFO [logger.py]: Finished exporting 370707 records to local storage for integration ml_inference_intergroup...
2026-02-13 08:00:49,638 INFO [logger.py]: Successfully wrote 370707 records to storage for integration ml_inference_intergroup
```

- [X] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(intergroup-backfills-v3) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-02-13 08:01:10,857 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-02-13 08:01:10,919 INFO [logger.py]: Not clearing any queues.
2026-02-13 08:01:10,939 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-02-13 08:01:10,944 INFO [logger.py]: Registered 0 new files for migration (0 already in tracker).
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-13 08:01:10,965 INFO [logger.py]: Registered 9 new files for migration (190 already in tracker).                                                                             
Initialized migration tracker db for ml_inference_intergroup/cache (199 files)
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 92.29it/s]
Finished initializing migration tracker db
2026-02-13 08:01:10,968 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Processing prefixes:   0%|                                                                                                                                    | 0/2 [00:00<?, ?it/s]2026-02-13 08:01:11,030 INFO [logger.py]: Migrating 0 file(s) for ml_inference_intergroup/active
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]
2026-02-13 08:01:11,031 INFO [logger.py]: Migrating 9 file(s) for ml_inference_intergroup/cache
                                                                                                                                                                                   2026-02-13 08:01:11,032 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/25eaad2ed4284d828345f3040b9e5da7-0.parquet
2026-02-13 08:01:11,032 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/25eaad2ed4284d828345f3040b9e5da7-0.parquet to S3 (6.48 MB)
2026-02-13 08:01:18,171 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/25eaad2ed4284d828345f3040b9e5da7-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-16/25eaad2ed4284d828345f3040b9e5da7-0.parquet
2026-02-13 08:01:18,174 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/25eaad2ed4284d828345f3040b9e5da7-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:18,177 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/463b694c218645a2822aa43d0de321ae-0.parquet
2026-02-13 08:01:18,177 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/463b694c218645a2822aa43d0de321ae-0.parquet to S3 (1.04 MB)
2026-02-13 08:01:18,662 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/463b694c218645a2822aa43d0de321ae-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-18/463b694c218645a2822aa43d0de321ae-0.parquet
2026-02-13 08:01:18,664 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/463b694c218645a2822aa43d0de321ae-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:18,667 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/263a84ad6f4e4e36afd16c18e27d6377-0.parquet
2026-02-13 08:01:18,667 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/263a84ad6f4e4e36afd16c18e27d6377-0.parquet to S3 (0.42 MB)
2026-02-13 08:01:18,942 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/263a84ad6f4e4e36afd16c18e27d6377-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-20/263a84ad6f4e4e36afd16c18e27d6377-0.parquet
2026-02-13 08:01:18,944 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/263a84ad6f4e4e36afd16c18e27d6377-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:18,946 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d482a9d687bc450997ed3a7b7b04f0fb-0.parquet
2026-02-13 08:01:18,946 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d482a9d687bc450997ed3a7b7b04f0fb-0.parquet to S3 (5.83 MB)
2026-02-13 08:01:20,679 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d482a9d687bc450997ed3a7b7b04f0fb-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d482a9d687bc450997ed3a7b7b04f0fb-0.parquet
2026-02-13 08:01:20,680 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d482a9d687bc450997ed3a7b7b04f0fb-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:20,681 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/c8f6ace212cb435ea74e51bed7cdd6f3-0.parquet
2026-02-13 08:01:20,681 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/c8f6ace212cb435ea74e51bed7cdd6f3-0.parquet to S3 (7.32 MB)
2026-02-13 08:01:22,428 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/c8f6ace212cb435ea74e51bed7cdd6f3-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-19/c8f6ace212cb435ea74e51bed7cdd6f3-0.parquet
2026-02-13 08:01:22,431 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/c8f6ace212cb435ea74e51bed7cdd6f3-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:22,434 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/aefd02b3ffc54c9c83f00f4ababd6cbb-0.parquet
2026-02-13 08:01:22,435 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/aefd02b3ffc54c9c83f00f4ababd6cbb-0.parquet to S3 (3.52 MB)
2026-02-13 08:01:27,379 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/aefd02b3ffc54c9c83f00f4ababd6cbb-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-17/aefd02b3ffc54c9c83f00f4ababd6cbb-0.parquet
2026-02-13 08:01:27,381 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/aefd02b3ffc54c9c83f00f4ababd6cbb-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:27,384 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/9789d2eabea34ae9bdd0e04b3f87333d-0.parquet
2026-02-13 08:01:27,384 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/9789d2eabea34ae9bdd0e04b3f87333d-0.parquet to S3 (4.59 MB)
2026-02-13 08:01:29,431 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/9789d2eabea34ae9bdd0e04b3f87333d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-15/9789d2eabea34ae9bdd0e04b3f87333d-0.parquet
2026-02-13 08:01:29,433 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/9789d2eabea34ae9bdd0e04b3f87333d-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:29,437 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/a14c2788ed694cfeba5a80e63776b2f5-0.parquet
2026-02-13 08:01:29,438 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/a14c2788ed694cfeba5a80e63776b2f5-0.parquet to S3 (6.40 MB)
2026-02-13 08:01:31,738 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/a14c2788ed694cfeba5a80e63776b2f5-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-22/a14c2788ed694cfeba5a80e63776b2f5-0.parquet
2026-02-13 08:01:31,740 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/a14c2788ed694cfeba5a80e63776b2f5-0.parquet
                                                                                                                                                                                   2026-02-13 08:01:31,749 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/8fa4d98c38cb4869be1e9d5f4bb317d1-0.parquet
2026-02-13 08:01:31,749 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/8fa4d98c38cb4869be1e9d5f4bb317d1-0.parquet to S3 (0.29 MB)
2026-02-13 08:01:31,846 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/8fa4d98c38cb4869be1e9d5f4bb317d1-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-14/8fa4d98c38cb4869be1e9d5f4bb317d1-0.parquet
2026-02-13 08:01:31,848 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/8fa4d98c38cb4869be1e9d5f4bb317d1-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████| 9/9 [00:20<00:00,  2.31s/it]
2026-02-13 08:01:31,849 INFO [logger.py]: Migrated 9 files for ml_inference_intergroup/cache: 9 succeeded, 0 failed.██████████████████████████████████| 9/9 [00:20<00:00,  1.76s/it]
Processing prefixes: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:20<00:00, 10.41s/it]
```

- [ ] **Validate in Athena**

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
1	-1	2
2	0	2493700
3	1	243241
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM (
  SELECT DISTINCT label, uri, partition_date
  FROM archive_ml_inference_intergroup
) t
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
6	2024-10-04	915
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	28525
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72989
15	2024-10-15	47074
16	2024-10-16	47541
17	2024-10-17	4160
18	2024-10-18	11843
19	2024-10-19	41765
20	2024-10-20	70941
21	2024-10-21	71065
22	2024-10-22	73140
23	2024-10-23	84596
24	2024-10-24	71993
25	2024-10-25	72545
26	2024-10-26	62379
27	2024-10-27	59806
28	2024-10-28	47483
29	2024-10-29	68359
30	2024-10-30	64191
31	2024-10-31	73458
32	2024-11-01	65812
33	2024-11-02	52408
34	2024-11-03	62927
35	2024-11-04	62562
36	2024-11-05	65707
37	2024-11-06	83342
38	2024-11-07	57045
39	2024-11-08	82324
40	2024-11-09	71323
41	2024-11-10	70005
42	2024-11-11	79875
43	2024-11-12	77872
44	2024-11-13	87764
45	2024-11-14	75076
46	2024-11-15	99306
47	2024-11-16	75781
48	2024-11-17	41832
49	2024-11-18	11319
50	2024-11-19	83103
51	2024-11-20	4293
52	2024-11-21	66323
53	2024-11-22	76518
54	2024-11-23	1797
55	2024-11-24	10
56	2024-11-25	3698
57	2024-11-26	11375
58	2024-11-27	6821
59	2024-11-28	8429
60	2024-11-29	9819
61	2024-11-30	8958
62	2024-12-01	5126
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [X] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
0
(.venv) (base) ➜  queue sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
742
```

- [ ] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 9: 2024-11-23 to 2024-11-29

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-23 --end-date 2024-11-29 --source-data-location s3`

```bash

```

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

```bash

```

- [ ] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash

```

- [ ] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash

```

- [ ] **Validate in Athena**

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
ORDER BY 1 ASC;
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [ ] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash

```

- [ ] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```

---

## Week 10: 2024-11-30 to 2024-12-01

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-30 --end-date 2024-12-01 --source-data-location s3 --sample-records --sample-proportion 0.10`

```bash

```

- [ ] **Running integrations**
  - `uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 50000`

```bash

```

- [ ] **Write cache to storage**
  - `uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash

```

- [ ] **Migrate to S3**
  - `uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash

```

- [ ] **Validate in Athena**

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
ORDER BY 1 ASC;
```

Numbers check out compared to the total URIs:

```sql
SELECT
  earliest_partition_date AS partition_date,
  COUNT(uri) AS uri_count
FROM (
  SELECT
    uri,
    partition_date,
    MIN(partition_date) OVER (PARTITION BY uri) AS earliest_partition_date
  FROM archive_fetch_posts_used_in_feeds
) t
WHERE partition_date = earliest_partition_date
GROUP BY earliest_partition_date
ORDER BY earliest_partition_date ASC;
```

```bash

```

- [ ] **Look at integration queues**

```bash
sqlite3 input_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
sqlite3 output_ml_inference_intergroup.db "SELECT COUNT(*) FROM queue;"
```

```bash

```

- [ ] **Delete input and output queues** (optional, after validation)

```bash
rm input_ml_inference_intergroup.db
rm output_ml_inference_intergroup.db
```
