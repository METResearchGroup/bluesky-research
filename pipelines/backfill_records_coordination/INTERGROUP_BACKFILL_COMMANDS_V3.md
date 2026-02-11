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

- [ ] **Look at integration queues**

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

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3`

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

## Week 6: 2024-11-02 to 2024-11-08

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3 --sample-records --sample-proportion 0.10`

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

## Week 7: 2024-11-09 to 2024-11-15

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-09 --end-date 2024-11-15 --source-data-location s3 --sample-records --sample-proportion 0.10`

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

## Week 8: 2024-11-16 to 2024-11-22

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-16 --end-date 2024-11-22 --source-data-location s3 --sample-records --sample-proportion 0.10`

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

## Week 9: 2024-11-23 to 2024-11-29

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-23 --end-date 2024-11-29 --source-data-location s3 --sample-records --sample-proportion 0.10`

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
