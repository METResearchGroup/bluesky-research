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

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-12 --end-date 2024-10-18 --source-data-location s3`

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

## Week 4: 2024-10-19 to 2024-10-25

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-19 --end-date 2024-10-25 --source-data-location s3 --sample-records --sample-proportion 0.10`

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

## Week 5: 2024-10-26 to 2024-11-01

- [ ] **Enqueueing**
  - `uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3 --sample-records --sample-proportion 0.10`

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
