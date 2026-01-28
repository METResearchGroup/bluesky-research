# Commands to run for intergroup backfill

This runbook chunks the backfill into weekly windows. For each week:
- Enqueue posts for `ml_inference_intergroup` from S3 into the input queue.
- Run the intergroup integration to process the queue.
- Write the output queue (“cache buffer”) to permanent storage.

Notes:
- `--source-data-location s3` applies to the enqueue step (reading posts + label history).
- The “write cache to storage” step uses the existing `CacheBufferWriterService` behavior.


## Week 1: 2024-09-28 to 2024-10-04

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-09-28 --end-date 2024-10-04 --source-data-location s3`

```
2026-01-22 15:58:06,867 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 15:58:06,904 INFO [logger.py]: Writing 16678 items as 17 minibatches to DB.
2026-01-22 15:58:06,904 INFO [logger.py]: Writing 17 minibatches to DB as 1 batches...
2026-01-22 15:58:06,904 INFO [logger.py]: Processing batch 1/1...
2026-01-22 15:58:06,924 INFO [logger.py]: Inserted 16678 posts into queue for integration: ml_inference_intergroup
2026-01-22 15:58:06,945 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-22 15:58:06,946 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 2500`

Iteration 1:

```bash
Classifying batches: 100%|██████████████████████████████████████████████| 99/99 [14:24<00:00,  8.73s/batch, successful=1966, failed=0]
Execution time for run_batch_classification: 14 minutes, 26 seconds
Memory usage for run_batch_classification: -416.046875 MB
Execution time for orchestrate_classification: 14 minutes, 30 seconds
Memory usage for orchestrate_classification: -261.40625 MB
Execution time for classify_latest_posts: 14 minutes, 32 seconds
Memory usage for classify_latest_posts: -394.09375 MB
2026-01-22 16:13:24,774 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 16:13:24,775 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 2 (ran with 1200):

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 1200
2026-01-22 16:22:43,423 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 16:22:43,542 INFO [logger.py]: Not clearing any queues.
2026-01-22 16:22:43,542 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 16:22:43,543 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 16:22:50,908 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 16:22:50,917 INFO [logger.py]: Current queue size: 14 items
2026-01-22 16:22:50,971 INFO [logger.py]: Loaded 13678 posts to classify.
2026-01-22 16:22:50,971 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 16:22:50,971 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 16:22:50,982 INFO [logger.py]: After dropping duplicates, 13678 posts remain.
2026-01-22 16:22:50,995 INFO [logger.py]: After filtering, 13508 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 33.8125 MB
2026-01-22 16:22:52,180 INFO [logger.py]: Limited posts from 13508 to 987 (max_records_per_run=1200, included 1 complete batches)
2026-01-22 16:22:52,183 INFO [logger.py]: Classifying 987 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/50 [00:00<?, ?batch/s]2026-01-22 16:22:53,238 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/50...
2026-01-22 16:22:59,869 INFO [logger.py]: Successfully labeled 20 posts.
...
2026-01-22 16:29:31,602 INFO [logger.py]: Current queue size: 151 items
2026-01-22 16:29:31,602 INFO [logger.py]: Adding 7 posts to the output queue.
2026-01-22 16:29:31,603 INFO [logger.py]: Writing 7 items as 1 minibatches to DB.
2026-01-22 16:29:31,603 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 16:29:31,603 INFO [logger.py]: Processing batch 1/1...
2026-01-22 16:29:31,606 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 16:29:31,608 INFO [logger.py]: Deleted 0 items from queue.
2026-01-22 16:29:31,608 INFO [logger.py]: Total items before delete: 13 Total items after delete: 13
Classifying batches: 100%|███████████████████████████████████████████████| 50/50 [06:38<00:00,  7.97s/batch, successful=987, failed=0]
Execution time for run_batch_classification: 6 minutes, 39 seconds
Memory usage for run_batch_classification: -483.734375 MB
Execution time for orchestrate_classification: 6 minutes, 44 seconds
Memory usage for orchestrate_classification: -471.9375 MB
Execution time for classify_latest_posts: 6 minutes, 46 seconds
Memory usage for classify_latest_posts: -473.609375 MB
2026-01-22 16:29:34,735 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 16:29:34,737 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 3:

```bash
2026-01-22 16:53:06,573 INFO [logger.py]: Total items before delete: 12 Total items after delete: 12
Classifying batches: 100%|███████████████████████████████████████████████| 50/50 [06:47<00:00,  8.14s/batch, successful=990, failed=0]
Execution time for run_batch_classification: 6 minutes, 48 seconds
Memory usage for run_batch_classification: -503.421875 MB
Execution time for orchestrate_classification: 6 minutes, 53 seconds
Memory usage for orchestrate_classification: -481.609375 MB
Execution time for classify_latest_posts: 6 minutes, 55 seconds
Memory usage for classify_latest_posts: -490.1875 MB
2026-01-22 16:53:09,697 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 16:53:09,698 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 4:

```bash
2026-01-22 17:00:55,910 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 17:00:55,912 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|███████████████████████████████████████████████| 50/50 [05:51<00:00,  7.02s/batch, successful=988, failed=0]
Execution time for run_batch_classification: 5 minutes, 52 seconds
Memory usage for run_batch_classification: -420.328125 MB
Execution time for orchestrate_classification: 5 minutes, 57 seconds
Memory usage for orchestrate_classification: -451.3125 MB
Execution time for classify_latest_posts: 5 minutes, 59 seconds
Memory usage for classify_latest_posts: -452.421875 MB
2026-01-22 17:00:59,006 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 17:00:59,007 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Figured out that there is a bug affecting incomplete jobs. I'll work on that in a worktree while I do this here. [PR here](https://github.com/METResearchGroup/bluesky-research/pull/367)

Iteration 5: upp-ed to 3000 for max total

```bash
2026-01-22 17:31:07,945 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 17:31:07,948 INFO [logger.py]: Current queue size: 399 items
2026-01-22 17:31:07,948 INFO [logger.py]: Adding 18 posts to the output queue.
2026-01-22 17:31:07,948 INFO [logger.py]: Writing 18 items as 1 minibatches to DB.
2026-01-22 17:31:07,948 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 17:31:07,949 INFO [logger.py]: Processing batch 1/1...
2026-01-22 17:31:07,951 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 17:31:07,952 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 148/148 [26:57<00:00, 10.93s/batch, successful=2958, failed=0]
Execution time for run_batch_classification: 26 minutes, 58 seconds
Memory usage for run_batch_classification: -419.171875 MB
Execution time for orchestrate_classification: 27 minutes, 3 seconds
Memory usage for orchestrate_classification: -461.53125 MB
Execution time for classify_latest_posts: 27 minutes, 5 seconds
Memory usage for classify_latest_posts: -462.25 MB
2026-01-22 17:31:11,070 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 17:31:11,072 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 6: at this rate, probably another 1-1.5 hours (looks like ~6000-7000 posts per hour)

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 3000
2026-01-22 17:33:37,940 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 17:33:38,062 INFO [logger.py]: Not clearing any queues.
2026-01-22 17:33:38,063 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 17:33:38,063 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 17:33:45,698 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 17:33:45,709 INFO [logger.py]: Current queue size: 8 items
2026-01-22 17:33:45,746 INFO [logger.py]: Loaded 7678 posts to classify.
2026-01-22 17:33:45,747 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 17:33:45,747 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 17:33:45,764 INFO [logger.py]: After dropping duplicates, 7678 posts remain.
2026-01-22 17:33:45,778 INFO [logger.py]: After filtering, 7585 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: -13.1875 MB
2026-01-22 17:33:46,834 INFO [logger.py]: Limited posts from 7585 to 2967 (max_records_per_run=3000, included 3 complete batches)
2026-01-22 17:33:46,836 INFO [logger.py]: Classifying 2967 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/149 [00:00<?, ?batch/s]2026-01-22 17:33:47,899 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/149...
2026-01-22 17:33:57,703 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-22 17:33:57,704 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 17:33:57,704 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 17:33:57,706 INFO [logger.py]: Current queue size: 8 items
2026-01-22 17:33:57,706 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 17:33:57,706 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 17:33:57,718 INFO [logger.py]: Current queue size: 400 items
2026-01-22 17:33:57,718 INFO [logger.py]: Adding 20 posts to the output queue.
2026-01-22 17:33:57,719 INFO [logger.py]: Writing 20 items as 1 minibatches to DB.
2026-01-22 17:33:57,719 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 17:33:57,719 INFO [logger.py]: Processing batch 1/1...
2026-01-22 17:33:57,722 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
...
2026-01-22 17:51:54,513 INFO [logger.py]: Adding 7 posts to the output queue.
2026-01-22 17:51:54,513 INFO [logger.py]: Writing 7 items as 1 minibatches to DB.
2026-01-22 17:51:54,514 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 17:51:54,514 INFO [logger.py]: Processing batch 1/1...
2026-01-22 17:51:54,516 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 17:51:54,517 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 149/149 [18:06<00:00,  7.29s/batch, successful=2967, failed=0]
Execution time for run_batch_classification: 18 minutes, 8 seconds
Memory usage for run_batch_classification: -415.359375 MB
Execution time for orchestrate_classification: 18 minutes, 12 seconds
Memory usage for orchestrate_classification: -437.921875 MB
Execution time for classify_latest_posts: 18 minutes, 14 seconds
Memory usage for classify_latest_posts: -438.125 MB
2026-01-22 17:51:57,640 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 17:51:57,642 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 7:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 3000
2026-01-22 19:07:08,207 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 19:07:08,331 INFO [logger.py]: Not clearing any queues.
2026-01-22 19:07:08,332 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 19:07:08,332 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 19:07:19,125 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 19:07:19,134 INFO [logger.py]: Current queue size: 5 items
2026-01-22 19:07:19,162 INFO [logger.py]: Loaded 4678 posts to classify.
2026-01-22 19:07:19,165 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 19:07:19,165 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 19:07:19,186 INFO [logger.py]: After dropping duplicates, 4678 posts remain.
2026-01-22 19:07:19,217 INFO [logger.py]: After filtering, 4618 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: -91.578125 MB
2026-01-22 19:07:20,270 INFO [logger.py]: Limited posts from 4618 to 2959 (max_records_per_run=3000, included 3 complete batches)
2026-01-22 19:07:20,271 INFO [logger.py]: Classifying 2959 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/148 [00:00<?, ?batch/s]2026-01-22 19:07:21,331 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/148...
2026-01-22 19:07:31,359 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-22 19:07:31,360 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 19:07:31,360 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 19:07:31,361 INFO [logger.py]: Current queue size: 5 items
2026-01-22 19:07:31,361 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
...
01-22 19:25:05,708 INFO [logger.py]: Current queue size: 696 items
2026-01-22 19:25:05,708 INFO [logger.py]: Adding 19 posts to the output queue.
2026-01-22 19:25:05,708 INFO [logger.py]: Writing 19 items as 1 minibatches to DB.
2026-01-22 19:25:05,708 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 19:25:05,708 INFO [logger.py]: Processing batch 1/1...
2026-01-22 19:25:05,709 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 19:25:05,710 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 148/148 [17:44<00:00,  7.19s/batch, successful=2959, failed=0]
Execution time for run_batch_classification: 17 minutes, 45 seconds
Memory usage for run_batch_classification: -367.296875 MB
Execution time for orchestrate_classification: 17 minutes, 50 seconds
Memory usage for orchestrate_classification: -496.046875 MB
Execution time for classify_latest_posts: 17 minutes, 52 seconds
Memory usage for classify_latest_posts: -496.015625 MB
2026-01-22 19:25:08,815 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 19:25:08,816 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 8:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 3000
2026-01-22 19:50:16,337 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 19:50:16,471 INFO [logger.py]: Not clearing any queues.
2026-01-22 19:50:16,472 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 19:50:16,472 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 19:50:24,909 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 19:50:24,919 INFO [logger.py]: Current queue size: 2 items
2026-01-22 19:50:24,929 INFO [logger.py]: Loaded 1678 posts to classify.
2026-01-22 19:50:24,930 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 19:50:24,930 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 19:50:24,936 INFO [logger.py]: After dropping duplicates, 1678 posts remain.
2026-01-22 19:50:24,943 INFO [logger.py]: After filtering, 1659 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 7.484375 MB
2026-01-22 19:50:25,987 INFO [logger.py]: Classifying 1659 posts with intergroup classifier...
...
2026-01-22 20:00:52,412 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:00:52,413 INFO [logger.py]: Current queue size: 0 items
2026-01-22 20:00:52,414 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:00:52,414 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 20:00:52,417 INFO [logger.py]: Current queue size: 779 items
2026-01-22 20:00:52,417 INFO [logger.py]: Adding 19 posts to the output queue.
2026-01-22 20:00:52,418 INFO [logger.py]: Writing 19 items as 1 minibatches to DB.
2026-01-22 20:00:52,418 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 20:00:52,418 INFO [logger.py]: Processing batch 1/1...
2026-01-22 20:00:52,420 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 20:00:52,420 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 83/83 [10:25<00:00,  7.53s/batch, successful=1659, failed=0]
Execution time for run_batch_classification: 10 minutes, 26 seconds
Memory usage for run_batch_classification: -404.203125 MB
Execution time for orchestrate_classification: 10 minutes, 31 seconds
Memory usage for orchestrate_classification: -395.640625 MB
Execution time for classify_latest_posts: 10 minutes, 33 seconds
Memory usage for classify_latest_posts: -395.625 MB
2026-01-22 20:00:55,610 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 20:00:55,612 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-22 20:11:40,612 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 20:11:40,728 INFO [logger.py]: Not clearing any queues.
2026-01-22 20:11:40,728 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:11:40,729 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 20:11:40,752 INFO [logger.py]: Current queue size: 780 items
2026-01-22 20:11:40,845 INFO [logger.py]: Exporting 15534 records to local storage for integration ml_inference_intergroup...
2026-01-22 20:11:40,888 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-09-29] Exporting n=1503 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-22 20:11:40,996 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-22 20:11:41,001 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-09-30] Exporting n=3894 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-22 20:11:41,009 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-22 20:11:41,013 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-01] Exporting n=3726 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-22 20:11:41,021 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-22 20:11:41,024 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-02] Exporting n=2591 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-22 20:11:41,030 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-22 20:11:41,034 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-03] Exporting n=2982 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-22 20:11:41,041 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-22 20:11:41,043 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-04] Exporting n=838 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-22 20:11:41,046 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-22 20:11:41,047 INFO [logger.py]: Finished exporting 15534 records to local storage for integration ml_inference_intergroup...
2026-01-22 20:11:41,047 INFO [logger.py]: Successfully wrote 15534 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-09-29	archive_ml_inference_intergroup:partition_date=2024-09-30	archive_ml_inference_intergroup:partition_date=2024-10-01	archive_ml_inference_intergroup:partition_date=2024-10-02	archive_ml_inference_intergroup:partition_date=2024-10-03	archive_ml_inference_intergroup:partition_date=2024-10-04
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-09-29
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-09-30
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-01
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-02
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-03
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-04
```

```sql
SELECT label, COUNT(*)
FROM archive_ml_inference_intergroup
GROUP BY 1
```

```bash
#	label	_col1
1	1	1674
2	0	15561
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```
#	partition_date	total_labels
1	2024-09-29	1503
2	2024-09-30	3894
3	2024-10-01	3726
4	2024-10-02	2591
5	2024-10-03	2982
6	2024-10-04	838
7	2024-10-12	1701
```

- [X] Delete input and output queues

## Week 2: 2024-10-05 to 2024-10-11

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-05 --end-date 2024-10-11 --source-data-location s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-05 --end-date 2024-10-11 --source-data-location s3
2026-01-22 20:16:39,608 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 20:16:39,727 INFO [logger.py]: Not clearing any queues.
2026-01-22 20:16:39,748 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
2026-01-22 20:16:39,805 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 20:16:39,811 INFO [logger.py]: [Progress: 1/1] Enqueuing records for integration: ml_inference_intergroup
2026-01-22 20:16:39,812 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-22 20:16:39,978 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-05] Found n_files=1 parquet objects.
2026-01-22 20:16:39,979 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-22 20:16:40,557 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 687, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.0833292007446289)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:16:40,558 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=['cache'], n_days=5.
2026-01-22 20:16:40,595 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-01] Found n_files=2 parquet objects.
2026-01-22 20:16:40,633 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-02] Found n_files=2 parquet objects.
2026-01-22 20:16:40,673 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-03] Found n_files=2 parquet objects.
2026-01-22 20:16:40,711 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-04] Found n_files=2 parquet objects.
2026-01-22 20:16:40,754 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-05] Found n_files=2 parquet objects.
2026-01-22 20:16:40,754 INFO [logger.py]: Listed total_parquet_files=10 for dataset=preprocessed_posts.
2026-01-22 20:16:55,634 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1159191, 'columns': 3}, 'result_memory_usage_mb': np.float64(451.56801414489746)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:16:59,165 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-22 20:16:59,200 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-06] Found n_files=1 parquet objects.
2026-01-22 20:16:59,201 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-22 20:16:59,591 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 314, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.03815269470214844)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:16:59,592 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=['cache'], n_days=5.
2026-01-22 20:16:59,630 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-02] Found n_files=2 parquet objects.
2026-01-22 20:16:59,668 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-03] Found n_files=2 parquet objects.
2026-01-22 20:16:59,710 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-04] Found n_files=2 parquet objects.
2026-01-22 20:16:59,755 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-05] Found n_files=2 parquet objects.
2026-01-22 20:16:59,796 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-06] Found n_files=2 parquet objects.
2026-01-22 20:16:59,796 INFO [logger.py]: Listed total_parquet_files=10 for dataset=preprocessed_posts.
2026-01-22 20:17:17,958 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1519164, 'columns': 3}, 'result_memory_usage_mb': np.float64(581.7477426528931)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:17:24,472 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-22 20:17:24,509 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-07] Found n_files=1 parquet objects.
2026-01-22 20:17:24,509 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-22 20:17:24,972 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 138, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.016836166381835938)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:17:24,975 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=['cache'], n_days=5.
2026-01-22 20:17:25,014 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-03] Found n_files=2 parquet objects.
2026-01-22 20:17:25,053 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-04] Found n_files=2 parquet objects.
2026-01-22 20:17:25,090 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-05] Found n_files=2 parquet objects.
2026-01-22 20:17:25,127 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-06] Found n_files=2 parquet objects.
2026-01-22 20:17:25,165 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-07] Found n_files=2 parquet objects.
2026-01-22 20:17:25,165 INFO [logger.py]: Listed total_parquet_files=10 for dataset=preprocessed_posts.
2026-01-22 20:17:44,003 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1449798, 'columns': 3}, 'result_memory_usage_mb': np.float64(552.9414501190186)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:17:51,095 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-22 20:17:51,136 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-09] Found n_files=1 parquet objects.
2026-01-22 20:17:51,136 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-22 20:17:51,759 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 1642, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.19899559020996094)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:17:51,765 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=['cache'], n_days=4.
2026-01-22 20:17:51,810 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-05] Found n_files=2 parquet objects.
2026-01-22 20:17:51,861 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-06] Found n_files=2 parquet objects.
2026-01-22 20:17:51,915 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-07] Found n_files=2 parquet objects.
2026-01-22 20:17:51,959 INFO [logger.py]: Listed total_parquet_files=6 for dataset=preprocessed_posts.
2026-01-22 20:18:07,802 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1165148, 'columns': 3}, 'result_memory_usage_mb': np.float64(438.0504608154297)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:18:12,792 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-22 20:18:12,836 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-10] Found n_files=1 parquet objects.
2026-01-22 20:18:12,836 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-22 20:18:13,590 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 3146, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.38115501403808594)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:18:13,593 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=['cache'], n_days=4.
2026-01-22 20:18:13,635 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-06] Found n_files=2 parquet objects.
2026-01-22 20:18:13,676 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-07] Found n_files=2 parquet objects.
2026-01-22 20:18:13,752 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-10] Found n_files=2 parquet objects.
2026-01-22 20:18:13,752 INFO [logger.py]: Listed total_parquet_files=6 for dataset=preprocessed_posts.
2026-01-22 20:19:40,799 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1777488, 'columns': 3}, 'result_memory_usage_mb': np.float64(708.0376844406128)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:19:46,686 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-22 20:19:46,832 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-11] Found n_files=1 parquet objects.
2026-01-22 20:19:46,832 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-22 20:19:47,541 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 9385, 'columns': 1}, 'result_memory_usage_mb': np.float64(1.1368017196655273)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:19:47,552 INFO [logger.py]: Listing S3 parquet URIs for dataset=preprocessed_posts, storage_tiers=['cache'], n_days=4.
2026-01-22 20:19:47,592 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-07] Found n_files=2 parquet objects.
2026-01-22 20:19:47,677 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-10] Found n_files=2 parquet objects.
2026-01-22 20:19:47,716 INFO [logger.py]: [dataset=preprocessed_posts tier=cache partition_date=2024-10-11] Found n_files=2 parquet objects.
2026-01-22 20:19:47,717 INFO [logger.py]: Listed total_parquet_files=6 for dataset=preprocessed_posts.
2026-01-22 20:21:18,952 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': "SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != ''", 'result_shape': {'rows': 1438987, 'columns': 3}, 'result_memory_usage_mb': np.float64(586.4589624404907)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:21:24,316 INFO [logger.py]: Listing S3 parquet URIs for dataset=ml_inference_intergroup, storage_tiers=['cache'], n_days=6.
2026-01-22 20:21:24,787 INFO [logger.py]: Listed total_parquet_files=0 for dataset=ml_inference_intergroup.
2026-01-22 20:21:24,788 WARNING [logger.py]: 
                    filepaths must be provided when mode='parquet.
                    There are scenarios where data is missing (e.g., in the "active"
                    path, there might not be any up-to-date records). In these cases,
                    it's assumed that the filepaths are not provided.
                    
2026-01-22 20:21:24,797 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-22 20:21:24,798 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-22 20:21:24,805 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:21:24,836 INFO [logger.py]: Writing 12934 items as 13 minibatches to DB.
2026-01-22 20:21:24,836 INFO [logger.py]: Writing 13 minibatches to DB as 1 batches...
2026-01-22 20:21:24,836 INFO [logger.py]: Processing batch 1/1...
2026-01-22 20:21:24,853 INFO [logger.py]: Inserted 12934 posts into queue for integration: ml_inference_intergroup
2026-01-22 20:21:24,879 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-22 20:21:24,879 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - Doing it in batches:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 3000`

Iteration 1:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 3000
2026-01-22 20:22:29,410 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 20:22:29,526 INFO [logger.py]: Not clearing any queues.
2026-01-22 20:22:29,527 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 20:22:29,528 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 20:22:37,163 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:22:37,172 INFO [logger.py]: Current queue size: 13 items
2026-01-22 20:22:37,228 INFO [logger.py]: Loaded 12934 posts to classify.
2026-01-22 20:22:37,229 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 20:22:37,229 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 20:22:37,239 INFO [logger.py]: After dropping duplicates, 12934 posts remain.
2026-01-22 20:22:37,253 INFO [logger.py]: After filtering, 12704 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 30.03125 MB
2026-01-22 20:22:38,439 INFO [logger.py]: Limited posts from 12704 to 2940 (max_records_per_run=3000, included 3 complete batches)
2026-01-22 20:22:38,442 INFO [logger.py]: Classifying 2940 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/147 [00:00<?, ?batch/s]2026-01-22 20:22:39,500 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/147...
2026-01-22 20:22:46,390 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-22 20:22:46,390 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:22:46,391 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:22:46,392 INFO [logger.py]: Current queue size: 13 items
2026-01-22 20:22:46,392 INFO [logger.py]: Creating new SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 20:22:46,396 INFO [logger.py]: Adding 20 posts to the output queue.
2026-01-22 20:22:46,397 INFO [logger.py]: Writing 20 items as 1 minibatches to DB.
2026-01-22 20:22:46,397 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 20:22:46,397 INFO [logger.py]: Processing batch 1/1...
2026-01-22 20:22:46,399 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 20:22:46,400 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches:   1%|▎                                               | 1/147 [00:06<16:47,  6.90s/batch, successful=20, failed=0]
...
Classifying batches:  99%|███████████████████████████████████████████▋| 146/147 [17:30<00:07,  7.10s/batch, successful=2920, failed=0]2026-01-22 20:40:20,425 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-22 20:40:20,426 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:40:20,426 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:40:20,429 INFO [logger.py]: Current queue size: 10 items
2026-01-22 20:40:20,429 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:40:20,429 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 20:40:20,438 INFO [logger.py]: Current queue size: 146 items
2026-01-22 20:40:20,438 INFO [logger.py]: Adding 20 posts to the output queue.
2026-01-22 20:40:20,438 INFO [logger.py]: Writing 20 items as 1 minibatches to DB.
2026-01-22 20:40:20,438 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 20:40:20,438 INFO [logger.py]: Processing batch 1/1...
2026-01-22 20:40:20,439 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 20:40:20,440 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 147/147 [17:40<00:00,  7.22s/batch, successful=2940, failed=0]
Execution time for run_batch_classification: 17 minutes, 42 seconds
Memory usage for run_batch_classification: -30.484375 MB
Execution time for orchestrate_classification: 17 minutes, 46 seconds
Memory usage for orchestrate_classification: -47.28125 MB
Execution time for classify_latest_posts: 17 minutes, 49 seconds
Memory usage for classify_latest_posts: -48.484375 MB
2026-01-22 20:40:23,627 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 20:40:23,629 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 2:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 3000                                                                          
2026-01-22 20:48:37,755 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 20:48:37,916 INFO [logger.py]: Not clearing any queues.
2026-01-22 20:48:37,918 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 20:48:37,918 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 20:48:45,155 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:48:45,166 INFO [logger.py]: Current queue size: 10 items
2026-01-22 20:48:45,209 INFO [logger.py]: Loaded 9934 posts to classify.
2026-01-22 20:48:45,210 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 20:48:45,210 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 20:48:45,218 INFO [logger.py]: After dropping duplicates, 9934 posts remain.
2026-01-22 20:48:45,229 INFO [logger.py]: After filtering, 9764 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 25.296875 MB
2026-01-22 20:48:46,410 INFO [logger.py]: Limited posts from 9764 to 2945 (max_records_per_run=3000, included 3 complete batches)
2026-01-22 20:48:46,414 INFO [logger.py]: Classifying 2945 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/148 [00:00<?, ?batch/s]2026-01-22 20:48:47,477 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/148...
2026-01-22 20:48:57,912 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-22 20:48:57,913 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:48:57,913 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:48:57,914 INFO [logger.py]: Current queue size: 10 items
2026-01-22 20:48:57,914 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:48:57,914 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 20:48:57,921 INFO [logger.py]: Current queue size: 147 items
2026-01-22 20:48:57,921 INFO [logger.py]: Adding 20 posts to the output queue.
2026-01-22 20:48:57,922 INFO [logger.py]: Writing 20 items as 1 minibatches to DB.
2026-01-22 20:48:57,922 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 20:48:57,922 INFO [logger.py]: Processing batch 1/1...
2026-01-22 20:48:57,924 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 20:48:57,925 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches:   1%|▎                                               | 1/148 [00:10<25:35, 10.45s/batch, successful=20, failed=0]2026-01-22 20:49:03,380 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-22 20:49:03,381 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:49:03,381 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 20:49:03,381 INFO [logger.py]: Current queue size: 9 items
2026-01-22 20:49:03,381 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-22 20:49:03,382 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-22 20:49:03,382 INFO [logger.py]: Current queue size: 148 items
2026-01-22 20:49:03,382 INFO [logger.py]: Adding 20 posts to the output queue.
2026-01-22 20:49:03,382 INFO [logger.py]: Writing 20 items as 1 minibatches to DB.
2026-01-22 20:49:03,382 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 20:49:03,382 INFO [logger.py]: Processing batch 1/1...
2026-01-22 20:49:03,383 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
...
2026-01-22 21:07:59,646 INFO [logger.py]: Adding 5 posts to the output queue.
2026-01-22 21:07:59,646 INFO [logger.py]: Writing 5 items as 1 minibatches to DB.
2026-01-22 21:07:59,647 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-22 21:07:59,647 INFO [logger.py]: Processing batch 1/1...
2026-01-22 21:07:59,650 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-22 21:07:59,650 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 148/148 [19:12<00:00,  7.78s/batch, successful=2945, failed=0]
Execution time for run_batch_classification: 19 minutes, 13 seconds
Memory usage for run_batch_classification: -481.3125 MB
Execution time for orchestrate_classification: 19 minutes, 18 seconds
Memory usage for orchestrate_classification: -481.890625 MB
Execution time for classify_latest_posts: 19 minutes, 20 seconds
Memory usage for classify_latest_posts: -481.859375 MB
2026-01-22 21:08:02,841 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-22 21:08:02,843 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 3: going to let this run overnight.

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 7000                
2026-01-22 22:44:55,682 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-22 22:44:55,804 INFO [logger.py]: Not clearing any queues.
2026-01-22 22:44:55,806 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-22 22:44:55,806 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-22 22:45:03,235 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-22 22:45:03,243 INFO [logger.py]: Current queue size: 7 items
2026-01-22 22:45:03,275 INFO [logger.py]: Loaded 6934 posts to classify.
2026-01-22 22:45:03,275 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-22 22:45:03,275 INFO [logger.py]: Latest inference timestamp: None
2026-01-22 22:45:03,283 INFO [logger.py]: After dropping duplicates, 6934 posts remain.
2026-01-22 22:45:03,292 INFO [logger.py]: After filtering, 6819 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 19.25 MB
2026-01-22 22:45:04,463 INFO [logger.py]: Classifying 6819 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/341 [00:00<?, ?batch/s]2026-01-22 22:45:05,548 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/341...
...
2026-01-23 00:06:16,197 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 00:06:16,199 INFO [logger.py]: Current queue size: 635 items
2026-01-23 00:06:16,199 INFO [logger.py]: Adding 19 posts to the output queue.
2026-01-23 00:06:16,200 INFO [logger.py]: Writing 19 items as 1 minibatches to DB.
2026-01-23 00:06:16,200 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 00:06:16,200 INFO [logger.py]: Processing batch 1/1...
2026-01-23 00:06:16,202 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 00:06:16,203 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████| 341/341 [1:21:10<00:00, 14.28s/batch, successful=6819, failed=0]
Execution time for run_batch_classification: 81 minutes, 12 seconds
Memory usage for run_batch_classification: -450.65625 MB
Execution time for orchestrate_classification: 81 minutes, 16 seconds
Memory usage for orchestrate_classification: -434.328125 MB
Execution time for classify_latest_posts: 81 minutes, 18 seconds
Memory usage for classify_latest_posts: -444.59375 MB
2026-01-23 00:06:19,385 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 00:06:19,387 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`


```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-23 08:23:12,026 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 08:23:12,209 INFO [logger.py]: Not clearing any queues.
2026-01-23 08:23:12,210 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-23 08:23:12,210 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 08:23:12,227 INFO [logger.py]: Current queue size: 636 items
2026-01-23 08:23:12,300 INFO [logger.py]: Exporting 12704 records to local storage for integration ml_inference_intergroup...
2026-01-23 08:23:12,329 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-04] Exporting n=351 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 08:23:12,489 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 08:23:12,491 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-05] Exporting n=325 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 08:23:12,493 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 08:23:12,495 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-06] Exporting n=393 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 08:23:12,498 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 08:23:12,500 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-07] Exporting n=45 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 08:23:12,502 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 08:23:12,507 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-10] Exporting n=4007 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 08:23:12,515 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 08:23:12,523 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-11] Exporting n=7583 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 08:23:12,535 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 08:23:12,536 INFO [logger.py]: Finished exporting 12704 records to local storage for integration ml_inference_intergroup...
2026-01-23 08:23:12,536 INFO [logger.py]: Successfully wrote 12704 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-23 08:23:30,725 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 08:23:30,831 INFO [logger.py]: Not clearing any queues.
2026-01-23 08:23:30,891 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-23 08:23:30,895 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-23 08:23:30,895 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-23 08:23:30,896 INFO [logger.py]: Registering 18 files for migration                                                           
2026-01-23 08:23:30,898 INFO [logger.py]: Registered 18 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (18 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 515.33it/s]
Finished initializing migration tracker db
2026-01-23 08:23:30,904 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-23 08:23:30,975 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/8b65edb81dd140d2b268e7229a770864-0.parquet
2026-01-23 08:23:30,975 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/8b65edb81dd140d2b268e7229a770864-0.parquet to S3 (0.19 MB)
2026-01-23 08:23:31,343 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/8b65edb81dd140d2b268e7229a770864-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-10/8b65edb81dd140d2b268e7229a770864-0.parquet
2026-01-23 08:23:31,344 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/8b65edb81dd140d2b268e7229a770864-0.parquet
                                                                                                                                     2026-01-23 08:23:31,345 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/a905406653254b21a6037ad02aed5b68-0.parquet
2026-01-23 08:23:31,345 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/a905406653254b21a6037ad02aed5b68-0.parquet to S3 (0.43 MB)
2026-01-23 08:23:31,595 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/a905406653254b21a6037ad02aed5b68-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-10/a905406653254b21a6037ad02aed5b68-0.parquet
2026-01-23 08:23:31,597 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-10/a905406653254b21a6037ad02aed5b68-0.parquet
                                                                                                                                     2026-01-23 08:23:31,598 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/dcb4b45cd9d446519ab3957fe34acd7c-0.parquet
2026-01-23 08:23:31,598 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/dcb4b45cd9d446519ab3957fe34acd7c-0.parquet to S3 (0.80 MB)
2026-01-23 08:23:31,790 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/dcb4b45cd9d446519ab3957fe34acd7c-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-11/dcb4b45cd9d446519ab3957fe34acd7c-0.parquet
2026-01-23 08:23:31,791 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/dcb4b45cd9d446519ab3957fe34acd7c-0.parquet
                                                                                                                                     2026-01-23 08:23:31,792 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/f4855dfa7b074aaa8def1a158ee6fe6c-0.parquet
2026-01-23 08:23:31,792 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/f4855dfa7b074aaa8def1a158ee6fe6c-0.parquet to S3 (0.04 MB)
2026-01-23 08:23:31,895 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/f4855dfa7b074aaa8def1a158ee6fe6c-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-05/f4855dfa7b074aaa8def1a158ee6fe6c-0.parquet
2026-01-23 08:23:31,897 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/f4855dfa7b074aaa8def1a158ee6fe6c-0.parquet
                                                                                                                                     2026-01-23 08:23:31,899 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/dd17ed9798e54f25a6e9ef767b201121-0.parquet
2026-01-23 08:23:31,899 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/dd17ed9798e54f25a6e9ef767b201121-0.parquet to S3 (0.04 MB)
2026-01-23 08:23:31,954 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/dd17ed9798e54f25a6e9ef767b201121-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-05/dd17ed9798e54f25a6e9ef767b201121-0.parquet
2026-01-23 08:23:31,957 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-05/dd17ed9798e54f25a6e9ef767b201121-0.parquet
2026-01-23 08:23:31,958 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/f95aab94bdc847b2877378051bed0e9d-0.parquet
2026-01-23 08:23:31,959 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/f95aab94bdc847b2877378051bed0e9d-0.parquet to S3 (0.04 MB)
2026-01-23 08:23:32,074 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/f95aab94bdc847b2877378051bed0e9d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-04/f95aab94bdc847b2877378051bed0e9d-0.parquet
2026-01-23 08:23:32,075 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/f95aab94bdc847b2877378051bed0e9d-0.parquet
                                                                                                                                     2026-01-23 08:23:32,076 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/7e7934ee6c4b4f239fd8642882ad36bd-0.parquet
2026-01-23 08:23:32,077 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/7e7934ee6c4b4f239fd8642882ad36bd-0.parquet to S3 (0.04 MB)
2026-01-23 08:23:32,125 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/7e7934ee6c4b4f239fd8642882ad36bd-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-04/7e7934ee6c4b4f239fd8642882ad36bd-0.parquet
2026-01-23 08:23:32,127 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-04/7e7934ee6c4b4f239fd8642882ad36bd-0.parquet
2026-01-23 08:23:32,127 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/42aa4abeb81144d790e56440781e8523-0.parquet
2026-01-23 08:23:32,128 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/42aa4abeb81144d790e56440781e8523-0.parquet to S3 (0.04 MB)
2026-01-23 08:23:32,234 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/42aa4abeb81144d790e56440781e8523-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-06/42aa4abeb81144d790e56440781e8523-0.parquet
2026-01-23 08:23:32,237 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/42aa4abeb81144d790e56440781e8523-0.parquet
                                                                                                                                     2026-01-23 08:23:32,238 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/ebe8127fe5c84f3e8f69da74534a3b9a-0.parquet
2026-01-23 08:23:32,238 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/ebe8127fe5c84f3e8f69da74534a3b9a-0.parquet to S3 (0.04 MB)
2026-01-23 08:23:32,288 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/ebe8127fe5c84f3e8f69da74534a3b9a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-06/ebe8127fe5c84f3e8f69da74534a3b9a-0.parquet
2026-01-23 08:23:32,289 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-06/ebe8127fe5c84f3e8f69da74534a3b9a-0.parquet
2026-01-23 08:23:32,290 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/0627561296b54b4eab5051d686877169-0.parquet
2026-01-23 08:23:32,290 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/0627561296b54b4eab5051d686877169-0.parquet to S3 (0.01 MB)
2026-01-23 08:23:32,344 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/0627561296b54b4eab5051d686877169-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-07/0627561296b54b4eab5051d686877169-0.parquet
2026-01-23 08:23:32,345 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/0627561296b54b4eab5051d686877169-0.parquet
                                                                                                                                     2026-01-23 08:23:32,347 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/bc84e7141ee547f69419c94098e98c3f-0.parquet
2026-01-23 08:23:32,347 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/bc84e7141ee547f69419c94098e98c3f-0.parquet to S3 (0.01 MB)
2026-01-23 08:23:32,397 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/bc84e7141ee547f69419c94098e98c3f-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-07/bc84e7141ee547f69419c94098e98c3f-0.parquet
2026-01-23 08:23:32,398 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-07/bc84e7141ee547f69419c94098e98c3f-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████| 11/11 [00:01<00:00,  7.72it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:01<00:00,  1.40it/s]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```sql
SELECT label, COUNT(*)
FROM archive_ml_inference_intergroup
GROUP BY 1
```

```bash
#	label	_col1
1	1	3065
2	0	29814
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
6	2024-10-04	1540
7	2024-10-05	650
8	2024-10-06	786
9	2024-10-07	79
10	2024-10-10	5844
11	2024-10-11	7583
12	2024-10-12	1701
```

- [X] Check input queue:

```sql
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db                             
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> .tables
queue
sqlite> SELECT COUNT(*) FROM queue;
0
```

- [X] Delete input and output queues

## Week 3: 2024-10-12 to 2024-10-18

### Pt. I: 2024-10-12 to 2024-10-13

Weirdly, the current CLI app doesn't support 1-day backfills (e.g., with the partition-date flag) even though I know that the helper tooling does. It's OK, we'll roll with it.

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-12 --end-date 2024-10-13 --source-data-location s3`

```bash
2026-01-23 08:54:39,748 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 08:54:39,964 INFO [logger.py]: Writing 154885 items as 155 minibatches to DB.
2026-01-23 08:54:39,964 INFO [logger.py]: Writing 155 minibatches to DB as 7 batches...
2026-01-23 08:54:39,964 INFO [logger.py]: Processing batch 1/7...
2026-01-23 08:54:40,194 INFO [logger.py]: Inserted 154885 posts into queue for integration: ml_inference_intergroup
2026-01-23 08:54:40,243 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-23 08:54:40,244 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

Sheesh, 150,000 posts? I'll have my work cut out for me.

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 2500`

Iteration 1:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 2500
2026-01-23 08:56:46,421 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 08:56:46,557 INFO [logger.py]: Not clearing any queues.
2026-01-23 08:56:46,558 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-23 08:56:46,558 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-23 08:56:56,835 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 08:56:56,852 INFO [logger.py]: Current queue size: 155 items
2026-01-23 08:56:57,502 INFO [logger.py]: Loaded 154885 posts to classify.
2026-01-23 08:56:57,504 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-23 08:56:57,504 INFO [logger.py]: Latest inference timestamp: None
2026-01-23 08:56:57,589 INFO [logger.py]: After dropping duplicates, 154885 posts remain.
2026-01-23 08:56:57,765 INFO [logger.py]: After filtering, 152422 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 3 seconds
Memory usage for get_posts_to_classify: 346.25 MB
2026-01-23 08:56:59,689 INFO [logger.py]: Limited posts from 152422 to 1960 (max_records_per_run=2500, included 2 complete batches)
2026-01-23 08:56:59,719 INFO [logger.py]: Classifying 1960 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/98 [00:00<?, ?batch/s]2026-01-23 08:57:00,774 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/98...
...
Classifying batches:  99%|█████████████████████████████████████████████▌| 97/98 [14:31<00:06,  6.78s/batch, successful=1940, failed=0]2026-01-23 09:11:43,193 INFO [logger.py]: Successfully labeled 20 posts.
2026-01-23 09:11:43,195 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-23 09:11:43,195 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 09:11:43,209 INFO [logger.py]: Current queue size: 153 items
2026-01-23 09:11:43,210 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-23 09:11:43,210 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 09:11:43,212 INFO [logger.py]: Current queue size: 97 items
2026-01-23 09:11:43,212 INFO [logger.py]: Adding 20 posts to the output queue.
2026-01-23 09:11:43,212 INFO [logger.py]: Writing 20 items as 1 minibatches to DB.
2026-01-23 09:11:43,212 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 09:11:43,212 INFO [logger.py]: Processing batch 1/1...
2026-01-23 09:11:43,214 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 09:11:43,214 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 98/98 [14:42<00:00,  9.00s/batch, successful=1960, failed=0]
Execution time for run_batch_classification: 14 minutes, 43 seconds
Memory usage for run_batch_classification: -547.953125 MB
Execution time for orchestrate_classification: 14 minutes, 49 seconds
Memory usage for orchestrate_classification: -444.453125 MB
Execution time for classify_latest_posts: 14 minutes, 52 seconds
Memory usage for classify_latest_posts: -445.71875 MB
2026-01-23 09:11:46,317 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 09:11:46,320 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 2:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 4000
2026-01-23 09:47:32,526 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 09:47:32,645 INFO [logger.py]: Not clearing any queues.
2026-01-23 09:47:32,646 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-23 09:47:32,647 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-23 09:47:39,833 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 09:47:39,861 INFO [logger.py]: Current queue size: 153 items
2026-01-23 09:47:40,456 INFO [logger.py]: Loaded 152885 posts to classify.
2026-01-23 09:47:40,457 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-23 09:47:40,458 INFO [logger.py]: Latest inference timestamp: None
2026-01-23 09:47:40,544 INFO [logger.py]: After dropping duplicates, 152885 posts remain.
2026-01-23 09:47:40,699 INFO [logger.py]: After filtering, 150462 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 3 seconds
Memory usage for get_posts_to_classify: 331.109375 MB
2026-01-23 09:47:42,482 INFO [logger.py]: Limited posts from 150462 to 3922 (max_records_per_run=4000, included 4 complete batches)
2026-01-23 09:47:42,514 INFO [logger.py]: Classifying 3922 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/197 [00:00<?, ?batch/s]2026-01-23 09:47:43,599 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/197...
...
2026-01-23 10:14:53,595 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 10:14:53,598 INFO [logger.py]: Current queue size: 294 items
2026-01-23 10:14:53,598 INFO [logger.py]: Adding 2 posts to the output queue.
2026-01-23 10:14:53,599 INFO [logger.py]: Writing 2 items as 1 minibatches to DB.
2026-01-23 10:14:53,599 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 10:14:53,599 INFO [logger.py]: Processing batch 1/1...
2026-01-23 10:14:53,603 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 10:14:53,603 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 197/197 [27:10<00:00,  8.27s/batch, successful=3922, failed=0]
Execution time for run_batch_classification: 27 minutes, 11 seconds
Memory usage for run_batch_classification: -558.734375 MB
Execution time for orchestrate_classification: 27 minutes, 17 seconds
Memory usage for orchestrate_classification: -463.140625 MB
Execution time for classify_latest_posts: 27 minutes, 19 seconds
Memory usage for classify_latest_posts: -465.0 MB
2026-01-23 10:14:56,724 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 10:14:56,726 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
...
2026-01-23 12:11:53,709 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 12:11:53,710 INFO [logger.py]: Current queue size: 145 items
2026-01-23 12:11:53,710 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-23 12:11:53,710 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 12:11:53,711 INFO [logger.py]: Current queue size: 492 items
2026-01-23 12:11:53,712 INFO [logger.py]: Adding 3 posts to the output queue.
2026-01-23 12:11:53,712 INFO [logger.py]: Writing 3 items as 1 minibatches to DB.
2026-01-23 12:11:53,712 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 12:11:53,712 INFO [logger.py]: Processing batch 1/1...
2026-01-23 12:11:53,713 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 12:11:53,714 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████████| 198/198 [25:18<00:00,  7.67s/batch, successful=3943, failed=0]
Execution time for run_batch_classification: 25 minutes, 20 seconds
Memory usage for run_batch_classification: -652.59375 MB
Execution time for orchestrate_classification: 25 minutes, 25 seconds
Memory usage for orchestrate_classification: -538.421875 MB
Execution time for classify_latest_posts: 25 minutes, 27 seconds
Memory usage for classify_latest_posts: -538.5625 MB
2026-01-23 12:11:56,834 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 12:11:56,836 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 3:

I tried to change DEFAULT_BATCH_SIZE from 20 to 100 and it DOES speed up. Sheesh.

```bash
2026-01-23 19:11:04,400 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 19:11:04,403 INFO [logger.py]: Current queue size: 532 items
2026-01-23 19:11:04,403 INFO [logger.py]: Adding 34 posts to the output queue.
2026-01-23 19:11:04,403 INFO [logger.py]: Writing 34 items as 1 minibatches to DB.
2026-01-23 19:11:04,404 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 19:11:04,404 INFO [logger.py]: Processing batch 1/1...
2026-01-23 19:11:04,406 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 19:11:04,407 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 40/40 [06:52<00:00, 10.30s/batch, successful=3934, failed=0]
Execution time for run_batch_classification: 6 minutes, 53 seconds
Memory usage for run_batch_classification: -143.09375 MB
Execution time for orchestrate_classification: 6 minutes, 59 seconds
Memory usage for orchestrate_classification: -42.75 MB
Execution time for classify_latest_posts: 7 minutes, 1 seconds
Memory usage for classify_latest_posts: -42.75 MB
2026-01-23 19:11:07,514 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 19:11:07,516 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 4: trying with batch size of 500 now.

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 4000
2026-01-23 19:15:13,841 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 19:15:13,976 INFO [logger.py]: Not clearing any queues.
2026-01-23 19:15:13,977 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-23 19:15:13,977 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-23 19:15:21,475 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 19:15:21,497 INFO [logger.py]: Current queue size: 141 items
2026-01-23 19:15:22,069 INFO [logger.py]: Loaded 140885 posts to classify.
2026-01-23 19:15:22,070 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-23 19:15:22,070 INFO [logger.py]: Latest inference timestamp: None
2026-01-23 19:15:22,150 INFO [logger.py]: After dropping duplicates, 140885 posts remain.
2026-01-23 19:15:22,281 INFO [logger.py]: After filtering, 138663 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 314.171875 MB
2026-01-23 19:15:23,865 INFO [logger.py]: Limited posts from 138663 to 3937 (max_records_per_run=4000, included 4 complete batches)
2026-01-23 19:15:23,894 INFO [logger.py]: Classifying 3937 posts with intergroup classifier...
Classifying batches:   0%|                                                                                   | 0/8 [00:00<?, ?batch/s]2026-01-23 19:15:24,975 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/8...
2026-01-23 19:15:47,604 INFO [logger.py]: Successfully labeled 500 posts.
2026-01-23 19:15:47,604 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...

...

Classifying batches: 100%|████████████████████████████████████████████████| 8/8 [02:56<00:00, 22.10s/batch, successful=3921, failed=0]
Execution time for run_batch_classification: 2 minutes, 58 seconds
Memory usage for run_batch_classification: -91.953125 MB
Execution time for orchestrate_classification: 3 minutes, 3 seconds
Memory usage for orchestrate_classification: -22.296875 MB
Execution time for classify_latest_posts: 3 minutes, 6 seconds
Memory usage for classify_latest_posts: -32.234375 MB
2026-01-23 19:26:47,227 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 19:26:47,229 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

We're classifying ~4,000 posts in ~4 minutes. Promising! I'll try a bigger amount, 10,000.

Iteration 5:

```bash
2026-01-23 19:38:29,762 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 19:38:29,776 INFO [logger.py]: Current queue size: 576 items
2026-01-23 19:38:29,777 INFO [logger.py]: Adding 352 posts to the output queue.
2026-01-23 19:38:29,777 INFO [logger.py]: Writing 352 items as 1 minibatches to DB.
2026-01-23 19:38:29,777 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 19:38:29,778 INFO [logger.py]: Processing batch 1/1...
2026-01-23 19:38:29,779 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 19:38:29,782 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 20/20 [08:10<00:00, 24.55s/batch, successful=9852, failed=0]
Execution time for run_batch_classification: 8 minutes, 12 seconds
Memory usage for run_batch_classification: -98.90625 MB
Execution time for orchestrate_classification: 8 minutes, 17 seconds
Memory usage for orchestrate_classification: -19.8125 MB
Execution time for classify_latest_posts: 8 minutes, 20 seconds
Memory usage for classify_latest_posts: -19.8125 MB
2026-01-23 19:38:32,884 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 19:38:32,886 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 6:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 20000
2026-01-23 19:42:59,521 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 19:42:59,643 INFO [logger.py]: Not clearing any queues.
2026-01-23 19:42:59,644 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-23 19:42:59,644 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-23 19:43:06,822 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 19:43:06,843 INFO [logger.py]: Current queue size: 119 items
2026-01-23 19:43:07,289 INFO [logger.py]: Loaded 118885 posts to classify.
2026-01-23 19:43:07,290 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-23 19:43:07,290 INFO [logger.py]: Latest inference timestamp: None
2026-01-23 19:43:07,357 INFO [logger.py]: After dropping duplicates, 118885 posts remain.
2026-01-23 19:43:07,468 INFO [logger.py]: After filtering, 117013 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 252.71875 MB
2026-01-23 19:43:09,016 INFO [logger.py]: Limited posts from 117013 to 19674 (max_records_per_run=20000, included 20 complete batches)
2026-01-23 19:43:09,043 INFO [logger.py]: Classifying 19674 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/40 [00:00<?, ?batch/s]2026-01-23 19:43:10,112 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/40...

...

2026-01-23 19:57:45,341 INFO [logger.py]: Current queue size: 616 items
2026-01-23 19:57:45,341 INFO [logger.py]: Adding 174 posts to the output queue.
2026-01-23 19:57:45,342 INFO [logger.py]: Writing 174 items as 1 minibatches to DB.
2026-01-23 19:57:45,342 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 19:57:45,342 INFO [logger.py]: Processing batch 1/1...
2026-01-23 19:57:45,348 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 19:57:45,349 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 40/40 [14:35<00:00, 21.88s/batch, successful=19674, failed=0]
Execution time for run_batch_classification: 14 minutes, 36 seconds
Memory usage for run_batch_classification: -564.765625 MB
Execution time for orchestrate_classification: 14 minutes, 42 seconds
Memory usage for orchestrate_classification: -458.28125 MB
Execution time for classify_latest_posts: 14 minutes, 44 seconds
Memory usage for classify_latest_posts: -458.265625 MB
2026-01-23 19:57:48,474 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 19:57:48,476 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 7: Let's now do 80,000

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 80000
2026-01-23 20:03:31,442 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 20:03:31,562 INFO [logger.py]: Not clearing any queues.
2026-01-23 20:03:31,563 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-23 20:03:31,563 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-23 20:03:39,124 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 20:03:39,140 INFO [logger.py]: Current queue size: 99 items
2026-01-23 20:03:39,506 INFO [logger.py]: Loaded 98885 posts to classify.
2026-01-23 20:03:39,506 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-23 20:03:39,506 INFO [logger.py]: Latest inference timestamp: None
2026-01-23 20:03:39,563 INFO [logger.py]: After dropping duplicates, 98885 posts remain.
2026-01-23 20:03:39,654 INFO [logger.py]: After filtering, 97339 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 207.078125 MB
2026-01-23 20:03:41,173 INFO [logger.py]: Limited posts from 97339 to 79716 (max_records_per_run=80000, included 81 complete batches)
2026-01-23 20:03:41,178 INFO [logger.py]: Classifying 79716 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/160 [00:00<?, ?batch/s]2026-01-23 20:03:42,245 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/160...

...

2026-01-23 21:42:45,113 INFO [logger.py]: Current queue size: 776 items
2026-01-23 21:42:45,113 INFO [logger.py]: Adding 216 posts to the output queue.
2026-01-23 21:42:45,114 INFO [logger.py]: Writing 216 items as 1 minibatches to DB.
2026-01-23 21:42:45,114 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 21:42:45,114 INFO [logger.py]: Processing batch 1/1...
2026-01-23 21:42:45,115 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 21:42:45,116 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████| 160/160 [1:39:02<00:00, 37.14s/batch, successful=79716, failed=0]
Execution time for run_batch_classification: 99 minutes, 4 seconds
Memory usage for run_batch_classification: -134.171875 MB
Execution time for orchestrate_classification: 99 minutes, 9 seconds
Memory usage for orchestrate_classification: 8.828125 MB
Execution time for classify_latest_posts: 99 minutes, 11 seconds
Memory usage for classify_latest_posts: 8.828125 MB
2026-01-23 21:42:48,251 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 21:42:48,252 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

OK, now the last remaining bit!

Iteration 8:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 20000
2026-01-23 21:48:32,944 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 21:48:33,063 INFO [logger.py]: Not clearing any queues.
2026-01-23 21:48:33,063 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-23 21:48:33,064 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-23 21:48:40,869 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 21:48:40,875 INFO [logger.py]: Current queue size: 18 items
2026-01-23 21:48:40,948 INFO [logger.py]: Loaded 17885 posts to classify.
2026-01-23 21:48:40,948 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-23 21:48:40,948 INFO [logger.py]: Latest inference timestamp: None
2026-01-23 21:48:40,969 INFO [logger.py]: After dropping duplicates, 17885 posts remain.
2026-01-23 21:48:40,987 INFO [logger.py]: After filtering, 17623 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 41.59375 MB
2026-01-23 21:48:42,193 INFO [logger.py]: Classifying 17623 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/36 [00:00<?, ?batch/s]2026-01-23 21:48:43,254 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/36...

...

2026-01-23 22:03:40,711 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 22:03:40,713 INFO [logger.py]: Current queue size: 812 items
2026-01-23 22:03:40,713 INFO [logger.py]: Adding 123 posts to the output queue.
2026-01-23 22:03:40,714 INFO [logger.py]: Writing 123 items as 1 minibatches to DB.
2026-01-23 22:03:40,714 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-23 22:03:40,714 INFO [logger.py]: Processing batch 1/1...
2026-01-23 22:03:40,715 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-23 22:03:40,715 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 36/36 [14:57<00:00, 24.93s/batch, successful=17623, failed=0]
Execution time for run_batch_classification: 14 minutes, 59 seconds
Memory usage for run_batch_classification: -68.40625 MB
Execution time for orchestrate_classification: 15 minutes, 3 seconds
Memory usage for orchestrate_classification: -47.671875 MB
Execution time for classify_latest_posts: 15 minutes, 5 seconds
Memory usage for classify_latest_posts: -47.671875 MB
2026-01-23 22:03:43,843 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-23 22:03:43,844 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-23 22:10:08,413 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 22:10:08,608 INFO [logger.py]: Not clearing any queues.
2026-01-23 22:10:08,608 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-23 22:10:08,609 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-23 22:10:08,650 INFO [logger.py]: Current queue size: 813 items
2026-01-23 22:10:09,994 INFO [logger.py]: Exporting 152422 records to local storage for integration ml_inference_intergroup...
2026-01-23 22:10:10,363 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-11] Exporting n=21890 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 22:10:10,657 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 22:10:10,738 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-12] Exporting n=63792 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 22:10:10,984 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 22:10:11,141 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-13] Exporting n=66740 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-23 22:10:11,358 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-23 22:10:11,383 INFO [logger.py]: Finished exporting 152422 records to local storage for integration ml_inference_intergroup...
2026-01-23 22:10:11,407 INFO [logger.py]: Successfully wrote 152422 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-23 22:11:34,385 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-23 22:11:34,528 INFO [logger.py]: Not clearing any queues.
2026-01-23 22:11:34,556 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-23 22:11:34,564 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-23 22:11:34,564 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-23 22:11:34,567 INFO [logger.py]: Registering 21 files for migration                                                           
2026-01-23 22:11:34,570 INFO [logger.py]: Registered 21 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (21 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 276.62it/s]
Finished initializing migration tracker db
2026-01-23 22:11:34,576 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-23 22:11:34,668 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/f7cfb0c1a5fb4d24a8043da321c55530-0.parquet
2026-01-23 22:11:34,668 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/f7cfb0c1a5fb4d24a8043da321c55530-0.parquet to S3 (2.42 MB)
2026-01-23 22:11:35,720 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/f7cfb0c1a5fb4d24a8043da321c55530-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-11/f7cfb0c1a5fb4d24a8043da321c55530-0.parquet
2026-01-23 22:11:35,722 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/f7cfb0c1a5fb4d24a8043da321c55530-0.parquet
                                                                                                                                     2026-01-23 22:11:35,724 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/43ddfde1008c4f8c9e086e31852b0674-0.parquet
2026-01-23 22:11:35,724 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/43ddfde1008c4f8c9e086e31852b0674-0.parquet to S3 (7.11 MB)
2026-01-23 22:11:37,769 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/43ddfde1008c4f8c9e086e31852b0674-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-13/43ddfde1008c4f8c9e086e31852b0674-0.parquet
2026-01-23 22:11:37,770 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/43ddfde1008c4f8c9e086e31852b0674-0.parquet
                                                                                                                                     2026-01-23 22:11:37,772 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/42e2110cd83b45a291c35d34056d6bad-0.parquet
2026-01-23 22:11:37,772 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/42e2110cd83b45a291c35d34056d6bad-0.parquet to S3 (6.75 MB)
2026-01-23 22:11:40,201 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/42e2110cd83b45a291c35d34056d6bad-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-12/42e2110cd83b45a291c35d34056d6bad-0.parquet
2026-01-23 22:11:40,202 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/42e2110cd83b45a291c35d34056d6bad-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 3/3 [00:05<00:00,  1.85s/it]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:05<00:00,  2.77s/it]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-10-13
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-13
```

```sql
SELECT label, COUNT(*)
FROM archive_ml_inference_intergroup
GROUP BY 1
```

```bash
#	label	_col1
1	1	16301
2	0	169000
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
6	2024-10-04	1540
7	2024-10-05	650
8	2024-10-06	786
9	2024-10-07	79
10	2024-10-10	5844
11	2024-10-11	29473
12	2024-10-12	65493
13	2024-10-13	66740
```

- [X] Inspect input queue

```bash
(.venv) (base) ➜  queue sqlite3 output_ml_inference_intergroup.db        
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
813
sqlite> .exit
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
```

- [X] Delete input and output queues

### Pt. II: 2024-10-14 to 2024-10-15

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-14 --end-date 2024-10-15 --source-data-location s3`

```bash
...
2026-01-23 22:18:49,936 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-23 22:18:49,936 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-23 22:18:49,951 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-23 22:18:50,202 INFO [logger.py]: Writing 167910 items as 168 minibatches to DB.
2026-01-23 22:18:50,202 INFO [logger.py]: Writing 168 minibatches to DB as 7 batches...
2026-01-23 22:18:50,202 INFO [logger.py]: Processing batch 1/7...
2026-01-23 22:18:50,482 INFO [logger.py]: Inserted 167910 posts into queue for integration: ml_inference_intergroup
2026-01-23 22:18:50,547 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-23 22:18:50,548 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 100000`

Iteration 1: we'll let this run overnight as a large job. We'll do 200,000 samples (so that we can have it run the entire night).

```bash
2026-01-24 00:29:16,163 INFO [logger.py]: Current queue size: 330 items
2026-01-24 00:29:16,163 INFO [logger.py]: Adding 470 posts to the output queue.
2026-01-24 00:29:16,164 INFO [logger.py]: Writing 470 items as 1 minibatches to DB.
2026-01-24 00:29:16,164 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-24 00:29:16,164 INFO [logger.py]: Processing batch 1/1...
2026-01-24 00:29:16,166 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-24 00:29:16,166 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████| 331/331 [2:08:51<00:00, 23.36s/batch, successful=165470, failed=0]
Execution time for run_batch_classification: 128 minutes, 52 seconds
Memory usage for run_batch_classification: 148.859375 MB
Execution time for orchestrate_classification: 128 minutes, 58 seconds
Memory usage for orchestrate_classification: 587.6875 MB
Execution time for classify_latest_posts: 129 minutes, 1 seconds
Memory usage for classify_latest_posts: 565.9375 MB
2026-01-24 00:29:19,404 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-24 00:29:19,406 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-24 09:43:12,669 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-24 09:43:12,756 INFO [logger.py]: Not clearing any queues.
2026-01-24 09:43:12,785 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-24 09:43:12,789 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-24 09:43:12,790 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-24 09:43:12,793 INFO [logger.py]: Registering 31 files for migration                                                           
2026-01-24 09:43:12,796 INFO [logger.py]: Registered 31 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (31 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 273.86it/s]
Finished initializing migration tracker db
2026-01-24 09:43:12,801 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-24 09:43:12,865 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/2dfa5c6ea6f6424f8e4eed6d76b61b4a-0.parquet
2026-01-24 09:43:12,866 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/2dfa5c6ea6f6424f8e4eed6d76b61b4a-0.parquet to S3 (0.01 MB)
2026-01-24 09:43:13,080 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/2dfa5c6ea6f6424f8e4eed6d76b61b4a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-11/2dfa5c6ea6f6424f8e4eed6d76b61b4a-0.parquet
2026-01-24 09:43:13,082 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/2dfa5c6ea6f6424f8e4eed6d76b61b4a-0.parquet
                                                                                                                                     2026-01-24 09:43:13,083 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/a8c58bcc77d7484a8ab386781600bfc6-0.parquet
2026-01-24 09:43:13,083 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/a8c58bcc77d7484a8ab386781600bfc6-0.parquet to S3 (0.01 MB)
2026-01-24 09:43:13,134 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/a8c58bcc77d7484a8ab386781600bfc6-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-11/a8c58bcc77d7484a8ab386781600bfc6-0.parquet
2026-01-24 09:43:13,135 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-11/a8c58bcc77d7484a8ab386781600bfc6-0.parquet
2026-01-24 09:43:13,137 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/ac96d90b2f32471982717bd026f6915d-0.parquet
2026-01-24 09:43:13,137 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/ac96d90b2f32471982717bd026f6915d-0.parquet to S3 (7.93 MB)
2026-01-24 09:43:16,857 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/ac96d90b2f32471982717bd026f6915d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-14/ac96d90b2f32471982717bd026f6915d-0.parquet
2026-01-24 09:43:16,858 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/ac96d90b2f32471982717bd026f6915d-0.parquet
                                                                                                                                     2026-01-24 09:43:16,860 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/a213c89de1d241a7a9c9ad420ac1cbcd-0.parquet
2026-01-24 09:43:16,860 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/a213c89de1d241a7a9c9ad420ac1cbcd-0.parquet to S3 (7.93 MB)
2026-01-24 09:43:18,481 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/a213c89de1d241a7a9c9ad420ac1cbcd-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-14/a213c89de1d241a7a9c9ad420ac1cbcd-0.parquet
2026-01-24 09:43:18,484 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/a213c89de1d241a7a9c9ad420ac1cbcd-0.parquet
                                                                                                                                     2026-01-24 09:43:18,486 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/79dde5d8a95a4cd7898aca4684dd0b47-0.parquet
2026-01-24 09:43:18,486 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/79dde5d8a95a4cd7898aca4684dd0b47-0.parquet to S3 (3.79 MB)
2026-01-24 09:43:19,288 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/79dde5d8a95a4cd7898aca4684dd0b47-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-13/79dde5d8a95a4cd7898aca4684dd0b47-0.parquet
2026-01-24 09:43:19,290 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/79dde5d8a95a4cd7898aca4684dd0b47-0.parquet
                                                                                                                                     2026-01-24 09:43:19,291 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/55785b081691479fb4635ec6eb3b6071-0.parquet
2026-01-24 09:43:19,291 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/55785b081691479fb4635ec6eb3b6071-0.parquet to S3 (3.79 MB)
2026-01-24 09:43:20,096 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/55785b081691479fb4635ec6eb3b6071-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-13/55785b081691479fb4635ec6eb3b6071-0.parquet
2026-01-24 09:43:20,097 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/55785b081691479fb4635ec6eb3b6071-0.parquet
                                                                                                                                     2026-01-24 09:43:20,098 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/31c4ae3b57664f4a90d29e3a98132999-0.parquet
2026-01-24 09:43:20,098 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/31c4ae3b57664f4a90d29e3a98132999-0.parquet to S3 (1.30 MB)
2026-01-24 09:43:20,408 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/31c4ae3b57664f4a90d29e3a98132999-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-12/31c4ae3b57664f4a90d29e3a98132999-0.parquet
2026-01-24 09:43:20,409 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/31c4ae3b57664f4a90d29e3a98132999-0.parquet
                                                                                                                                     2026-01-24 09:43:20,411 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/130d44be9d044be88285c6c22f79650d-0.parquet
2026-01-24 09:43:20,411 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/130d44be9d044be88285c6c22f79650d-0.parquet to S3 (1.30 MB)
2026-01-24 09:43:20,725 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/130d44be9d044be88285c6c22f79650d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-12/130d44be9d044be88285c6c22f79650d-0.parquet
2026-01-24 09:43:20,727 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-12/130d44be9d044be88285c6c22f79650d-0.parquet
                                                                                                                                     2026-01-24 09:43:20,728 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/01f05e05ee14428a8df2bf9140d4cfb0-0.parquet
2026-01-24 09:43:20,729 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/01f05e05ee14428a8df2bf9140d4cfb0-0.parquet to S3 (4.90 MB)
2026-01-24 09:43:21,765 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/01f05e05ee14428a8df2bf9140d4cfb0-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-15/01f05e05ee14428a8df2bf9140d4cfb0-0.parquet
2026-01-24 09:43:21,768 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/01f05e05ee14428a8df2bf9140d4cfb0-0.parquet
                                                                                                                                     2026-01-24 09:43:21,772 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/946a0e5f772646ad9f5357edc1c97838-0.parquet
2026-01-24 09:43:21,773 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/946a0e5f772646ad9f5357edc1c97838-0.parquet to S3 (4.90 MB)
2026-01-24 09:43:25,093 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/946a0e5f772646ad9f5357edc1c97838-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-15/946a0e5f772646ad9f5357edc1c97838-0.parquet
2026-01-24 09:43:25,097 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/946a0e5f772646ad9f5357edc1c97838-0.parquet
Migrating ml_inference_intergroup/cache: 100%|████████████████████████████████████████████████████████| 10/10 [00:12<00:00,  1.22s/it]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:12<00:00,  6.12s/it]
```

- [ ] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-10-14	archive_ml_inference_intergroup:partition_date=2024-10-15
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-14
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-15
```

Basically my code freaked out and crashed the computer mid-write to parquet locally and it seems like the job finished, but I wasn't sure and so I reran it. Turns out it just happened to create duplicates, but IDK in which files they're interspersed. I did the naive query and found dupes, so I deduplicate here to get accurate counts.

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

OK, looks like we've got a few that are -1. That's OK, we'll just have to filter those out when we do 
```bash
#	label	_col1
1	1	28197
2	0	285534
3	-1	3
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
6	2024-10-04	903
7	2024-10-05	325
8	2024-10-06	393
9	2024-10-07	45
10	2024-10-10	4007
11	2024-10-11	27844
12	2024-10-12	65829
13	2024-10-13	68387
14	2024-10-14	72688
15	2024-10-15	45999
```

- [X] Inspect input queue

```bash
(.venv) (base) ➜  queue sqlite3 input_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(.venv) (base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
331
sqlite> .exit
```

- [X] Delete input and output queues

### Pt. III: 2024-10-16 to 2024-10-18

We implement sampling starting at this point, since it'll be too much work and cost to label all posts.

I'm starting with sampling 20% of posts for now. I'll upsample later on.

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-16 --end-date 2024-10-18 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-16 --end-date 2024-10-18 --source-data-location s3 --sample-records --sample-proportion 0.2                                                                                      
2026-01-26 11:16:36,350 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 11:16:36,462 INFO [logger.py]: Not clearing any queues.
2026-01-26 11:16:36,477 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
2026-01-26 11:16:36,541 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 11:16:36,546 INFO [logger.py]: Listing S3 parquet URIs for dataset=fetch_posts_used_in_feeds, storage_tiers=['cache'], n_days=1.
2026-01-26 11:16:37,180 INFO [logger.py]: [dataset=fetch_posts_used_in_feeds tier=cache partition_date=2024-10-16] Found n_files=1 parquet objects.
2026-01-26 11:16:37,180 INFO [logger.py]: Listed total_parquet_files=1 for dataset=fetch_posts_used_in_feeds.
2026-01-26 11:16:38,874 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM fetch_posts_used_in_feeds', 'result_shape': {'rows': 89311, 'columns': 1}, 'result_memory_usage_mb': np.float64(10.817170143127441)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
...
2026-01-26 11:18:18,765 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-26 11:18:18,765 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-26 11:18:18,773 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 11:18:18,816 INFO [logger.py]: Writing 21323 items as 22 minibatches to DB.
2026-01-26 11:18:18,817 INFO [logger.py]: Writing 22 minibatches to DB as 1 batches...
2026-01-26 11:18:18,817 INFO [logger.py]: Processing batch 1/1...
2026-01-26 11:18:18,838 INFO [logger.py]: Inserted 21323 posts into queue for integration: ml_inference_intergroup
2026-01-26 11:18:18,840 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-26 11:18:18,840 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g  --max-records-per-run 10000`

Iteration 1: doing 10,000 out of the 21,000 records.

```bash
2026-01-26 11:35:46,060 INFO [logger.py]: Adding 360 posts to the output queue.
2026-01-26 11:35:46,061 INFO [logger.py]: Writing 360 items as 1 minibatches to DB.
2026-01-26 11:35:46,061 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 11:35:46,061 INFO [logger.py]: Processing batch 1/1...
2026-01-26 11:35:46,063 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 11:35:46,064 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|██████████████████████████████████████████████| 20/20 [14:29<00:00, 43.50s/batch, successful=9860, failed=0]
Execution time for run_batch_classification: 14 minutes, 31 seconds
Memory usage for run_batch_classification: -49.15625 MB
Execution time for orchestrate_classification: 14 minutes, 36 seconds
Memory usage for orchestrate_classification: -13.890625 MB
Execution time for classify_latest_posts: 14 minutes, 38 seconds
Memory usage for classify_latest_posts: -13.875 MB
2026-01-26 11:35:49,193 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 11:35:49,195 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 2: doing 12,000 records.

```bash
2026-01-26 11:53:41,319 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-26 11:53:41,320 INFO [logger.py]: Current queue size: 42 items
2026-01-26 11:53:41,321 INFO [logger.py]: Adding 150 posts to the output queue.
2026-01-26 11:53:41,321 INFO [logger.py]: Writing 150 items as 1 minibatches to DB.
2026-01-26 11:53:41,321 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 11:53:41,321 INFO [logger.py]: Processing batch 1/1...
2026-01-26 11:53:41,323 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 11:53:41,324 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 23/23 [15:37<00:00, 40.75s/batch, successful=11150, failed=0]
Execution time for run_batch_classification: 15 minutes, 38 seconds
Memory usage for run_batch_classification: -28.625 MB
Execution time for orchestrate_classification: 15 minutes, 43 seconds
Memory usage for orchestrate_classification: -4.0 MB
Execution time for classify_latest_posts: 15 minutes, 45 seconds
Memory usage for classify_latest_posts: -4.015625 MB
2026-01-26 11:53:44,439 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 11:53:44,440 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-26 11:57:28,454 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 11:57:28,559 INFO [logger.py]: Not clearing any queues.
2026-01-26 11:57:28,559 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-26 11:57:28,559 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-26 11:57:28,565 INFO [logger.py]: Current queue size: 43 items
2026-01-26 11:57:28,673 INFO [logger.py]: Exporting 21010 records to local storage for integration ml_inference_intergroup...
2026-01-26 11:57:28,714 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-13] Exporting n=1 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 11:57:28,796 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 11:57:28,799 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-14] Exporting n=3138 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 11:57:28,806 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 11:57:28,811 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-15] Exporting n=6102 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 11:57:28,820 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 11:57:28,827 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-16] Exporting n=9467 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 11:57:28,841 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 11:57:28,843 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-17] Exporting n=880 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 11:57:28,846 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 11:57:28,848 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-18] Exporting n=1422 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 11:57:28,852 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 11:57:28,853 INFO [logger.py]: Finished exporting 21010 records to local storage for integration ml_inference_intergroup...
2026-01-26 11:57:28,853 INFO [logger.py]: Successfully wrote 21010 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-26 12:20:44,980 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 12:20:45,084 INFO [logger.py]: Not clearing any queues.
2026-01-26 12:20:45,106 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-26 12:20:45,110 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-26 12:20:45,110 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-26 12:20:45,113 INFO [logger.py]: Registering 37 files for migration                                                           
2026-01-26 12:20:45,115 INFO [logger.py]: Registered 37 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (37 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 351.00it/s]
Finished initializing migration tracker db
2026-01-26 12:20:45,119 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-26 12:20:45,165 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/e9fc9e9dd77b421fb0d5ec28aab183df-0.parquet
2026-01-26 12:20:45,165 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/e9fc9e9dd77b421fb0d5ec28aab183df-0.parquet to S3 (0.10 MB)
2026-01-26 12:20:45,947 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/e9fc9e9dd77b421fb0d5ec28aab183df-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-17/e9fc9e9dd77b421fb0d5ec28aab183df-0.parquet
2026-01-26 12:20:45,950 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-17/e9fc9e9dd77b421fb0d5ec28aab183df-0.parquet
                                                                                                                                     2026-01-26 12:20:45,952 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/199665fd43344a7abafb018c4b9247b5-0.parquet
2026-01-26 12:20:45,952 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/199665fd43344a7abafb018c4b9247b5-0.parquet to S3 (1.15 MB)
2026-01-26 12:20:46,244 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/199665fd43344a7abafb018c4b9247b5-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-16/199665fd43344a7abafb018c4b9247b5-0.parquet
2026-01-26 12:20:46,249 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-16/199665fd43344a7abafb018c4b9247b5-0.parquet
                                                                                                                                     2026-01-26 12:20:46,254 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/bbf2d20cec994986911beb3f20279fd0-0.parquet
2026-01-26 12:20:46,254 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/bbf2d20cec994986911beb3f20279fd0-0.parquet to S3 (0.16 MB)
2026-01-26 12:20:46,326 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/bbf2d20cec994986911beb3f20279fd0-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-18/bbf2d20cec994986911beb3f20279fd0-0.parquet
2026-01-26 12:20:46,329 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/bbf2d20cec994986911beb3f20279fd0-0.parquet
2026-01-26 12:20:46,330 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/f443bffc2ee94201ad5d938fa30e4483-0.parquet
2026-01-26 12:20:46,330 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/f443bffc2ee94201ad5d938fa30e4483-0.parquet to S3 (0.38 MB)
2026-01-26 12:20:46,442 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/f443bffc2ee94201ad5d938fa30e4483-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-14/f443bffc2ee94201ad5d938fa30e4483-0.parquet
2026-01-26 12:20:46,444 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-14/f443bffc2ee94201ad5d938fa30e4483-0.parquet
                                                                                                                                     2026-01-26 12:20:46,446 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/0bbd39ded57944b48f65667df6937473-0.parquet
2026-01-26 12:20:46,446 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/0bbd39ded57944b48f65667df6937473-0.parquet to S3 (0.01 MB)
2026-01-26 12:20:46,502 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/0bbd39ded57944b48f65667df6937473-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-13/0bbd39ded57944b48f65667df6937473-0.parquet
2026-01-26 12:20:46,505 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-13/0bbd39ded57944b48f65667df6937473-0.parquet
2026-01-26 12:20:46,507 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/175c01d048fa4d04929367d3a008fbc8-0.parquet
2026-01-26 12:20:46,507 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/175c01d048fa4d04929367d3a008fbc8-0.parquet to S3 (0.71 MB)
2026-01-26 12:20:46,689 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/175c01d048fa4d04929367d3a008fbc8-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-15/175c01d048fa4d04929367d3a008fbc8-0.parquet
2026-01-26 12:20:46,697 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-15/175c01d048fa4d04929367d3a008fbc8-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 6/6 [00:01<00:00,  3.91it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:01<00:00,  1.30it/s]
```

- [ ] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-10-16	archive_ml_inference_intergroup:partition_date=2024-10-17	archive_ml_inference_intergroup:partition_date=2024-10-18
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-16
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-17
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-18
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.
```bash
#	label	_col1
1	1	28197
2	0	285534
3	-1	3
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
14	2024-10-14	72931
15	2024-10-15	46618
16	2024-10-16	9467
17	2024-10-17	880
18	2024-10-18	1422
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
43
sqlite> .exit
```

- [X] Delete input and output queues

## Week 4: 2024-10-19 to 2024-10-25

- [ ] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-19 --end-date 2024-10-25 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
2026-01-26 12:57:43,774 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-26 12:57:43,774 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-26 12:57:43,820 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 12:57:44,026 INFO [logger.py]: Writing 96874 items as 97 minibatches to DB.
2026-01-26 12:57:44,026 INFO [logger.py]: Writing 97 minibatches to DB as 4 batches...
2026-01-26 12:57:44,026 INFO [logger.py]: Processing batch 1/4...
2026-01-26 12:57:44,105 INFO [logger.py]: Inserted 96874 posts into queue for integration: ml_inference_intergroup
2026-01-26 12:57:44,111 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-26 12:57:44,111 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

Iteration 1: 20,000 records:

```bash
...
2026-01-26 13:29:04,991 INFO [logger.py]: Adding 152 posts to the output queue.
2026-01-26 13:29:04,994 INFO [logger.py]: Writing 152 items as 1 minibatches to DB.
2026-01-26 13:29:04,994 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 13:29:04,995 INFO [logger.py]: Processing batch 1/1...
2026-01-26 13:29:04,996 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 13:29:04,997 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 40/40 [28:06<00:00, 42.17s/batch, successful=19652, failed=0]
Execution time for run_batch_classification: 28 minutes, 8 seconds
Memory usage for run_batch_classification: -96.109375 MB
Execution time for orchestrate_classification: 28 minutes, 13 seconds
Memory usage for orchestrate_classification: -5.328125 MB
Execution time for classify_latest_posts: 28 minutes, 15 seconds
Memory usage for classify_latest_posts: -5.328125 MB
2026-01-26 13:29:08,138 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 13:29:08,140 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 2: 20,000 records:

```bash
2026-01-26 14:05:31,384 INFO [logger.py]: Adding 225 posts to the output queue.
2026-01-26 14:05:31,385 INFO [logger.py]: Writing 225 items as 1 minibatches to DB.
2026-01-26 14:05:31,385 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 14:05:31,385 INFO [logger.py]: Processing batch 1/1...
2026-01-26 14:05:31,387 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 14:05:31,387 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 40/40 [33:01<00:00, 49.55s/batch, successful=19725, failed=0]
Execution time for run_batch_classification: 33 minutes, 3 seconds
Memory usage for run_batch_classification: -91.40625 MB
Execution time for orchestrate_classification: 33 minutes, 8 seconds
Memory usage for orchestrate_classification: -16.078125 MB
Execution time for classify_latest_posts: 33 minutes, 10 seconds
Memory usage for classify_latest_posts: -15.984375 MB
2026-01-26 14:05:34,507 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 14:05:34,510 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 3: 60,000 records (so, the rest of the records)

```bash
...
2026-01-26 15:48:58,840 INFO [logger.py]: Current queue size: 192 items
2026-01-26 15:48:58,840 INFO [logger.py]: Adding 36 posts to the output queue.
2026-01-26 15:48:58,841 INFO [logger.py]: Writing 36 items as 1 minibatches to DB.
2026-01-26 15:48:58,844 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 15:48:58,844 INFO [logger.py]: Processing batch 1/1...
2026-01-26 15:48:58,845 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 15:48:58,845 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████| 113/113 [1:33:23<00:00, 49.59s/batch, successful=56036, failed=0]
Execution time for run_batch_classification: 93 minutes, 25 seconds
Memory usage for run_batch_classification: 22.109375 MB
Execution time for orchestrate_classification: 93 minutes, 30 seconds
Memory usage for orchestrate_classification: 12.734375 MB
Execution time for classify_latest_posts: 93 minutes, 32 seconds
Memory usage for classify_latest_posts: 12.734375 MB
2026-01-26 15:49:02,056 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 15:49:02,058 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-26 16:58:19,726 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 16:58:19,855 INFO [logger.py]: Not clearing any queues.
2026-01-26 16:58:19,855 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-26 16:58:19,856 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-26 16:58:19,877 INFO [logger.py]: Current queue size: 193 items
2026-01-26 16:58:20,368 INFO [logger.py]: Exporting 95413 records to local storage for integration ml_inference_intergroup...
2026-01-26 16:58:20,517 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-18] Exporting n=1066 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,562 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,569 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-19] Exporting n=8357 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,584 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,595 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-20] Exporting n=14180 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,614 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,625 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-21] Exporting n=14265 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,644 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,655 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-22] Exporting n=14366 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,674 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,688 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-23] Exporting n=17044 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,710 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,722 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-24] Exporting n=14381 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,742 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,752 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-25] Exporting n=11754 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 16:58:20,769 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 16:58:20,772 INFO [logger.py]: Finished exporting 95413 records to local storage for integration ml_inference_intergroup...
2026-01-26 16:58:20,775 INFO [logger.py]: Successfully wrote 95413 records to storage for integration ml_inference_intergroup
```

- [ ] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-26 16:59:04,653 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 16:59:04,748 INFO [logger.py]: Not clearing any queues.
2026-01-26 16:59:04,771 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-26 16:59:04,775 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-26 16:59:04,775 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-26 16:59:04,779 INFO [logger.py]: Registering 45 files for migration                                                           
2026-01-26 16:59:04,781 INFO [logger.py]: Registered 45 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (45 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 306.22it/s]
Finished initializing migration tracker db
2026-01-26 16:59:04,786 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-26 16:59:04,846 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/18819089e6304e589ed391a6f934fd50-0.parquet
2026-01-26 16:59:04,846 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/18819089e6304e589ed391a6f934fd50-0.parquet to S3 (1.66 MB)
2026-01-26 16:59:05,590 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/18819089e6304e589ed391a6f934fd50-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-21/18819089e6304e589ed391a6f934fd50-0.parquet
2026-01-26 16:59:05,594 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-21/18819089e6304e589ed391a6f934fd50-0.parquet
                                                                                                                                     2026-01-26 16:59:05,598 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/cbb57517415748b6a6c36b9c73d5d6b5-0.parquet
2026-01-26 16:59:05,598 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/cbb57517415748b6a6c36b9c73d5d6b5-0.parquet to S3 (0.92 MB)
2026-01-26 16:59:06,691 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/cbb57517415748b6a6c36b9c73d5d6b5-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-19/cbb57517415748b6a6c36b9c73d5d6b5-0.parquet
2026-01-26 16:59:06,693 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-19/cbb57517415748b6a6c36b9c73d5d6b5-0.parquet
                                                                                                                                     2026-01-26 16:59:06,695 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/ce216e7bbbbf418d877577e13e03a090-0.parquet
2026-01-26 16:59:06,695 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/ce216e7bbbbf418d877577e13e03a090-0.parquet to S3 (0.13 MB)
2026-01-26 16:59:07,739 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/ce216e7bbbbf418d877577e13e03a090-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-18/ce216e7bbbbf418d877577e13e03a090-0.parquet
2026-01-26 16:59:07,741 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-18/ce216e7bbbbf418d877577e13e03a090-0.parquet
                                                                                                                                     2026-01-26 16:59:07,743 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/0d960d2ea0f044369259b8680a64e62e-0.parquet
2026-01-26 16:59:07,743 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/0d960d2ea0f044369259b8680a64e62e-0.parquet to S3 (1.57 MB)
2026-01-26 16:59:08,110 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/0d960d2ea0f044369259b8680a64e62e-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-20/0d960d2ea0f044369259b8680a64e62e-0.parquet
2026-01-26 16:59:08,113 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-20/0d960d2ea0f044369259b8680a64e62e-0.parquet
                                                                                                                                     2026-01-26 16:59:08,114 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/57867af7baf949768b49354da17aa3ab-0.parquet
2026-01-26 16:59:08,114 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/57867af7baf949768b49354da17aa3ab-0.parquet to S3 (1.36 MB)
2026-01-26 16:59:08,596 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/57867af7baf949768b49354da17aa3ab-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-25/57867af7baf949768b49354da17aa3ab-0.parquet
2026-01-26 16:59:08,597 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/57867af7baf949768b49354da17aa3ab-0.parquet
                                                                                                                                     2026-01-26 16:59:08,598 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/1cf746bb5d244dbf8af796f7544bede6-0.parquet
2026-01-26 16:59:08,598 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/1cf746bb5d244dbf8af796f7544bede6-0.parquet to S3 (1.65 MB)
2026-01-26 16:59:09,122 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/1cf746bb5d244dbf8af796f7544bede6-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-22/1cf746bb5d244dbf8af796f7544bede6-0.parquet
2026-01-26 16:59:09,124 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-22/1cf746bb5d244dbf8af796f7544bede6-0.parquet
                                                                                                                                     2026-01-26 16:59:09,125 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/931547773c014466a683004e65ccbc31-0.parquet
2026-01-26 16:59:09,125 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/931547773c014466a683004e65ccbc31-0.parquet to S3 (1.95 MB)
2026-01-26 16:59:10,258 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/931547773c014466a683004e65ccbc31-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-23/931547773c014466a683004e65ccbc31-0.parquet
2026-01-26 16:59:10,260 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-23/931547773c014466a683004e65ccbc31-0.parquet
                                                                                                                                     2026-01-26 16:59:10,262 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/cf10d665bc1347a4a1a2f5700dc1c8bc-0.parquet
2026-01-26 16:59:10,262 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/cf10d665bc1347a4a1a2f5700dc1c8bc-0.parquet to S3 (1.67 MB)
2026-01-26 16:59:10,754 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/cf10d665bc1347a4a1a2f5700dc1c8bc-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-24/cf10d665bc1347a4a1a2f5700dc1c8bc-0.parquet
2026-01-26 16:59:10,755 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-24/cf10d665bc1347a4a1a2f5700dc1c8bc-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 8/8 [00:05<00:00,  1.35it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:05<00:00,  2.96s/it]
```

- [ ] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-10-19	archive_ml_inference_intergroup:partition_date=2024-10-20	archive_ml_inference_intergroup:partition_date=2024-10-21	archive_ml_inference_intergroup:partition_date=2024-10-22	archive_ml_inference_intergroup:partition_date=2024-10-23	archive_ml_inference_intergroup:partition_date=2024-10-24	archive_ml_inference_intergroup:partition_date=2024-10-25
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-19
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-20
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-21
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-22
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-23
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-24
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-25
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.
```bash
#	label	_col1
1	0	380766
2	-1	15
3	1	28341
```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```bash
#	partition_date	total_labels
1	2024-10-19	8357
2	2024-10-23	17044
3	2024-10-18	2463
4	2024-10-13	68387
5	2024-10-07	45
6	2024-09-29	1503
7	2024-10-06	393
8	2024-10-10	4007
9	2024-10-20	14180
10	2024-10-14	72931
11	2024-10-25	11754
12	2024-10-24	14381
13	2024-10-16	9467
14	2024-10-02	2591
15	2024-10-04	903
16	2024-10-12	65829
17	2024-10-15	46618
18	2024-10-01	3726
19	2024-10-17	880
20	2024-10-05	325
21	2024-10-11	27844
22	2024-09-30	3894
23	2024-10-21	14265
24	2024-10-22	14366
25	2024-10-03	2982
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
193
sqlite> .exit
```

- [X] Delete input and output queues


## Week 5: 2024-10-26 to 2024-11-01

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-10-26 --end-date 2024-11-01 --source-data-location s3 --sample-records --sample-proportion 0.2`
- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

Ran this. Here's the last batch:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 20000
2026-01-26 18:53:50,448 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 18:53:50,550 INFO [logger.py]: Not clearing any queues.
2026-01-26 18:53:50,550 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-26 18:53:50,550 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-26 18:53:57,341 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 18:53:57,342 INFO [logger.py]: Current queue size: 13 items
2026-01-26 18:53:57,390 INFO [logger.py]: Loaded 12526 posts to classify.
2026-01-26 18:53:57,390 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-26 18:53:57,390 INFO [logger.py]: Latest inference timestamp: None
2026-01-26 18:53:57,395 INFO [logger.py]: After dropping duplicates, 12526 posts remain.
2026-01-26 18:53:57,403 INFO [logger.py]: After filtering, 12351 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 30.640625 MB
2026-01-26 18:53:58,568 INFO [logger.py]: Classifying 12351 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/25 [00:00<?, ?batch/s]2026-01-26 18:53:59,632 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/25...

...

2026-01-26 19:11:13,089 INFO [logger.py]: Current queue size: 184 items
2026-01-26 19:11:13,090 INFO [logger.py]: Adding 351 posts to the output queue.
2026-01-26 19:11:13,090 INFO [logger.py]: Writing 351 items as 1 minibatches to DB.
2026-01-26 19:11:13,090 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 19:11:13,090 INFO [logger.py]: Processing batch 1/1...
2026-01-26 19:11:13,092 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 19:11:13,093 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 25/25 [17:13<00:00, 41.34s/batch, successful=12351, failed=0]
Execution time for run_batch_classification: 17 minutes, 15 seconds
Memory usage for run_batch_classification: -18.59375 MB
Execution time for orchestrate_classification: 17 minutes, 19 seconds
Memory usage for orchestrate_classification: 0.125 MB
Execution time for classify_latest_posts: 17 minutes, 21 seconds
Memory usage for classify_latest_posts: 0.109375 MB
2026-01-26 19:11:16,295 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 19:11:16,297 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-26 19:22:11,821 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 19:22:11,927 INFO [logger.py]: Not clearing any queues.
2026-01-26 19:22:11,928 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-26 19:22:11,928 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-26 19:22:11,945 INFO [logger.py]: Current queue size: 185 items
2026-01-26 19:22:12,407 INFO [logger.py]: Exporting 91139 records to local storage for integration ml_inference_intergroup...
2026-01-26 19:22:12,548 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-25] Exporting n=3986 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,646 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,656 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-26] Exporting n=12553 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,672 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,682 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-27] Exporting n=12020 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,720 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,729 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-28] Exporting n=9431 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,742 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,753 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-29] Exporting n=13752 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,771 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,781 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-30] Exporting n=12795 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,798 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,809 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-10-31] Exporting n=14617 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,830 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,840 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-01] Exporting n=11985 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 19:22:12,856 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 19:22:12,859 INFO [logger.py]: Finished exporting 91139 records to local storage for integration ml_inference_intergroup...
2026-01-26 19:22:12,862 INFO [logger.py]: Successfully wrote 91139 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-26 19:22:52,502 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 19:22:52,565 INFO [logger.py]: Not clearing any queues.
2026-01-26 19:22:52,589 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-26 19:22:52,592 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-26 19:22:52,592 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-26 19:22:52,596 INFO [logger.py]: Registering 53 files for migration                                                           
2026-01-26 19:22:52,605 INFO [logger.py]: Registered 53 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (53 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 151.54it/s]
Finished initializing migration tracker db
2026-01-26 19:22:52,609 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-26 19:22:52,674 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/ba773bc21be34c0dbe164e1967f4c561-0.parquet
2026-01-26 19:22:52,674 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/ba773bc21be34c0dbe164e1967f4c561-0.parquet to S3 (1.41 MB)
2026-01-26 19:22:53,247 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/ba773bc21be34c0dbe164e1967f4c561-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-26/ba773bc21be34c0dbe164e1967f4c561-0.parquet
2026-01-26 19:22:53,248 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-26/ba773bc21be34c0dbe164e1967f4c561-0.parquet
                                                                                                                                     2026-01-26 19:22:53,249 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/d1eac83c1c76423db700492faf07b6c3-0.parquet
2026-01-26 19:22:53,249 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/d1eac83c1c76423db700492faf07b6c3-0.parquet to S3 (1.10 MB)
2026-01-26 19:22:53,639 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/d1eac83c1c76423db700492faf07b6c3-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-28/d1eac83c1c76423db700492faf07b6c3-0.parquet
2026-01-26 19:22:53,641 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-28/d1eac83c1c76423db700492faf07b6c3-0.parquet
                                                                                                                                     2026-01-26 19:22:53,642 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/3c6bf289f5104c2ca7190b6d398e6ab2-0.parquet
2026-01-26 19:22:53,642 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/3c6bf289f5104c2ca7190b6d398e6ab2-0.parquet to S3 (1.58 MB)
^[[C2026-01-26 19:22:53,999 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/3c6bf289f5104c2ca7190b6d398e6ab2-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-29/3c6bf289f5104c2ca7190b6d398e6ab2-0.parquet
2026-01-26 19:22:54,000 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-29/3c6bf289f5104c2ca7190b6d398e6ab2-0.parquet
                                                                                                                                     2026-01-26 19:22:54,001 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/205c6c7638fc4639b711f400bb74770a-0.parquet
2026-01-26 19:22:54,001 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/205c6c7638fc4639b711f400bb74770a-0.parquet to S3 (1.35 MB)
2026-01-26 19:22:54,429 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/205c6c7638fc4639b711f400bb74770a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-27/205c6c7638fc4639b711f400bb74770a-0.parquet
2026-01-26 19:22:54,430 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-27/205c6c7638fc4639b711f400bb74770a-0.parquet
                                                                                                                                     2026-01-26 19:22:54,432 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/e5e395b3fc634a4c9d42c92b229f7a41-0.parquet
2026-01-26 19:22:54,432 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/e5e395b3fc634a4c9d42c92b229f7a41-0.parquet to S3 (0.46 MB)
2026-01-26 19:22:54,559 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/e5e395b3fc634a4c9d42c92b229f7a41-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-25/e5e395b3fc634a4c9d42c92b229f7a41-0.parquet
2026-01-26 19:22:54,562 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-25/e5e395b3fc634a4c9d42c92b229f7a41-0.parquet
                                                                                                                                     2026-01-26 19:22:54,564 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/626ab22b8c374a1a8c7a685d7c22ffea-0.parquet
2026-01-26 19:22:54,565 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/626ab22b8c374a1a8c7a685d7c22ffea-0.parquet to S3 (1.36 MB)
2026-01-26 19:22:54,885 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/626ab22b8c374a1a8c7a685d7c22ffea-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-01/626ab22b8c374a1a8c7a685d7c22ffea-0.parquet
2026-01-26 19:22:54,892 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/626ab22b8c374a1a8c7a685d7c22ffea-0.parquet
                                                                                                                                     2026-01-26 19:22:54,893 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/41c1b5194ebe4ce9a3ac0793bc6c52e5-0.parquet
2026-01-26 19:22:54,893 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/41c1b5194ebe4ce9a3ac0793bc6c52e5-0.parquet to S3 (1.49 MB)
2026-01-26 19:22:55,351 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/41c1b5194ebe4ce9a3ac0793bc6c52e5-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-30/41c1b5194ebe4ce9a3ac0793bc6c52e5-0.parquet
2026-01-26 19:22:55,363 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-30/41c1b5194ebe4ce9a3ac0793bc6c52e5-0.parquet
                                                                                                                                     2026-01-26 19:22:55,364 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/1e48b8ae1b374b6fa5545321c3e30401-0.parquet
2026-01-26 19:22:55,365 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/1e48b8ae1b374b6fa5545321c3e30401-0.parquet to S3 (1.62 MB)
2026-01-26 19:22:55,856 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/1e48b8ae1b374b6fa5545321c3e30401-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-10-31/1e48b8ae1b374b6fa5545321c3e30401-0.parquet
2026-01-26 19:22:55,858 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-10-31/1e48b8ae1b374b6fa5545321c3e30401-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 8/8 [00:03<00:00,  2.51it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:03<00:00,  1.59s/it]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-10-26	archive_ml_inference_intergroup:partition_date=2024-10-27	archive_ml_inference_intergroup:partition_date=2024-10-28	archive_ml_inference_intergroup:partition_date=2024-10-29	archive_ml_inference_intergroup:partition_date=2024-10-30	archive_ml_inference_intergroup:partition_date=2024-10-31	archive_ml_inference_intergroup:partition_date=2024-11-01
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-26
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-27
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-28
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-29
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-30
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-10-31
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-01
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.

```bash
#	label	_col1
1	1	28460
2	-1	31
3	0	471508
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
14	2024-10-14	72931
15	2024-10-15	46618
16	2024-10-16	9467
17	2024-10-17	880
18	2024-10-18	2463
19	2024-10-19	8357
20	2024-10-20	14180
21	2024-10-21	14265
22	2024-10-22	14366
23	2024-10-23	17044
24	2024-10-24	14381
25	2024-10-25	15478
26	2024-10-26	12553
27	2024-10-27	12020
28	2024-10-28	9431
29	2024-10-29	13752
30	2024-10-30	12795
31	2024-10-31	14617
32	2024-11-01	11985
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
185
sqlite> .exit
```

- [X] Delete input and output queues

## Week 6: 2024-11-02 to 2024-11-08

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-02 --end-date 2024-11-08 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
...
2026-01-26 19:43:07,159 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-26 19:43:07,203 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 19:43:07,401 INFO [logger.py]: Writing 93761 items as 94 minibatches to DB.
2026-01-26 19:43:07,401 INFO [logger.py]: Writing 94 minibatches to DB as 4 batches...
2026-01-26 19:43:07,401 INFO [logger.py]: Processing batch 1/4...
2026-01-26 19:43:07,512 INFO [logger.py]: Inserted 93761 posts into queue for integration: ml_inference_intergroup
2026-01-26 19:43:07,518 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-26 19:43:07,518 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 20000`

Iteration 1: 20,000

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 20000
2026-01-26 19:45:27,184 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 19:45:27,284 INFO [logger.py]: Not clearing any queues.
2026-01-26 19:45:27,284 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-26 19:45:27,284 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-26 19:45:34,130 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 19:45:34,132 INFO [logger.py]: Current queue size: 94 items
2026-01-26 19:45:34,425 INFO [logger.py]: Loaded 93761 posts to classify.
2026-01-26 19:45:34,426 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-26 19:45:34,426 INFO [logger.py]: Latest inference timestamp: None
2026-01-26 19:45:34,476 INFO [logger.py]: After dropping duplicates, 93761 posts remain.
2026-01-26 19:45:34,546 INFO [logger.py]: After filtering, 92400 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 209.078125 MB
2026-01-26 19:45:36,043 INFO [logger.py]: Limited posts from 92400 to 19705 (max_records_per_run=20000, included 20 complete batches)
2026-01-26 19:45:36,064 INFO [logger.py]: Classifying 19705 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/40 [00:00<?, ?batch/s]2026-01-26 19:45:37,131 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/40...

...

2026-01-26 20:13:51,026 INFO [logger.py]: Adding 205 posts to the output queue.
2026-01-26 20:13:51,026 INFO [logger.py]: Writing 205 items as 1 minibatches to DB.
2026-01-26 20:13:51,026 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 20:13:51,027 INFO [logger.py]: Processing batch 1/1...
2026-01-26 20:13:51,029 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 20:13:51,029 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████████| 40/40 [28:13<00:00, 42.35s/batch, successful=19705, failed=0]
Execution time for run_batch_classification: 28 minutes, 15 seconds
Memory usage for run_batch_classification: -75.59375 MB
Execution time for orchestrate_classification: 28 minutes, 20 seconds
Memory usage for orchestrate_classification: 3.359375 MB
Execution time for classify_latest_posts: 28 minutes, 22 seconds
Memory usage for classify_latest_posts: 3.34375 MB
2026-01-26 20:13:54,222 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 20:13:54,224 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

Iteration 2: I decided to do the rest, so 80,000.

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 80000
2026-01-26 20:14:39,793 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 20:14:39,894 INFO [logger.py]: Not clearing any queues.
2026-01-26 20:14:39,894 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-26 20:14:39,894 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-26 20:14:46,464 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 20:14:46,471 INFO [logger.py]: Current queue size: 74 items
2026-01-26 20:14:46,737 INFO [logger.py]: Loaded 73761 posts to classify.
2026-01-26 20:14:46,737 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-26 20:14:46,737 INFO [logger.py]: Latest inference timestamp: None
2026-01-26 20:14:46,772 INFO [logger.py]: After dropping duplicates, 73761 posts remain.
2026-01-26 20:14:46,833 INFO [logger.py]: After filtering, 72695 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 162.640625 MB
2026-01-26 20:14:48,160 INFO [logger.py]: Classifying 72695 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/146 [00:00<?, ?batch/s]2026-01-26 20:14:49,236 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/146...

...

2026-01-26 22:00:31,710 INFO [logger.py]: Adding 195 posts to the output queue.
2026-01-26 22:00:31,711 INFO [logger.py]: Writing 195 items as 1 minibatches to DB.
2026-01-26 22:00:31,711 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-26 22:00:31,711 INFO [logger.py]: Processing batch 1/1...
2026-01-26 22:00:31,713 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-26 22:00:31,714 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████| 146/146 [1:45:42<00:00, 43.44s/batch, successful=72695, failed=0]
Execution time for run_batch_classification: 105 minutes, 44 seconds
Memory usage for run_batch_classification: -21.984375 MB
Execution time for orchestrate_classification: 105 minutes, 48 seconds
Memory usage for orchestrate_classification: 66.671875 MB
Execution time for classify_latest_posts: 105 minutes, 51 seconds
Memory usage for classify_latest_posts: 66.78125 MB
2026-01-26 22:00:34,973 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-26 22:00:34,975 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-26 22:47:22,819 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 22:47:22,925 INFO [logger.py]: Not clearing any queues.
2026-01-26 22:47:22,925 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-26 22:47:22,925 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-26 22:47:22,944 INFO [logger.py]: Current queue size: 186 items
2026-01-26 22:47:23,403 INFO [logger.py]: Exporting 92400 records to local storage for integration ml_inference_intergroup...
2026-01-26 22:47:23,547 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-01] Exporting n=2278 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,655 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,663 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-02] Exporting n=10527 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,680 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,691 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-03] Exporting n=12539 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,708 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,719 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-04] Exporting n=12404 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,736 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,746 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-05] Exporting n=13033 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,764 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,777 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-06] Exporting n=16866 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,798 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,808 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-07] Exporting n=11474 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,826 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,837 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-08] Exporting n=13279 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-26 22:47:23,854 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-26 22:47:23,858 INFO [logger.py]: Finished exporting 92400 records to local storage for integration ml_inference_intergroup...
2026-01-26 22:47:23,861 INFO [logger.py]: Successfully wrote 92400 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-26 22:47:44,959 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-26 22:47:45,051 INFO [logger.py]: Not clearing any queues.
2026-01-26 22:47:45,074 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-26 22:47:45,078 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-26 22:47:45,079 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-26 22:47:45,083 INFO [logger.py]: Registering 61 files for migration                                                           
2026-01-26 22:47:45,086 INFO [logger.py]: Registered 61 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (61 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 257.96it/s]
Finished initializing migration tracker db
2026-01-26 22:47:45,092 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-26 22:47:45,137 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/e11feef5ea5844a2a8f131c9006a99d2-0.parquet
2026-01-26 22:47:45,137 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/e11feef5ea5844a2a8f131c9006a99d2-0.parquet to S3 (1.42 MB)
2026-01-26 22:47:45,771 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/e11feef5ea5844a2a8f131c9006a99d2-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-04/e11feef5ea5844a2a8f131c9006a99d2-0.parquet
2026-01-26 22:47:45,773 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-04/e11feef5ea5844a2a8f131c9006a99d2-0.parquet
                                                                                                                                     2026-01-26 22:47:45,775 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/012d989fb2c94759a1a3dbe4c04a6011-0.parquet
2026-01-26 22:47:45,775 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/012d989fb2c94759a1a3dbe4c04a6011-0.parquet to S3 (1.42 MB)
2026-01-26 22:47:46,230 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/012d989fb2c94759a1a3dbe4c04a6011-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-03/012d989fb2c94759a1a3dbe4c04a6011-0.parquet
2026-01-26 22:47:46,231 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-03/012d989fb2c94759a1a3dbe4c04a6011-0.parquet
                                                                                                                                     2026-01-26 22:47:46,232 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/50c0409a2cf648d3aa6816f097b841a8-0.parquet
2026-01-26 22:47:46,232 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/50c0409a2cf648d3aa6816f097b841a8-0.parquet to S3 (1.19 MB)
2026-01-26 22:47:46,677 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/50c0409a2cf648d3aa6816f097b841a8-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-02/50c0409a2cf648d3aa6816f097b841a8-0.parquet
2026-01-26 22:47:46,678 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-02/50c0409a2cf648d3aa6816f097b841a8-0.parquet
                                                                                                                                     2026-01-26 22:47:46,679 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/0cc5aab3faee44cc9a065d49eb7daf96-0.parquet
2026-01-26 22:47:46,679 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/0cc5aab3faee44cc9a065d49eb7daf96-0.parquet to S3 (1.47 MB)
2026-01-26 22:47:47,586 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/0cc5aab3faee44cc9a065d49eb7daf96-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-05/0cc5aab3faee44cc9a065d49eb7daf96-0.parquet
2026-01-26 22:47:47,588 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-05/0cc5aab3faee44cc9a065d49eb7daf96-0.parquet
                                                                                                                                     2026-01-26 22:47:47,589 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/8d652bbec0bd4533a921c9379578d941-0.parquet
2026-01-26 22:47:47,590 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/8d652bbec0bd4533a921c9379578d941-0.parquet to S3 (1.33 MB)
2026-01-26 22:47:48,076 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/8d652bbec0bd4533a921c9379578d941-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-07/8d652bbec0bd4533a921c9379578d941-0.parquet
2026-01-26 22:47:48,081 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/8d652bbec0bd4533a921c9379578d941-0.parquet
                                                                                                                                     2026-01-26 22:47:48,085 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/9e8f30e9674644efb2bb4e7a421d67a8-0.parquet
2026-01-26 22:47:48,085 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/9e8f30e9674644efb2bb4e7a421d67a8-0.parquet to S3 (1.89 MB)
2026-01-26 22:47:48,643 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/9e8f30e9674644efb2bb4e7a421d67a8-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-06/9e8f30e9674644efb2bb4e7a421d67a8-0.parquet
2026-01-26 22:47:48,646 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-06/9e8f30e9674644efb2bb4e7a421d67a8-0.parquet
                                                                                                                                     2026-01-26 22:47:48,648 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/03192117c35e4e719135f1af1d246f40-0.parquet
2026-01-26 22:47:48,648 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/03192117c35e4e719135f1af1d246f40-0.parquet to S3 (0.27 MB)
2026-01-26 22:47:48,736 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/03192117c35e4e719135f1af1d246f40-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-01/03192117c35e4e719135f1af1d246f40-0.parquet
2026-01-26 22:47:48,739 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-01/03192117c35e4e719135f1af1d246f40-0.parquet
2026-01-26 22:47:48,740 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/0f0e11fd754b4c188ce2e0c100a7edc6-0.parquet
2026-01-26 22:47:48,741 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/0f0e11fd754b4c188ce2e0c100a7edc6-0.parquet to S3 (1.53 MB)
2026-01-26 22:47:49,096 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/0f0e11fd754b4c188ce2e0c100a7edc6-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-08/0f0e11fd754b4c188ce2e0c100a7edc6-0.parquet
2026-01-26 22:47:49,101 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/0f0e11fd754b4c188ce2e0c100a7edc6-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 8/8 [00:03<00:00,  2.02it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:03<00:00,  1.98s/it]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-11-02	archive_ml_inference_intergroup:partition_date=2024-11-03	archive_ml_inference_intergroup:partition_date=2024-11-04	archive_ml_inference_intergroup:partition_date=2024-11-05	archive_ml_inference_intergroup:partition_date=2024-11-06	archive_ml_inference_intergroup:partition_date=2024-11-07	archive_ml_inference_intergroup:partition_date=2024-11-08
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-02
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-03
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-04
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-05
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-06
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-07
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-08
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.

```bash
#	label	_col1
1	1	28563
2	0	563520
3	-1	49
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
14	2024-10-14	72931
15	2024-10-15	46618
16	2024-10-16	9467
17	2024-10-17	880
18	2024-10-18	2463
19	2024-10-19	8357
20	2024-10-20	14180
21	2024-10-21	14265
22	2024-10-22	14366
23	2024-10-23	17044
24	2024-10-24	14381
25	2024-10-25	15478
26	2024-10-26	12553
27	2024-10-27	12020
28	2024-10-28	9431
29	2024-10-29	13752
30	2024-10-30	12795
31	2024-10-31	14617
32	2024-11-01	13996
33	2024-11-02	10527
34	2024-11-03	12539
35	2024-11-04	12404
36	2024-11-05	13033
37	2024-11-06	16866
38	2024-11-07	11474
39	2024-11-08	13279
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db                                                    
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
186
sqlite> .exit
```

- [X] Delete input and output queues

## Week 7: 2024-11-09 to 2024-11-15

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-09 --end-date 2024-11-15 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
...
2026-01-26 23:10:51,585 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-26 23:10:51,585 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-26 23:10:51,652 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-26 23:10:51,904 INFO [logger.py]: Writing 110987 items as 111 minibatches to DB.
2026-01-26 23:10:51,905 INFO [logger.py]: Writing 111 minibatches to DB as 5 batches...
2026-01-26 23:10:51,905 INFO [logger.py]: Processing batch 1/5...
2026-01-26 23:10:52,004 INFO [logger.py]: Inserted 110987 posts into queue for integration: ml_inference_intergroup
2026-01-26 23:10:52,012 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-26 23:10:52,012 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

Decided I'd just run all of it, overnight:

```bash
...
2026-01-27 01:35:27,494 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-27 01:35:27,496 INFO [logger.py]: Current queue size: 218 items
2026-01-27 01:35:27,496 INFO [logger.py]: Adding 219 posts to the output queue.
2026-01-27 01:35:27,497 INFO [logger.py]: Writing 219 items as 1 minibatches to DB.
2026-01-27 01:35:27,497 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-27 01:35:27,497 INFO [logger.py]: Processing batch 1/1...
2026-01-27 01:35:27,499 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-27 01:35:27,500 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|████████████████████████████████████████| 219/219 [2:21:27<00:00, 38.75s/batch, successful=109219, failed=0]
Execution time for run_batch_classification: 141 minutes, 28 seconds
Memory usage for run_batch_classification: -85.3125 MB
Execution time for orchestrate_classification: 141 minutes, 34 seconds
Memory usage for orchestrate_classification: 64.125 MB
Execution time for classify_latest_posts: 141 minutes, 36 seconds
Memory usage for classify_latest_posts: 64.109375 MB
2026-01-27 01:35:30,770 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-27 01:35:30,772 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-27 09:26:07,793 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-27 09:26:07,911 INFO [logger.py]: Not clearing any queues.
2026-01-27 09:26:07,911 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-27 09:26:07,912 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-27 09:26:07,934 INFO [logger.py]: Current queue size: 219 items
2026-01-27 09:26:08,501 INFO [logger.py]: Exporting 109219 records to local storage for integration ml_inference_intergroup...
2026-01-27 09:26:08,687 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-07] Exporting n=393 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,786 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,790 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-08] Exporting n=4454 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,799 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,810 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-09] Exporting n=14371 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,829 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,840 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-10] Exporting n=14128 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,858 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,871 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-11] Exporting n=16128 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,892 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,904 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-12] Exporting n=15433 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,925 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,940 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-13] Exporting n=17374 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:08,965 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:08,978 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-14] Exporting n=15071 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:09,000 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:09,011 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-15] Exporting n=11867 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 09:26:09,028 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 09:26:09,032 INFO [logger.py]: Finished exporting 109219 records to local storage for integration ml_inference_intergroup...
2026-01-27 09:26:09,036 INFO [logger.py]: Successfully wrote 109219 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-27 09:27:10,168 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-27 09:27:10,231 INFO [logger.py]: Not clearing any queues.
2026-01-27 09:27:10,253 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-27 09:27:10,257 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-27 09:27:10,257 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-27 09:27:10,262 INFO [logger.py]: Registering 70 files for migration                                                           
2026-01-27 09:27:10,266 INFO [logger.py]: Registered 70 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (70 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 221.32it/s]
Finished initializing migration tracker db
2026-01-27 09:27:10,270 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-27 09:27:10,334 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/8e5808723d3d403098b6bfedaebebc8a-0.parquet
2026-01-27 09:27:10,334 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/8e5808723d3d403098b6bfedaebebc8a-0.parquet to S3 (1.81 MB)
2026-01-27 09:27:11,027 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/8e5808723d3d403098b6bfedaebebc8a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-11/8e5808723d3d403098b6bfedaebebc8a-0.parquet
2026-01-27 09:27:11,031 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-11/8e5808723d3d403098b6bfedaebebc8a-0.parquet
                                                                                                                                     2026-01-27 09:27:11,032 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/b51093c6b6804e0a8aa54ab7845e3425-0.parquet
2026-01-27 09:27:11,032 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/b51093c6b6804e0a8aa54ab7845e3425-0.parquet to S3 (1.60 MB)
2026-01-27 09:27:11,512 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/b51093c6b6804e0a8aa54ab7845e3425-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-10/b51093c6b6804e0a8aa54ab7845e3425-0.parquet
2026-01-27 09:27:11,516 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-10/b51093c6b6804e0a8aa54ab7845e3425-0.parquet
                                                                                                                                     2026-01-27 09:27:11,521 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/ebce3d1e9e104cec9769b9145d78aafb-0.parquet
2026-01-27 09:27:11,521 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/ebce3d1e9e104cec9769b9145d78aafb-0.parquet to S3 (1.73 MB)
2026-01-27 09:27:11,919 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/ebce3d1e9e104cec9769b9145d78aafb-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-12/ebce3d1e9e104cec9769b9145d78aafb-0.parquet
2026-01-27 09:27:11,922 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-12/ebce3d1e9e104cec9769b9145d78aafb-0.parquet
                                                                                                                                     2026-01-27 09:27:11,926 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a231f033d0514fb9a630cc87d9418bf7-0.parquet
2026-01-27 09:27:11,926 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a231f033d0514fb9a630cc87d9418bf7-0.parquet to S3 (1.30 MB)
2026-01-27 09:27:12,248 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a231f033d0514fb9a630cc87d9418bf7-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a231f033d0514fb9a630cc87d9418bf7-0.parquet
2026-01-27 09:27:12,251 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a231f033d0514fb9a630cc87d9418bf7-0.parquet
                                                                                                                                     2026-01-27 09:27:12,253 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/59f1cb0e37bc47fe9faa07dbaf22de04-0.parquet
2026-01-27 09:27:12,253 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/59f1cb0e37bc47fe9faa07dbaf22de04-0.parquet to S3 (1.65 MB)
2026-01-27 09:27:12,649 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/59f1cb0e37bc47fe9faa07dbaf22de04-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-14/59f1cb0e37bc47fe9faa07dbaf22de04-0.parquet
2026-01-27 09:27:12,650 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/59f1cb0e37bc47fe9faa07dbaf22de04-0.parquet
                                                                                                                                     2026-01-27 09:27:12,651 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/2b1d3f649c544aa9a81540751d084e6e-0.parquet
2026-01-27 09:27:12,651 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/2b1d3f649c544aa9a81540751d084e6e-0.parquet to S3 (1.93 MB)
2026-01-27 09:27:13,081 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/2b1d3f649c544aa9a81540751d084e6e-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-13/2b1d3f649c544aa9a81540751d084e6e-0.parquet
2026-01-27 09:27:13,083 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-13/2b1d3f649c544aa9a81540751d084e6e-0.parquet
                                                                                                                                     2026-01-27 09:27:13,084 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/392f0a3de0174d7b97fccc4a502799d6-0.parquet
2026-01-27 09:27:13,084 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/392f0a3de0174d7b97fccc4a502799d6-0.parquet to S3 (1.60 MB)
2026-01-27 09:27:13,701 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/392f0a3de0174d7b97fccc4a502799d6-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-09/392f0a3de0174d7b97fccc4a502799d6-0.parquet
2026-01-27 09:27:13,705 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-09/392f0a3de0174d7b97fccc4a502799d6-0.parquet
                                                                                                                                     2026-01-27 09:27:13,709 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/9b8f2a49e5594ce9a488de3343623069-0.parquet
2026-01-27 09:27:13,710 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/9b8f2a49e5594ce9a488de3343623069-0.parquet to S3 (0.05 MB)
2026-01-27 09:27:13,771 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/9b8f2a49e5594ce9a488de3343623069-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-07/9b8f2a49e5594ce9a488de3343623069-0.parquet
2026-01-27 09:27:13,774 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-07/9b8f2a49e5594ce9a488de3343623069-0.parquet
2026-01-27 09:27:13,776 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/bec2ebc9f9744020b49ad936ca475933-0.parquet
2026-01-27 09:27:13,776 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/bec2ebc9f9744020b49ad936ca475933-0.parquet to S3 (0.53 MB)
2026-01-27 09:27:14,042 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/bec2ebc9f9744020b49ad936ca475933-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-08/bec2ebc9f9744020b49ad936ca475933-0.parquet
2026-01-27 09:27:14,046 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-08/bec2ebc9f9744020b49ad936ca475933-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 9/9 [00:03<00:00,  2.42it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:03<00:00,  1.86s/it]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-11-09	archive_ml_inference_intergroup:partition_date=2024-11-10	archive_ml_inference_intergroup:partition_date=2024-11-11	archive_ml_inference_intergroup:partition_date=2024-11-12	archive_ml_inference_intergroup:partition_date=2024-11-13	archive_ml_inference_intergroup:partition_date=2024-11-14	archive_ml_inference_intergroup:partition_date=2024-11-15
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-09
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-10
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-11
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-12
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-13
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-14
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-15
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
1	1	28683
2	-1	68
3	0	672289
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
14	2024-10-14	72931
15	2024-10-15	46618
16	2024-10-16	9467
17	2024-10-17	880
18	2024-10-18	2463
19	2024-10-19	8357
20	2024-10-20	14180
21	2024-10-21	14265
22	2024-10-22	14366
23	2024-10-23	17044
24	2024-10-24	14381
25	2024-10-25	15478
26	2024-10-26	12553
27	2024-10-27	12020
28	2024-10-28	9431
29	2024-10-29	13752
30	2024-10-30	12795
31	2024-10-31	14617
32	2024-11-01	13996
33	2024-11-02	10527
34	2024-11-03	12539
35	2024-11-04	12404
36	2024-11-05	13033
37	2024-11-06	16866
38	2024-11-07	11799
39	2024-11-08	17490
40	2024-11-09	14371
41	2024-11-10	14128
42	2024-11-11	16128
43	2024-11-12	15433
44	2024-11-13	17374
45	2024-11-14	15071
46	2024-11-15	11867
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
219
sqlite> .exit
```

- [X] Delete input and output queues

## Week 8: 2024-11-16 to 2024-11-22

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-16 --end-date 2024-11-22 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
...
2026-01-27 09:46:38,892 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-27 09:46:38,892 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-27 09:46:38,940 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-27 09:46:39,145 INFO [logger.py]: Writing 82844 items as 83 minibatches to DB.
2026-01-27 09:46:39,145 INFO [logger.py]: Writing 83 minibatches to DB as 4 batches...
2026-01-27 09:46:39,145 INFO [logger.py]: Processing batch 1/4...
2026-01-27 09:46:39,229 INFO [logger.py]: Inserted 82844 posts into queue for integration: ml_inference_intergroup
2026-01-27 09:46:39,234 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-27 09:46:39,234 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

I decided to do it in one shot.

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 120000
2026-01-27 09:56:13,675 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-27 09:56:13,781 INFO [logger.py]: Not clearing any queues.
2026-01-27 09:56:13,781 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-27 09:56:13,781 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-27 09:56:20,565 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-27 09:56:20,566 INFO [logger.py]: Current queue size: 83 items
2026-01-27 09:56:20,849 INFO [logger.py]: Loaded 82844 posts to classify.
2026-01-27 09:56:20,850 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-27 09:56:20,850 INFO [logger.py]: Latest inference timestamp: None
2026-01-27 09:56:20,895 INFO [logger.py]: After dropping duplicates, 82844 posts remain.
2026-01-27 09:56:20,958 INFO [logger.py]: After filtering, 81419 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 182.703125 MB
2026-01-27 09:56:22,257 INFO [logger.py]: Classifying 81419 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/163 [00:00<?, ?batch/s]2026-01-27 09:56:23,330 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/163...

...
2026-01-27 13:02:07,079 INFO [logger.py]: Current queue size: 162 items
2026-01-27 13:02:07,080 INFO [logger.py]: Adding 419 posts to the output queue.
2026-01-27 13:02:07,082 INFO [logger.py]: Writing 419 items as 1 minibatches to DB.
2026-01-27 13:02:07,082 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-27 13:02:07,082 INFO [logger.py]: Processing batch 1/1...
2026-01-27 13:02:07,084 INFO [logger.py]: Deleting 1 batch IDs from the input queue.
2026-01-27 13:02:07,085 INFO [logger.py]: Deleted 0 items from queue.
Classifying batches: 100%|█████████████████████████████████████████| 163/163 [3:05:43<00:00, 68.37s/batch, successful=81419, failed=0]
Execution time for run_batch_classification: 185 minutes, 45 seconds
Memory usage for run_batch_classification: -66.84375 MB
Execution time for orchestrate_classification: 185 minutes, 50 seconds
Memory usage for orchestrate_classification: 38.78125 MB
Execution time for classify_latest_posts: 185 minutes, 52 seconds
Memory usage for classify_latest_posts: 38.765625 MB
2026-01-27 13:02:10,321 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-27 13:02:10,323 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-27 19:53:15,538 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-27 19:53:15,675 INFO [logger.py]: Not clearing any queues.
2026-01-27 19:53:15,675 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-27 19:53:15,676 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-27 19:53:15,693 INFO [logger.py]: Current queue size: 163 items
2026-01-27 19:53:16,120 INFO [logger.py]: Exporting 81419 records to local storage for integration ml_inference_intergroup...
2026-01-27 19:53:16,252 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-14] Exporting n=549 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,356 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,364 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-15] Exporting n=9532 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,378 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,390 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-16] Exporting n=15166 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,409 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,416 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-17] Exporting n=8413 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,429 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,432 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-18] Exporting n=2258 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,437 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,450 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-19] Exporting n=16512 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,471 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,474 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-20] Exporting n=884 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,477 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,487 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-21] Exporting n=13298 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,505 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,516 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-22] Exporting n=14807 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-27 19:53:16,539 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-27 19:53:16,542 INFO [logger.py]: Finished exporting 81419 records to local storage for integration ml_inference_intergroup...
2026-01-27 19:53:16,545 INFO [logger.py]: Successfully wrote 81419 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-27 19:53:51,432 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-27 19:53:51,501 INFO [logger.py]: Not clearing any queues.
2026-01-27 19:53:51,529 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-27 19:53:51,533 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-27 19:53:51,533 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-27 19:53:51,539 INFO [logger.py]: Registering 79 files for migration                                                           
2026-01-27 19:53:51,543 INFO [logger.py]: Registered 79 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (79 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 203.52it/s]
Finished initializing migration tracker db
2026-01-27 19:53:51,549 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-27 19:53:51,608 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/a3a3a53bc5c84ba7a64b33642a461c31-0.parquet
2026-01-27 19:53:51,608 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/a3a3a53bc5c84ba7a64b33642a461c31-0.parquet to S3 (1.64 MB)
2026-01-27 19:53:52,274 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/a3a3a53bc5c84ba7a64b33642a461c31-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-16/a3a3a53bc5c84ba7a64b33642a461c31-0.parquet
2026-01-27 19:53:52,276 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-16/a3a3a53bc5c84ba7a64b33642a461c31-0.parquet
                                                                                                                                     2026-01-27 19:53:52,278 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/93f14ff8042749c8858b91112fcfe51b-0.parquet
2026-01-27 19:53:52,278 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/93f14ff8042749c8858b91112fcfe51b-0.parquet to S3 (0.26 MB)
2026-01-27 19:53:52,558 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/93f14ff8042749c8858b91112fcfe51b-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-18/93f14ff8042749c8858b91112fcfe51b-0.parquet
2026-01-27 19:53:52,562 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-18/93f14ff8042749c8858b91112fcfe51b-0.parquet
                                                                                                                                     2026-01-27 19:53:52,565 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/1e7b07048de34a99a30bca609a7fc7e2-0.parquet
2026-01-27 19:53:52,565 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/1e7b07048de34a99a30bca609a7fc7e2-0.parquet to S3 (0.11 MB)
2026-01-27 19:53:52,876 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/1e7b07048de34a99a30bca609a7fc7e2-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-20/1e7b07048de34a99a30bca609a7fc7e2-0.parquet
2026-01-27 19:53:52,878 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-20/1e7b07048de34a99a30bca609a7fc7e2-0.parquet
                                                                                                                                     2026-01-27 19:53:52,880 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d28fcff647ef4413b3b7680ed53a1652-0.parquet
2026-01-27 19:53:52,880 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d28fcff647ef4413b3b7680ed53a1652-0.parquet to S3 (1.51 MB)
2026-01-27 19:53:53,762 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d28fcff647ef4413b3b7680ed53a1652-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d28fcff647ef4413b3b7680ed53a1652-0.parquet
2026-01-27 19:53:53,764 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/d28fcff647ef4413b3b7680ed53a1652-0.parquet
                                                                                                                                     2026-01-27 19:53:53,767 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/70c50975d6f440b9b46c967659abdc47-0.parquet
2026-01-27 19:53:53,767 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/70c50975d6f440b9b46c967659abdc47-0.parquet to S3 (1.84 MB)
2026-01-27 19:53:54,511 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/70c50975d6f440b9b46c967659abdc47-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-19/70c50975d6f440b9b46c967659abdc47-0.parquet
2026-01-27 19:53:54,515 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-19/70c50975d6f440b9b46c967659abdc47-0.parquet
                                                                                                                                     2026-01-27 19:53:54,519 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/9442464232574e1fbef2da956594fe29-0.parquet
2026-01-27 19:53:54,520 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/9442464232574e1fbef2da956594fe29-0.parquet to S3 (0.89 MB)
2026-01-27 19:53:54,852 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/9442464232574e1fbef2da956594fe29-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-17/9442464232574e1fbef2da956594fe29-0.parquet
2026-01-27 19:53:54,856 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-17/9442464232574e1fbef2da956594fe29-0.parquet
                                                                                                                                     2026-01-27 19:53:54,858 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a2302c08a08d4a588a2b2f766182b03a-0.parquet
2026-01-27 19:53:54,858 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a2302c08a08d4a588a2b2f766182b03a-0.parquet to S3 (1.07 MB)
2026-01-27 19:53:55,203 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a2302c08a08d4a588a2b2f766182b03a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a2302c08a08d4a588a2b2f766182b03a-0.parquet
2026-01-27 19:53:55,208 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-15/a2302c08a08d4a588a2b2f766182b03a-0.parquet
                                                                                                                                     2026-01-27 19:53:55,213 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/ef4ac99425c54570b50965c5a2c4e9b4-0.parquet
2026-01-27 19:53:55,214 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/ef4ac99425c54570b50965c5a2c4e9b4-0.parquet to S3 (1.66 MB)
2026-01-27 19:53:55,836 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/ef4ac99425c54570b50965c5a2c4e9b4-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-22/ef4ac99425c54570b50965c5a2c4e9b4-0.parquet
2026-01-27 19:53:55,839 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/ef4ac99425c54570b50965c5a2c4e9b4-0.parquet
                                                                                                                                     2026-01-27 19:53:55,841 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/033e58936bf64cf6b6f71b89ca3d6ba3-0.parquet
2026-01-27 19:53:55,841 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/033e58936bf64cf6b6f71b89ca3d6ba3-0.parquet to S3 (0.07 MB)
2026-01-27 19:53:55,965 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/033e58936bf64cf6b6f71b89ca3d6ba3-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-14/033e58936bf64cf6b6f71b89ca3d6ba3-0.parquet
2026-01-27 19:53:55,968 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-14/033e58936bf64cf6b6f71b89ca3d6ba3-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 9/9 [00:04<00:00,  2.06it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:04<00:00,  2.18s/it]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-11-16	archive_ml_inference_intergroup:partition_date=2024-11-17	archive_ml_inference_intergroup:partition_date=2024-11-18	archive_ml_inference_intergroup:partition_date=2024-11-19	archive_ml_inference_intergroup:partition_date=2024-11-20	archive_ml_inference_intergroup:partition_date=2024-11-21	archive_ml_inference_intergroup:partition_date=2024-11-22
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-16
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-17
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-18
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-19
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-20
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-21
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-22
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.

```bash
#	label	_col1
1	0	753149
2	-1	82
3	1	28756
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
14	2024-10-14	72931
15	2024-10-15	46618
16	2024-10-16	9467
17	2024-10-17	880
18	2024-10-18	2463
19	2024-10-19	8357
20	2024-10-20	14180
21	2024-10-21	14265
22	2024-10-22	14366
23	2024-10-23	17044
24	2024-10-24	14381
25	2024-10-25	15478
26	2024-10-26	12553
27	2024-10-27	12020
28	2024-10-28	9431
29	2024-10-29	13752
30	2024-10-30	12795
31	2024-10-31	14617
32	2024-11-01	13996
33	2024-11-02	10527
34	2024-11-03	12539
35	2024-11-04	12404
36	2024-11-05	13033
37	2024-11-06	16866
38	2024-11-07	11799
39	2024-11-08	17490
40	2024-11-09	14371
41	2024-11-10	14128
42	2024-11-11	16128
43	2024-11-12	15433
44	2024-11-13	17374
45	2024-11-14	15519
46	2024-11-15	21031
47	2024-11-16	15166
48	2024-11-17	8413
49	2024-11-18	2258
50	2024-11-19	16512
51	2024-11-20	884
52	2024-11-21	13298
53	2024-11-22	14807
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
163
sqlite> .exit
```

- [X] Delete input and output queues

## Week 9: 2024-11-23 to 2024-11-29

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-23 --end-date 2024-11-29 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
...
2026-01-27 20:12:02,102 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-27 20:12:02,103 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-27 20:12:02,146 INFO [logger.py]: DB for queue input_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-27 20:12:02,147 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-27 20:12:02,148 INFO [logger.py]: Current queue size: 0 items
2026-01-27 20:12:02,324 INFO [logger.py]: Writing 84254 items as 85 minibatches to DB.
2026-01-27 20:12:02,325 INFO [logger.py]: Writing 85 minibatches to DB as 4 batches...
2026-01-27 20:12:02,325 INFO [logger.py]: Processing batch 1/4...
2026-01-27 20:12:02,438 INFO [logger.py]: Inserted 84254 posts into queue for integration: ml_inference_intergroup
2026-01-27 20:12:02,443 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-27 20:12:02,443 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [X] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

I'll just do it in 1 iteration:

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 100000
2026-01-27 20:13:25,272 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-27 20:13:25,371 INFO [logger.py]: Not clearing any queues.
2026-01-27 20:13:25,371 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-27 20:13:25,371 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-27 20:13:32,757 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-27 20:13:32,761 INFO [logger.py]: Current queue size: 85 items
2026-01-27 20:13:33,030 INFO [logger.py]: Loaded 84254 posts to classify.
2026-01-27 20:13:33,031 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-27 20:13:33,032 INFO [logger.py]: Latest inference timestamp: None
2026-01-27 20:13:33,074 INFO [logger.py]: After dropping duplicates, 84254 posts remain.
2026-01-27 20:13:33,138 INFO [logger.py]: After filtering, 82890 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 2 seconds
Memory usage for get_posts_to_classify: 187.140625 MB
2026-01-27 20:13:34,551 INFO [logger.py]: Classifying 82890 posts with intergroup classifier...
Classifying batches:   0%|                                                                                 | 0/166 [00:00<?, ?batch/s]2026-01-27 20:13:35,631 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/166...
2026-01-27 20:13:36,934 INFO [_base_client.py]: Retrying request to /chat/completions in 0.396232 seconds
Retrying request to /chat/completions in 0.396232 seconds

...

2026-01-27 22:37:35,542 INFO [logger.py]: Adding 390 posts to the output queue.
2026-01-27 22:37:35,543 INFO [logger.py]: Writing 390 items as 1 minibatches to DB.
2026-01-27 22:37:35,543 INFO [logger.py]: Writing 1 minibatches to DB as 1 batches...
2026-01-27 22:37:35,543 INFO [logger.py]: Processing batch 1/1...
2026-01-27 22:37:35,546 INFO [logger.py]: Deleting 2 batch IDs from the input queue.
2026-01-27 22:37:35,547 INFO [logger.py]: Deleted 1 items from queue.
Classifying batches: 100%|█████████████████████████████████████████| 166/166 [2:23:59<00:00, 52.05s/batch, successful=82890, failed=0]
Execution time for run_batch_classification: 144 minutes, 1 seconds
Memory usage for run_batch_classification: -81.984375 MB
Execution time for orchestrate_classification: 144 minutes, 6 seconds
Memory usage for orchestrate_classification: 39.734375 MB
Execution time for classify_latest_posts: 144 minutes, 8 seconds
Memory usage for classify_latest_posts: 39.734375 MB
2026-01-27 22:37:38,779 INFO [logger.py]: Integration 1 of 1: ml_inference_intergroup completed successfully
2026-01-27 22:37:38,781 INFO [logger.py]: Integrations completed successfully: ml_inference_intergroup
```

- [X] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup
2026-01-28 07:12:19,865 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 07:12:19,971 INFO [logger.py]: Not clearing any queues.
2026-01-28 07:12:19,972 INFO [logger.py]: DB for queue output_ml_inference_intergroup already exists. Not overwriting, using existing DB...
2026-01-28 07:12:19,972 INFO [logger.py]: Loading existing SQLite DB for queue output_ml_inference_intergroup...
2026-01-28 07:12:19,988 INFO [logger.py]: Current queue size: 166 items
2026-01-28 07:12:20,449 INFO [logger.py]: Exporting 82890 records to local storage for integration ml_inference_intergroup...
2026-01-28 07:12:20,593 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-21] Exporting n=361 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,696 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,702 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-22] Exporting n=5822 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,712 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,717 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-23] Exporting n=3673 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,724 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,725 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-24] Exporting n=17 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,727 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,734 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-25] Exporting n=7249 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,745 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,762 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-26] Exporting n=22588 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,790 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,801 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-27] Exporting n=13642 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,820 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,834 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-28] Exporting n=16676 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,854 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,866 INFO [logger.py]: [Service = ml_inference_intergroup, Partition Date = 2024-11-29] Exporting n=12862 records to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache...
2026-01-28 07:12:20,885 INFO [logger.py]: [Service = ml_inference_intergroup] Successfully exported ml_inference_intergroup data to /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache as Parquet
2026-01-28 07:12:20,888 INFO [logger.py]: Finished exporting 82890 records to local storage for integration ml_inference_intergroup...
2026-01-28 07:12:20,890 INFO [logger.py]: Successfully wrote 82890 records to storage for integration ml_inference_intergroup
```

- [X] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3
2026-01-28 07:12:49,873 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 07:12:49,952 INFO [logger.py]: Not clearing any queues.
2026-01-28 07:12:49,982 INFO [logger.py]: Initialized migration tracker database: /Users/mark/Documents/work/bluesky-research/pipelines/backfill_records_coordination/.migration_tracker/migration_tracker_backfill.db
Processing prefixes:   0%|                                                                                      | 0/2 [00:00<?, ?it/s]Initializing migration tracker db for ml_inference_intergroup/active
                                                                       2026-01-28 07:12:49,986 INFO [logger.py]: Registering 0 files for migration                                                           
2026-01-28 07:12:49,986 INFO [logger.py]: Registered 0 files for migration
Initialized migration tracker db for ml_inference_intergroup/active (0 files)
Initializing migration tracker db for ml_inference_intergroup/cache
                                                                                                                                     2026-01-28 07:12:49,994 INFO [logger.py]: Registering 88 files for migration                                                           
2026-01-28 07:12:49,998 INFO [logger.py]: Registered 88 files for migration
Initialized migration tracker db for ml_inference_intergroup/cache (88 files)
Processing prefixes: 100%|█████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00, 159.15it/s]
Finished initializing migration tracker db
2026-01-28 07:12:50,006 INFO [credentials.py]: Found credentials in shared credentials file: ~/.aws/credentials
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                    | 0/2 [00:00<?, ?it/s]
Migrating ml_inference_intergroup/active: 0it [00:00, ?it/s]                                                                         2026-01-28 07:12:50,070 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-29/1574c75cefc747bc9d363272ff3b5114-0.parquet
2026-01-28 07:12:50,070 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-29/1574c75cefc747bc9d363272ff3b5114-0.parquet to S3 (1.42 MB)
2026-01-28 07:12:50,655 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-29/1574c75cefc747bc9d363272ff3b5114-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-29/1574c75cefc747bc9d363272ff3b5114-0.parquet
2026-01-28 07:12:50,658 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-29/1574c75cefc747bc9d363272ff3b5114-0.parquet
                                                                                                                                     2026-01-28 07:12:50,659 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-27/85afc5f59ee4472bb46fb00edab7ebba-0.parquet
2026-01-28 07:12:50,659 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-27/85afc5f59ee4472bb46fb00edab7ebba-0.parquet to S3 (1.54 MB)
2026-01-28 07:12:51,116 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-27/85afc5f59ee4472bb46fb00edab7ebba-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-27/85afc5f59ee4472bb46fb00edab7ebba-0.parquet
2026-01-28 07:12:51,118 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-27/85afc5f59ee4472bb46fb00edab7ebba-0.parquet
                                                                                                                                     2026-01-28 07:12:51,120 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/f285cfc0f3744805b4cf0ea5e1913946-0.parquet
2026-01-28 07:12:51,120 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/f285cfc0f3744805b4cf0ea5e1913946-0.parquet to S3 (0.05 MB)
2026-01-28 07:12:51,177 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/f285cfc0f3744805b4cf0ea5e1913946-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-21/f285cfc0f3744805b4cf0ea5e1913946-0.parquet
2026-01-28 07:12:51,178 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-21/f285cfc0f3744805b4cf0ea5e1913946-0.parquet
2026-01-28 07:12:51,180 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-26/8bf47cccd91a430592f617c117afb7fa-0.parquet
2026-01-28 07:12:51,180 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-26/8bf47cccd91a430592f617c117afb7fa-0.parquet to S3 (2.56 MB)
2026-01-28 07:12:51,735 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-26/8bf47cccd91a430592f617c117afb7fa-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-26/8bf47cccd91a430592f617c117afb7fa-0.parquet
2026-01-28 07:12:51,739 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-26/8bf47cccd91a430592f617c117afb7fa-0.parquet
                                                                                                                                     2026-01-28 07:12:51,741 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-28/eceadf78480447c39f7cf7a768c3880b-0.parquet
2026-01-28 07:12:51,741 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-28/eceadf78480447c39f7cf7a768c3880b-0.parquet to S3 (1.85 MB)
2026-01-28 07:12:52,160 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-28/eceadf78480447c39f7cf7a768c3880b-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-28/eceadf78480447c39f7cf7a768c3880b-0.parquet
2026-01-28 07:12:52,162 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-28/eceadf78480447c39f7cf7a768c3880b-0.parquet
                                                                                                                                     2026-01-28 07:12:52,163 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-23/841fc48028e947879b25ced1a0bc9b9a-0.parquet
2026-01-28 07:12:52,164 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-23/841fc48028e947879b25ced1a0bc9b9a-0.parquet to S3 (0.42 MB)
2026-01-28 07:12:52,285 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-23/841fc48028e947879b25ced1a0bc9b9a-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-23/841fc48028e947879b25ced1a0bc9b9a-0.parquet
2026-01-28 07:12:52,287 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-23/841fc48028e947879b25ced1a0bc9b9a-0.parquet
                                                                                                                                     2026-01-28 07:12:52,288 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-24/c227215c9fea4484bdd2f02a2460070d-0.parquet
2026-01-28 07:12:52,288 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-24/c227215c9fea4484bdd2f02a2460070d-0.parquet to S3 (0.01 MB)
2026-01-28 07:12:52,342 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-24/c227215c9fea4484bdd2f02a2460070d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-24/c227215c9fea4484bdd2f02a2460070d-0.parquet
2026-01-28 07:12:52,344 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-24/c227215c9fea4484bdd2f02a2460070d-0.parquet
2026-01-28 07:12:52,346 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-25/3105758523ba41fc82b9145d5b0523d8-0.parquet
2026-01-28 07:12:52,346 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-25/3105758523ba41fc82b9145d5b0523d8-0.parquet to S3 (0.85 MB)
2026-01-28 07:12:52,575 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-25/3105758523ba41fc82b9145d5b0523d8-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-25/3105758523ba41fc82b9145d5b0523d8-0.parquet
2026-01-28 07:12:52,576 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-25/3105758523ba41fc82b9145d5b0523d8-0.parquet
                                                                                                                                     2026-01-28 07:12:52,577 INFO [logger.py]: Marked file as started: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/1d324e47e8794d9e8cc401da7db2b37d-0.parquet
2026-01-28 07:12:52,577 INFO [logger.py]: Migrating file /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/1d324e47e8794d9e8cc401da7db2b37d-0.parquet to S3 (0.68 MB)
2026-01-28 07:12:52,752 INFO [logger.py]: Successfully uploaded /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/1d324e47e8794d9e8cc401da7db2b37d-0.parquet to s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/ml_inference_intergroup/cache/partition_date=2024-11-22/1d324e47e8794d9e8cc401da7db2b37d-0.parquet
2026-01-28 07:12:52,754 INFO [logger.py]: Marked file as completed: /Users/mark/Documents/work/bluesky_research_data/ml_inference_intergroup/cache/partition_date=2024-11-22/1d324e47e8794d9e8cc401da7db2b37d-0.parquet
Migrating ml_inference_intergroup/cache: 100%|██████████████████████████████████████████████████████████| 9/9 [00:02<00:00,  3.35it/s]
Processing prefixes: 100%|██████████████████████████████████████████████████████████████████████████████| 2/2 [00:02<00:00,  1.34s/it]
```

- [X] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash
Partitions not in metastore:	archive_ml_inference_intergroup:partition_date=2024-11-23	archive_ml_inference_intergroup:partition_date=2024-11-24	archive_ml_inference_intergroup:partition_date=2024-11-25	archive_ml_inference_intergroup:partition_date=2024-11-26	archive_ml_inference_intergroup:partition_date=2024-11-27	archive_ml_inference_intergroup:partition_date=2024-11-28	archive_ml_inference_intergroup:partition_date=2024-11-29
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-23
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-24
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-25
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-26
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-27
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-28
Repair: Added partition to metastore archive_ml_inference_intergroup:partition_date=2024-11-29
```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.

```bash
#	label	_col1
1	-1	88
2	1	28840
3	0	835580
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
14	2024-10-14	72931
15	2024-10-15	46618
16	2024-10-16	9467
17	2024-10-17	880
18	2024-10-18	2463
19	2024-10-19	8357
20	2024-10-20	14180
21	2024-10-21	14265
22	2024-10-22	14366
23	2024-10-23	17044
24	2024-10-24	14381
25	2024-10-25	15478
26	2024-10-26	12553
27	2024-10-27	12020
28	2024-10-28	9431
29	2024-10-29	13752
30	2024-10-30	12795
31	2024-10-31	14617
32	2024-11-01	13996
33	2024-11-02	10527
34	2024-11-03	12539
35	2024-11-04	12404
36	2024-11-05	13033
37	2024-11-06	16866
38	2024-11-07	11799
39	2024-11-08	17490
40	2024-11-09	14371
41	2024-11-10	14128
42	2024-11-11	16128
43	2024-11-12	15433
44	2024-11-13	17374
45	2024-11-14	15519
46	2024-11-15	21031
47	2024-11-16	15166
48	2024-11-17	8413
49	2024-11-18	2258
50	2024-11-19	16512
51	2024-11-20	884
52	2024-11-21	13584
53	2024-11-22	20335
54	2024-11-23	3673
55	2024-11-24	17
56	2024-11-25	7249
57	2024-11-26	22588
58	2024-11-27	13642
59	2024-11-28	16676
60	2024-11-29	12862
```

- [X] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> SELECT COUNT(*) FROM queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> SELECT COUNT(*) FROM queue;
166
sqlite> .exit
```

- [X] Delete input and output queues

## Week 10: 2024-11-30 to 2024-12-01

- [X] Enqueueing:
  - `python -m pipelines.backfill_records_coordination.app --add-to-queue --record-type posts_used_in_feeds --integrations g --start-date 2024-11-30 --end-date 2024-12-01 --source-data-location s3 --sample-records --sample-proportion 0.2`

```bash
...
2026-01-28 07:28:52,611 INFO [logger.py]: Loaded 182252 base posts (scope=posts_used_in_feeds).
2026-01-28 07:28:52,626 INFO [logger.py]: Sampled base posts (proportion=0.2): 182252 -> 36450
2026-01-28 07:28:52,626 INFO [logger.py]: [Progress: 1/1] Enqueuing records for integration: ml_inference_intergroup
2026-01-28 07:28:52,626 INFO [logger.py]: Listing S3 parquet URIs for dataset=ml_inference_intergroup, storage_tiers=['cache'], n_days=2.
2026-01-28 07:28:52,839 INFO [logger.py]: Listed total_parquet_files=0 for dataset=ml_inference_intergroup.
2026-01-28 07:28:52,840 WARNING [logger.py]: 
                    filepaths must be provided when mode='parquet.
                    There are scenarios where data is missing (e.g., in the "active"
                    path, there might not be any up-to-date records). In these cases,
                    it's assumed that the filepaths are not provided.
                    
2026-01-28 07:28:52,845 INFO [logger.py]: Query metrics: {'duckdb': {'query': {'sql': 'SELECT uri FROM ml_inference_intergroup', 'result_shape': {'rows': 0, 'columns': 1}, 'result_memory_usage_mb': np.float64(0.000118255615234375)}, 'database': {'total_size_mb': None, 'table_count': 0}}}
2026-01-28 07:28:52,845 INFO [logger.py]: Loaded 0 post URIs from S3 for service ml_inference_intergroup
2026-01-28 07:28:52,872 INFO [logger.py]: Creating new SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 07:28:52,981 INFO [logger.py]: Writing 36450 items as 37 minibatches to DB.
2026-01-28 07:28:52,981 INFO [logger.py]: Writing 37 minibatches to DB as 2 batches...
2026-01-28 07:28:52,981 INFO [logger.py]: Processing batch 1/2...
2026-01-28 07:28:53,016 INFO [logger.py]: Inserted 36450 posts into queue for integration: ml_inference_intergroup
2026-01-28 07:28:53,021 INFO [logger.py]: [Progress: 1/1] Completed enqueuing records for integration: ml_inference_intergroup
2026-01-28 07:28:53,022 INFO [logger.py]: [Progress: 1/1] Enqueuing records completed successfully.
```

- [ ] Running integrations:
  - `python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 10000`

I'll run in one shot.

```bash
(base) ➜  bluesky-research git:(start-intergroup-backfills) ✗ uv run python -m pipelines.backfill_records_coordination.app --run-integrations --integrations g --max-records-per-run 40000
2026-01-28 07:30:49,476 INFO [logger.py]: No connection provided, creating a new in-memory connection.
2026-01-28 07:30:49,581 INFO [logger.py]: Not clearing any queues.
2026-01-28 07:30:49,581 INFO [logger.py]: Running integrations: ml_inference_intergroup
2026-01-28 07:30:49,581 INFO [logger.py]: Running integration 1 of 1: ml_inference_intergroup
2026-01-28 07:30:57,246 INFO [logger.py]: Loading existing SQLite DB for queue input_ml_inference_intergroup...
2026-01-28 07:30:57,247 INFO [logger.py]: Current queue size: 37 items
2026-01-28 07:30:57,367 INFO [logger.py]: Loaded 36450 posts to classify.
2026-01-28 07:30:57,368 INFO [logger.py]: Getting posts to classify for inference type intergroup.
2026-01-28 07:30:57,368 INFO [logger.py]: Latest inference timestamp: None
2026-01-28 07:30:57,385 INFO [logger.py]: After dropping duplicates, 36450 posts remain.
2026-01-28 07:30:57,413 INFO [logger.py]: After filtering, 35840 posts remain.
Execution time for get_posts_to_classify: 0 minutes, 1 seconds
Memory usage for get_posts_to_classify: 81.078125 MB
2026-01-28 07:30:58,633 INFO [logger.py]: Classifying 35840 posts with intergroup classifier...
Classifying batches:   0%|                                                                                  | 0/72 [00:00<?, ?batch/s]2026-01-28 07:30:59,704 INFO [logger.py]: [Completion percentage: 0%] Processing batch 0/72...

...

```
- [ ] Writing cache to storage:
  - `python -m pipelines.backfill_records_coordination.app --write-cache-buffer-to-storage --service-source-buffer ml_inference_intergroup`

- [ ] Migrate to S3:
  - `python -m pipelines.backfill_records_coordination.app --integrations g --migrate-to-s3`

- [] Verify in Athena

```sql
MSCK REPAIR TABLE archive_ml_inference_intergroup;
```

```bash

```

```sql
SELECT label, COUNT(*)
FROM (
  SELECT DISTINCT label, uri
  FROM archive_ml_inference_intergroup
) t
GROUP BY label;
```

some -1s to consider here later for postprocessing.

```bash

```

```sql
SELECT partition_date, COUNT(*) as total_labels
FROM archive_ml_inference_intergroup
GROUP BY 1
ORDER BY 1 ASC
```

```bash

```

- [ ] Inspect input queue

```bash
(base) ➜  queue sqlite3 input_ml_inference_intergroup.db 
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
0
sqlite> .exit
(base) ➜  queue sqlite3 output_ml_inference_intergroup.db                                                    
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> select count(*) from queue;
186
sqlite> .exit
```

- [ ] Delete input and output queues