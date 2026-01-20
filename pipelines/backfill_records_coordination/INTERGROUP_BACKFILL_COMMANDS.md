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


