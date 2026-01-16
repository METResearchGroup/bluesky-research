# How do we do backfills end-to-end?

## Problem statement

## How to run the CLI tool

In `pipelines/backfill_records_coordination/app.py`, the CLI tool...

It supports 3 modes:

1. Adding posts to queue, to await backfill/
2. Run the integration(s).
3. Write the records from cache to permanent storage.

### Mode 1 (`--add-to-queue`): Add posts to queue, to be pending backfill

We can pick our source for posts that we want to enqueue for backfill.

There are two sources we support here:

1. `--record-type posts`: we enqueue all of our available posts as pending backfill (subtracting any posts that have been backfilled already for that integration).
2. `--record-type posts_used_in_feeds`: we enqueue all of the posts that were used in feeds (subtracting any posts that have been backfilled already for that integration).

For example, for the following command:

```bash
python app.py --record-type posts --integrations ml_inference_perspective_api --add-to-queue
```

We add all posts to the input queue for the `ml_inference_perspective_api` integration, subtracting any posts that we've previously classified.

### Mode 2: Running integrations

For example, in the following command:

```bash
python app.py --record-type posts --integrations ml_inference_perspective_api --run-integrations
```

We run the integration on whatever records are currently queued.

You can also omit `--record-type` when you are only running integrations (i.e., not enqueueing).
In that case, you must specify at least one `--integrations`:

```bash
python app.py --integrations ml_inference_perspective_api --run-integrations
```


### Mode 3: Writing cached records to permanent storage

## FAQs

1. How do we manage large datasets?
We can run in batches. We can do Steps 1, 2, and 3, for example, on a given date (or date range), and then repeat on the next date range.
