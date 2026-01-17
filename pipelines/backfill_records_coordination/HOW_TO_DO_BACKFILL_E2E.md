# How do we do backfills end-to-end?

## Problem statement

## Step 1: Add new data to the corresponding input queue

## Step 2: Run the service on the data from the input queue

## Step 3: Take the data from the output queue and write to permanent storage

## FAQs

1. How do we manage large datasets?
We can run in batches. We can do Steps 1, 2, and 3, for example, on a given date (or date range), and then repeat on the next date range.
