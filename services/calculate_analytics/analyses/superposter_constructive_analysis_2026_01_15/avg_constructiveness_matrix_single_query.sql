-- Single-query (Athena/Presto) superposter-status × condition constructiveness matrix
--
-- Output:
--   2 rows (superposter_status) × columns per condition:
--     - avg_prob_constructive_<condition>
--     - n_labeled_posts_<condition>
--
-- Condition strings (from `lib/constants.py`):
--   - representative_diversification
--   - engagement
--   - reverse_chronological
--
-- Definitions:
-- - "Post used in feeds": URI appears in `archive_generated_feeds.feed` JSON for any generated feed.
-- - "Labeled": `archive_ml_inference_perspective_api.was_successfully_labeled = TRUE` and
--   we take the latest label per URI (by parsed label_timestamp).
-- - "Is superposter": author is a superposter on ANY day in [post_date, post_date + 4 days],
--   using `superposter_author_dids` (partitioned by day).
--
-- Notes:
-- - Constructiveness metric used for averaging is `prob_constructive` (as requested).
-- - We also compute `prob_constructive_fixed = COALESCE(prob_constructive, prob_reasoning)`
--   for debugging/robustness, but the matrix averages use `prob_constructive` only.

WITH
/* -------------------------------------------------------------------------- */
/* 1) Parse feeds and explode JSON into one row per (feed_id, uri)             */
/* -------------------------------------------------------------------------- */
feeds AS (
  SELECT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    -- Defensive parsing: some rows are clean JSON arrays, others may be escaped or appended.
    coalesce(
      try(CAST(json_parse(feed) AS array(json))),
      try(CAST(json_parse(regexp_extract(feed, '^(\\[.*\\])\\s\"', 1)) AS array(json))),
      try(CAST(json_parse(regexp_replace(feed, '\\\\\"', '\"')) AS array(json))),
      try(
        CAST(
          json_parse(
            regexp_replace(regexp_extract(feed, '^(\\[.*\\])\\s\"', 1), '\\\\\"', '\"')
          ) AS array(json)
        )
      )
    ) AS feed_items
  FROM archive_generated_feeds
  WHERE condition IN (
    'representative_diversification',
    'engagement',
    'reverse_chronological'
  )
),
feed_posts_raw AS (
  SELECT
    f.feed_id,
    f.user,
    f.bluesky_handle,
    f.condition,
    f.feed_generation_timestamp,
    json_extract_scalar(item_obj, '$.item') AS uri
  FROM feeds f
  CROSS JOIN UNNEST(f.feed_items) AS t(item_obj)
  WHERE json_extract_scalar(item_obj, '$.item') IS NOT NULL
),
feed_posts AS (
  -- De-dupe within a feed (same URI can appear multiple times in the array).
  SELECT DISTINCT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    uri
  FROM feed_posts_raw
),

/* -------------------------------------------------------------------------- */
/* 2) Attach post metadata (author_did + post_date)                            */
/* -------------------------------------------------------------------------- */
post_meta AS (
  -- If `archive_preprocessed_posts` has multiple rows per URI, pick deterministic values.
  SELECT
    uri,
    max_by(author_did, partition_date) AS author_did,
    CAST(max(partition_date) AS date) AS post_date
  FROM archive_preprocessed_posts
  GROUP BY 1
),

/* -------------------------------------------------------------------------- */
/* 3) Deduplicate labels per URI (latest label), keep prob_constructive        */
/* -------------------------------------------------------------------------- */
labels_ranked AS (
  SELECT
    l.uri,
    l.prob_constructive,
    l.prob_reasoning,
    COALESCE(l.prob_constructive, l.prob_reasoning) AS prob_constructive_fixed,
    try(date_parse(l.label_timestamp, '%Y-%m-%d-%H:%i:%s')) AS label_ts,
    row_number() OVER (
      PARTITION BY l.uri
      ORDER BY try(date_parse(l.label_timestamp, '%Y-%m-%d-%H:%i:%s')) DESC NULLS LAST
    ) AS rn
  FROM archive_ml_inference_perspective_api l
  WHERE l.was_successfully_labeled = TRUE
    AND (l.prob_constructive IS NOT NULL OR l.prob_reasoning IS NOT NULL)
),
labels_dedup AS (
  SELECT
    uri,
    prob_constructive,
    prob_constructive_fixed
  FROM labels_ranked
  WHERE rn = 1
),

/* -------------------------------------------------------------------------- */
/* 4) Build per-post fact rows with superposter-in-window flag + label         */
/* -------------------------------------------------------------------------- */
fact AS (
  SELECT
    fp.condition,
    pm.author_did,
    pm.post_date,
    EXISTS (
      SELECT 1
      FROM superposter_author_dids s
      WHERE s.author_did = pm.author_did
        AND CAST(s.partition_date AS date)
            BETWEEN pm.post_date AND pm.post_date + INTERVAL '4' DAY
    ) AS is_superposter_in_0_to_4d_window,
    ld.prob_constructive,
    ld.prob_constructive_fixed
  FROM feed_posts fp
  JOIN post_meta pm
    ON fp.uri = pm.uri
  LEFT JOIN labels_dedup ld
    ON fp.uri = ld.uri
),

/* -------------------------------------------------------------------------- */
/* 5) Aggregate into the 2×3 matrix via conditional aggregation                */
/* -------------------------------------------------------------------------- */
grouped AS (
  SELECT
    CASE
      WHEN is_superposter_in_0_to_4d_window THEN 'is_superposter'
      ELSE 'is_not_superposter'
    END AS superposter_status,

    -- Averages: only over labeled rows (avg ignores NULL).
    avg(CASE WHEN condition = 'representative_diversification' THEN prob_constructive END)
      AS avg_prob_constructive_representative_diversification,
    avg(CASE WHEN condition = 'engagement' THEN prob_constructive END)
      AS avg_prob_constructive_engagement,
    avg(CASE WHEN condition = 'reverse_chronological' THEN prob_constructive END)
      AS avg_prob_constructive_reverse_chronological,

    -- Label coverage (how many labeled post rows contribute per cell).
    count(CASE WHEN condition = 'representative_diversification' AND prob_constructive IS NOT NULL THEN 1 END)
      AS n_labeled_posts_representative_diversification,
    count(CASE WHEN condition = 'engagement' AND prob_constructive IS NOT NULL THEN 1 END)
      AS n_labeled_posts_engagement,
    count(CASE WHEN condition = 'reverse_chronological' AND prob_constructive IS NOT NULL THEN 1 END)
      AS n_labeled_posts_reverse_chronological

  FROM fact
  GROUP BY 1
)
SELECT *
FROM grouped
ORDER BY superposter_status;
