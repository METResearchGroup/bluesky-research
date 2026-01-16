-- Superposter × condition constructiveness matrix (Athena/Presto SQL)
--
-- Goal:
--   Build a dataset of posts used in generated feeds, label them with constructiveness,
--   tag each post author as "is_superposter" if they are a superposter on ANY day in
--   [post_date, post_date + 4 days], and then aggregate average constructiveness by:
--     (condition ∈ {RC, EB, DE, ...}, superposter_status ∈ {is_superposter, is_not_superposter})
--
-- Why multiple queries?
--   This script is intentionally modular so you can run each query, inspect the output,
--   and (optionally) export intermediate results to CSV/Parquet and analyze/pivot in pandas.
--
-- Assumptions / inputs:
-- - `archive_generated_feeds` contains: feed_id, user, bluesky_handle, condition,
--     feed_generation_timestamp, feed (JSON array string of objects like [{"item":"at://..."}])
-- - `archive_preprocessed_posts` contains: uri, author_did, partition_date (day of the post)
-- - `superposter_author_dids` is a VIEW with: partition_date, author_did
--     (see `create_all_superposters_view.py` in this folder)
-- - `archive_ml_inference_perspective_api` contains labels with:
--     uri, prob_constructive, prob_reasoning, label_timestamp, was_successfully_labeled
--
-- Notes:
-- - We compute *two* constructiveness measures:
--     - `prob_constructive` (what you said you likely want to average)
--     - `prob_constructive_fixed` = COALESCE(prob_constructive, prob_reasoning)
--       (keeps parity with `average_constructive_by_superposter_status.sql`)
-- - Most downstream aggregations should filter to labeled rows; this script makes it explicit.


/* -------------------------------------------------------------------------- */
/* QUERY 1: Explode generated feeds into one row per (feed_id, uri)            */
/* -------------------------------------------------------------------------- */
-- Output columns:
--   feed_id, user, bluesky_handle, condition, feed_generation_timestamp, uri
--
-- Run this first and export as (example): feed_posts.csv
WITH
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
  -- De-dupe within a feed (sometimes the same URI can appear multiple times).
  SELECT DISTINCT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    uri
  FROM feed_posts_raw
)
SELECT *
FROM feed_posts
ORDER BY feed_generation_timestamp, feed_id, uri;


/* -------------------------------------------------------------------------- */
/* QUERY 2: Attach post metadata (author_did + post_date) to feed_posts        */
/* -------------------------------------------------------------------------- */
-- Output columns:
--   feed_id, user, bluesky_handle, condition, feed_generation_timestamp, uri,
--   author_did, post_date
--
-- Run this second and export as (example): feed_posts_with_post_meta.csv
WITH
feeds AS (
  SELECT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
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
  SELECT DISTINCT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    uri
  FROM feed_posts_raw
),
post_meta AS (
  -- If `archive_preprocessed_posts` has multiple rows per uri, pick a deterministic one.
  -- (Most likely it's unique, but this keeps things robust.)
  SELECT
    uri,
    max_by(author_did, partition_date) AS author_did,
    CAST(max(partition_date) AS date) AS post_date
  FROM archive_preprocessed_posts
  GROUP BY 1
)
SELECT
  fp.feed_id,
  fp.user,
  fp.bluesky_handle,
  fp.condition,
  fp.feed_generation_timestamp,
  fp.uri,
  pm.author_did,
  pm.post_date
FROM feed_posts fp
JOIN post_meta pm
  ON fp.uri = pm.uri
-- If you want to keep feed rows that couldn't be joined to post_meta, switch to LEFT JOIN.
ORDER BY fp.feed_generation_timestamp, fp.feed_id, fp.uri;


/* -------------------------------------------------------------------------- */
/* QUERY 3: Deduplicate labels per URI (keep latest), keep "fixed" fallback    */
/* -------------------------------------------------------------------------- */
-- Output columns:
--   uri, prob_constructive, prob_reasoning, prob_constructive_fixed, label_ts
--
-- Run this third and export as (example): labels_dedup.csv
WITH
labels_filtered AS (
  SELECT
    l.uri,
    l.prob_constructive,
    l.prob_reasoning,
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
    prob_reasoning,
    COALESCE(prob_constructive, prob_reasoning) AS prob_constructive_fixed,
    label_ts
  FROM labels_filtered
  WHERE rn = 1
)
SELECT *
FROM labels_dedup
ORDER BY uri;


/* -------------------------------------------------------------------------- */
/* QUERY 4: Build per-(feed_id, uri) fact table w/ superposter window + labels */
/* -------------------------------------------------------------------------- */
-- Output columns:
--   feed_id, user, bluesky_handle, condition, feed_generation_timestamp,
--   uri, author_did, post_date,
--   is_superposter_in_0_to_4d_window,
--   prob_constructive, prob_constructive_fixed
--
-- This is the most useful intermediate to export for pandas:
--   (example) feed_posts_labeled_with_superposter_flag.csv
WITH
feeds AS (
  SELECT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
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
  SELECT DISTINCT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    uri
  FROM feed_posts_raw
),
post_meta AS (
  SELECT
    uri,
    max_by(author_did, partition_date) AS author_did,
    CAST(max(partition_date) AS date) AS post_date
  FROM archive_preprocessed_posts
  GROUP BY 1
),
labels_filtered AS (
  SELECT
    l.uri,
    l.prob_constructive,
    l.prob_reasoning,
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
    COALESCE(prob_constructive, prob_reasoning) AS prob_constructive_fixed
  FROM labels_filtered
  WHERE rn = 1
),
feed_posts_enriched AS (
  SELECT
    fp.feed_id,
    fp.user,
    fp.bluesky_handle,
    fp.condition,
    fp.feed_generation_timestamp,
    fp.uri,
    pm.author_did,
    pm.post_date
  FROM feed_posts fp
  JOIN post_meta pm
    ON fp.uri = pm.uri
)
SELECT
  fpe.*,
  EXISTS (
    SELECT 1
    FROM superposter_author_dids s
    WHERE s.author_did = fpe.author_did
      AND CAST(s.partition_date AS date)
          BETWEEN fpe.post_date AND fpe.post_date + INTERVAL '4' DAY
  ) AS is_superposter_in_0_to_4d_window,
  ld.prob_constructive,
  ld.prob_constructive_fixed
FROM feed_posts_enriched fpe
LEFT JOIN labels_dedup ld
  ON fpe.uri = ld.uri
ORDER BY fpe.feed_generation_timestamp, fpe.feed_id, fpe.uri;


/* -------------------------------------------------------------------------- */
/* QUERY 5: Final aggregation for the matrix: avg constructiveness by group    */
/* -------------------------------------------------------------------------- */
-- Output rows:
--   condition, superposter_status, avg_prob_constructive, avg_prob_constructive_fixed,
--   n_post_rows, n_labeled_post_rows, n_distinct_uris, n_distinct_authors, n_distinct_feeds, n_distinct_users
--
-- This gives you a tidy long-form table; pivot in pandas into the RC/EB/DE matrix.
WITH
feeds AS (
  SELECT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
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
  SELECT DISTINCT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    uri
  FROM feed_posts_raw
),
post_meta AS (
  SELECT
    uri,
    max_by(author_did, partition_date) AS author_did,
    CAST(max(partition_date) AS date) AS post_date
  FROM archive_preprocessed_posts
  GROUP BY 1
),
labels_filtered AS (
  SELECT
    l.uri,
    l.prob_constructive,
    l.prob_reasoning,
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
    COALESCE(prob_constructive, prob_reasoning) AS prob_constructive_fixed
  FROM labels_filtered
  WHERE rn = 1
),
fact AS (
  SELECT
    fp.feed_id,
    fp.user,
    fp.bluesky_handle,
    fp.condition,
    fp.feed_generation_timestamp,
    fp.uri,
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
)
SELECT
  condition,
  CASE
    WHEN is_superposter_in_0_to_4d_window THEN 'is_superposter'
    ELSE 'is_not_superposter'
  END AS superposter_status,
  avg(prob_constructive) AS avg_prob_constructive,
  avg(prob_constructive_fixed) AS avg_prob_constructive_fixed,
  count(*) AS n_post_rows,
  count(prob_constructive_fixed) AS n_labeled_post_rows,
  count(DISTINCT uri) AS n_distinct_uris,
  count(DISTINCT author_did) AS n_distinct_authors,
  count(DISTINCT feed_id) AS n_distinct_feeds,
  count(DISTINCT user) AS n_distinct_users
FROM fact
GROUP BY 1, 2
ORDER BY 1, 2;


/* -------------------------------------------------------------------------- */
/* OPTIONAL QUERY 6: Pivot-like output in SQL (if you want it)                 */
/* -------------------------------------------------------------------------- */
-- If your conditions are exactly {'RC','EB','DE'}, this produces the matrix directly.
-- (Still recommend pivoting in pandas since it’s easier to sanity check.)
WITH grouped AS (
  WITH
  feeds AS (
    SELECT
      feed_id,
      user,
      bluesky_handle,
      condition,
      feed_generation_timestamp,
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
    SELECT DISTINCT
      feed_id,
      user,
      bluesky_handle,
      condition,
      feed_generation_timestamp,
      uri
    FROM feed_posts_raw
  ),
  post_meta AS (
    SELECT
      uri,
      max_by(author_did, partition_date) AS author_did,
      CAST(max(partition_date) AS date) AS post_date
    FROM archive_preprocessed_posts
    GROUP BY 1
  ),
  labels_filtered AS (
    SELECT
      l.uri,
      l.prob_constructive,
      l.prob_reasoning,
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
      COALESCE(prob_constructive, prob_reasoning) AS prob_constructive_fixed
    FROM labels_filtered
    WHERE rn = 1
  ),
  fact AS (
    SELECT
      fp.condition,
      EXISTS (
        SELECT 1
        FROM superposter_author_dids s
        WHERE s.author_did = pm.author_did
          AND CAST(s.partition_date AS date)
              BETWEEN pm.post_date AND pm.post_date + INTERVAL '4' DAY
      ) AS is_superposter_in_0_to_4d_window,
      ld.prob_constructive
    FROM feed_posts fp
    JOIN post_meta pm
      ON fp.uri = pm.uri
    LEFT JOIN labels_dedup ld
      ON fp.uri = ld.uri
  )
  SELECT
    CASE
      WHEN is_superposter_in_0_to_4d_window THEN 'is_superposter'
      ELSE 'is_not_superposter'
    END AS superposter_status,
    condition,
    avg(prob_constructive) AS avg_prob_constructive
  FROM fact
  GROUP BY 1, 2
)
SELECT
  superposter_status,
  max(CASE WHEN condition = 'RC' THEN avg_prob_constructive END) AS rc,
  max(CASE WHEN condition = 'EB' THEN avg_prob_constructive END) AS eb,
  max(CASE WHEN condition = 'DE' THEN avg_prob_constructive END) AS de
FROM grouped
GROUP BY 1
ORDER BY 1;


