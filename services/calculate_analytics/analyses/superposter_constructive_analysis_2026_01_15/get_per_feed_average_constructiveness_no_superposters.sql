/*
Compute per-feed constructiveness exactly like `get_per_feed_average_constructiveness.sql`,
but **exclude** any feed post whose author is a superposter on ANY day in:

  [post_date, post_date + 4 days]

(4 days is the study lookback window used elsewhere in this repo.)

This produces the same per-feed outputs:
  - feed_id, bluesky_handle, condition, feed_generation_timestamp
  - prop_constructive_labeled (share of labeled posts with prob_constructive >= 0.5)
  - n_posts_in_feed (after filtering out superposter-authored posts)
  - n_labeled_posts_in_feed

Dependencies / assumptions:
  - `superposter_author_dids` view exists (see
    `services/calculate_analytics/analyses/superposter_constructive_analysis_2026_01_15/create_all_superposters_view.py`)
  - `archive_preprocessed_posts` provides uri -> (author_did, partition_date)
  - `archive_generated_feeds` contains the feed JSON array of post objects with `$.item` URIs
  - `archive_ml_inference_perspective_api` contains `prob_constructive` labels
*/

WITH
feeds AS (
  SELECT
    feed_id,
    user,
    bluesky_handle,
    condition,
    feed_generation_timestamp,
    -- `feed` is not always a clean JSON array string in Athena. Some rows appear to look like:
    --   [{"item":"at://...","is_in_network":false}, ...] "[{\"item\":\"at://...\"...}]"
    -- So we parse defensively:
    -- - try raw json_parse(feed)
    -- - try extracting the *first* JSON array substring before a space+quote: ^(\[.*\])\s"
    -- - try unescaping \" -> " first
    -- - try both extraction + unescape
    --
    -- Final fallback to empty array to avoid UNNEST(NULL) errors.
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
      ),
      CAST(ARRAY[] AS array(json))
    ) AS feed_items
  FROM archive_generated_feeds
),

-- Explode the feed JSON array into one row per post URI in the feed.
feed_posts AS (
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

-- Attach author + post_date so we can apply the superposter window filter.
post_meta AS (
  SELECT
    uri,
    max_by(author_did, partition_date) AS author_did,
    CAST(max(partition_date) AS date) AS post_date
  FROM archive_preprocessed_posts
  GROUP BY 1
),

feed_posts_with_meta AS (
  SELECT
    p.feed_id,
    p.user,
    p.bluesky_handle,
    p.condition,
    p.feed_generation_timestamp,
    p.uri,
    pm.author_did,
    pm.post_date
  FROM feed_posts p
  JOIN post_meta pm
    ON p.uri = pm.uri
),

-- Filter out posts authored by users who are superposters within the lookback window.
feed_posts_no_superposters AS (
  SELECT
    fp.*
  FROM feed_posts_with_meta fp
  WHERE NOT EXISTS (
    SELECT 1
    FROM superposter_author_dids s
    WHERE s.author_did = fp.author_did
      AND CAST(s.partition_date AS date)
          BETWEEN fp.post_date AND fp.post_date + INTERVAL '4' DAY
  )
),

distinct_uris AS (
  SELECT DISTINCT uri
  FROM feed_posts_no_superposters
),

-- Filter to just URIs used in these feeds and dedupe per URI.
labels_filtered AS (
  SELECT
    l.uri,
    l.prob_constructive,
    try(date_parse(l.label_timestamp, '%Y-%m-%d-%H:%i:%s')) AS label_ts,
    row_number() OVER (
      PARTITION BY l.uri
      ORDER BY try(date_parse(l.label_timestamp, '%Y-%m-%d-%H:%i:%s')) DESC NULLS LAST
    ) AS rn
  FROM archive_ml_inference_perspective_api l
  WHERE l.uri IN (SELECT uri FROM distinct_uris)
    AND l.prob_constructive IS NOT NULL
),

labels_dedup AS (
  SELECT uri, prob_constructive
  FROM labels_filtered
  WHERE rn = 1
)

-- Final output: one row per feed with prop_constructive_labeled over its (non-superposter) posts.
SELECT
  p.feed_id,
  p.bluesky_handle,
  p.condition,
  p.feed_generation_timestamp,
  avg(
    CASE
      WHEN l.prob_constructive IS NULL THEN NULL
      WHEN l.prob_constructive >= 0.5 THEN 1.0
      ELSE 0.0
    END
  ) AS prop_constructive_labeled,
  count(*) AS n_posts_in_feed,
  count(l.prob_constructive) AS n_labeled_posts_in_feed
FROM feed_posts_no_superposters p
LEFT JOIN labels_dedup l
  ON p.uri = l.uri
GROUP BY 1,2,3,4
ORDER BY feed_generation_timestamp, feed_id;


