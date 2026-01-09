-- One-statement (single query) Athena/Presto SQL version of `experiment.py`.
--
-- Assumptions / mapping:
-- - feeds_table has columns:
--     feed_id, user, bluesky_handle, condition, feed_generation_timestamp, feed
--   where `feed` is a JSON string representing an array of objects like:
--     [{"item":"at://...","is_in_network":false}, ...]
-- - labels_table has columns:
--     uri, prob_constructive, label_timestamp
--
-- If your `feed` string is escaped (e.g., contains `{\"item\":\"...\"}`), you may need
-- to add a cleanup step before json_parse(), for example:
--   json_parse(replace(feed, '\\\"', '\"'))
--
-- Replace these with your real Athena table names:
--   feeds_table  -> archive_generated_feeds
--   labels_table -> archive_ml_inference_perspective_api

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

distinct_uris AS (
  SELECT DISTINCT uri
  FROM feed_posts
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

-- Final output: one row per feed with avg(prob_constructive) over its posts.
SELECT
  p.feed_id,
  p.bluesky_handle,
  p.condition,
  p.feed_generation_timestamp,
  avg(l.prob_constructive) AS avg_prob_constructive,
  count(*) AS n_posts_in_feed,
  count(l.prob_constructive) AS n_labeled_posts_in_feed
FROM feed_posts p
LEFT JOIN labels_dedup l
  ON p.uri = l.uri
GROUP BY 1,2,3,4
ORDER BY feed_generation_timestamp, feed_id;
