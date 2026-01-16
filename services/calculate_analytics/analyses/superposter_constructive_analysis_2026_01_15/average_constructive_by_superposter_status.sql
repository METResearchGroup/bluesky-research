-- Average constructiveness of posts used in feeds whose author is a
-- superposter on any day in [post date, post date + 4 days] (we use 4 days
-- since that was the lookback period in our study).

WITH used_posts AS (
  SELECT
    p.uri,
    p.author_did,
    CAST(p.partition_date AS DATE) AS post_date
  FROM archive_preprocessed_posts p
  JOIN archive_fetch_posts_used_in_feeds f
    ON p.uri = f.uri
),
labeled_used_posts AS (
  SELECT
    u.post_date,
    u.author_did,
    u.uri,
    COALESCE(m.prob_constructive, m.prob_reasoning) AS prob_constructive_fixed
  FROM used_posts u
  JOIN archive_ml_inference_perspective_api m
    ON u.uri = m.uri
  WHERE m.was_successfully_labeled = TRUE
),
wave1 AS (
  SELECT lup.*
  FROM labeled_used_posts lup
  WHERE EXISTS (
    SELECT 1
    FROM superposter_author_dids s
    WHERE s.author_did = lup.author_did
      AND CAST(s.partition_date AS DATE)
            BETWEEN lup.post_date AND lup.post_date + INTERVAL '4' DAY
  )
)
SELECT
  post_date AS partition_date,
  AVG(prob_constructive_fixed) AS avg_constructiveness,
  COUNT(*) AS n_posts,
  COUNT(DISTINCT author_did) AS n_authors
FROM wave1
GROUP BY 1
ORDER BY 1 ASC;

-- Average constructiveness of posts used in feeds whose author is NOT a
-- superposter on any day in [post date, post date + 4 days] (we use 4 days
-- since that was the lookback period in our study).
WITH used_posts AS (
  SELECT
    p.uri,
    p.author_did,
    CAST(p.partition_date AS DATE) AS post_date
  FROM archive_preprocessed_posts p
  JOIN archive_fetch_posts_used_in_feeds f
    ON p.uri = f.uri
),
labeled_used_posts AS (
  SELECT
    u.post_date,
    u.author_did,
    u.uri,
    COALESCE(m.prob_constructive, m.prob_reasoning) AS prob_constructive_fixed
  FROM used_posts u
  JOIN archive_ml_inference_perspective_api m
    ON u.uri = m.uri
  WHERE m.was_successfully_labeled = TRUE
),
wave2 AS (
  SELECT lup.*
  FROM labeled_used_posts lup
  WHERE NOT EXISTS (
    SELECT 1
    FROM superposter_author_dids s
    WHERE s.author_did = lup.author_did
      AND CAST(s.partition_date AS DATE)
            BETWEEN lup.post_date AND lup.post_date + INTERVAL '4' DAY
  )
)
SELECT
  post_date AS partition_date,
  AVG(prob_constructive_fixed) AS avg_constructiveness,
  COUNT(*) AS n_posts,
  COUNT(DISTINCT author_did) AS n_authors
FROM wave2
GROUP BY 1
ORDER BY 1 ASC;