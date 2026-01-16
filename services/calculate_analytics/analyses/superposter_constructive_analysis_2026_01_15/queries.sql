/* Get the average constructiveness of posts used in feeds AND whose author is a superposter
Result: 0.17906264253783263
*/
SELECT AVG(prob_constructive)
FROM archive_ml_inference_perspective_api
WHERE uri IN (
    SELECT uri
    FROM archive_preprocessed_posts
    WHERE uri in (
        /* Post is used in feeds */
        SELECT uri FROM archive_fetch_posts_used_in_feeds
    ) AND author_did IN (
        /* Author is a superposter */
        SELECT author_did FROM superposter_author_dids
    )
)

/* Get the average constructiveness of posts used in feeds 
Result: 0.17893248619002697
*/
SELECT AVG(prob_constructive)
FROM archive_ml_inference_perspective_api
WHERE uri IN (
    SELECT uri
    FROM archive_preprocessed_posts
    WHERE uri in (
        /* Post is used in feeds */
        SELECT uri FROM archive_fetch_posts_used_in_feeds
    )
)

/* Get the average constructiveness of posts used in feeds AND whose author is NOT a superposter
Result: 0.17033583496552065

So, if we were to penalize posts by superposters, turns out superposter posts
tend to be higher constructiveness on average.
*/
SELECT AVG(prob_constructive)
FROM archive_ml_inference_perspective_api
WHERE uri IN (
    SELECT uri
    FROM archive_preprocessed_posts
    WHERE uri in (
        /* Post is used in feeds */
        SELECT uri FROM archive_fetch_posts_used_in_feeds
    ) AND author_did NOT IN (
        /* Author is NOT a superposter */
        SELECT author_did FROM superposter_author_dids
    )
)
