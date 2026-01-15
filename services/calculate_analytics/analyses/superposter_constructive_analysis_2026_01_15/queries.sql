/* Get the average constructiveness of posts used in feeds AND whose author is a superposter
Result: 0.07387768
*/
SELECT AVG(prob_constructive)
FROM ml_inference_perspective_api
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

