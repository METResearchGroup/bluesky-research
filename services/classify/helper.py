from typing import Optional

from atproto_client.models.app.bsky.feed.defs import FeedViewPost

from services.transform.transform_raw_data import (
    flatten_feed_view_post, FlattenedFeedViewPost
)


def preprocess_post_for_inference(
    post: FeedViewPost
) -> Optional[FlattenedFeedViewPost]:
    """Processes a post for inference. Grabs only the necessary fields, as
    well as doing any processing.
    """
    flattened_post = flatten_feed_view_post(post)
    # return only English-language posts
    if "langs" not in flattened_post or flattened_post["langs"] is None:
        return None
    lang = flattened_post["langs"][0]
    if lang != "en":
        return None
    # return only posts with content
    if (
        "text" not in flattened_post
        or flattened_post["text"] is None
        or flattened_post["text"] == ""
        or len(flattened_post["text"]) == 0
    ):
        return None
    return flattened_post


def preprocess_posts_for_inference(
    posts: list[FeedViewPost]
) -> list[FlattenedFeedViewPost]:
    preprocessed_posts = []
    for post in posts:
        preprocessed_post = preprocess_post_for_inference(post)
        if preprocessed_post:
            preprocessed_posts.append(preprocessed_post)
    return preprocessed_posts