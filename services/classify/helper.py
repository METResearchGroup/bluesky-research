from atproto_client.models.app.bsky.feed.defs import FeedViewPost

from services.transform.transform_raw_data import (
    flatten_feed_view_post, FlattenedFeedViewPost
)


def preprocess_post_for_inference(post: FeedViewPost) -> FlattenedFeedViewPost:
    """Processes a post for inference. Grabs only the necessary fields, as
    well as doing any processing.
    """
    flattened_post = flatten_feed_view_post(post)
    lang = flattened_post["langs"][0]
    if lang == "en": # return only English-language posts
        return flattened_post


def preprocess_posts_for_inference(
    posts: list[FeedViewPost]
) -> list[FlattenedFeedViewPost]:
    return [preprocess_post_for_inference(post) for post in posts]
