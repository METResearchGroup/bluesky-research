from atproto_client.models.app.bsky.feed.defs import SkeletonFeedPost

from services.ranking.helper import create_feed_generator


def get_posts() -> list:
    """Get posts to rank."""
    pass


def rank_posts() -> list[str]:
    """Return a list of post URIs in reverse chronological order."""
    return []


def generate_feed() -> list[SkeletonFeedPost]:
    """Generate a feed of posts in reverse chronological order."""
    posts = get_posts()
    ranked_post_uris = rank_posts(posts)
    feed = create_feed_generator(ranked_post_uris)
    return feed