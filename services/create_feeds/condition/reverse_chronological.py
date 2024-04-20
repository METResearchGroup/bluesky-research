"""Logic for reverse-chronological feed creation."""
from services.create_feeds.constants import DEFAULT_NUMBER_POSTS_PER_FEED
from services.preprocess_raw_data.database import get_filtered_posts_as_list_dicts  # noqa


def rank_posts_for_user(user: dict) -> list[dict]:
    """Ranks the posts for a user in the condition.

    For the reverse-chronological condition, the candidate posts are just the
    most recent posts, sorted by timestamp descending.
    """
    pass


def create_generic_reverse_chronological_feed() -> list[dict]:
    """Creates a generic reverse chronological feed based on timestamp.

    Since all the users in the reverse-chronological condition get posts based
    on their timestamp, we just return the most recent posts for each user.

    This will change in the future once we include information about their
    social network and we then give them posts from their friends.
    """
    posts: list[dict] = get_filtered_posts_as_list_dicts(
        k=DEFAULT_NUMBER_POSTS_PER_FEED,
        order_by="synctimestamp",
        desc=True
    )
    return posts


if __name__ == "__main__":
    create_generic_reverse_chronological_feed()
