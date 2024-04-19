"""Rank candidates posts for the feed of each user.

Returns the latest feeds per user, rank ordered based on the criteria in their
study condition.
"""
from services.create_feeds.condition.engagement import rank_posts_for_user as engagement_rank_posts_for_user
from services.create_feeds.condition.representative_diversification import rank_posts_for_user as representative_diversification_rank_posts_for_user
from services.create_feeds.condition.reverse_chronological import (
    rank_posts_for_user as reverse_chronological_rank_posts_for_user,
    create_generic_reverse_chronological_feed
)
from services.participant_data.helper import get_user_to_bluesky_profiles_as_list_dicts # noqa


def get_study_users() -> list[dict]:
    """Get the users in the study."""
    return get_user_to_bluesky_profiles_as_list_dicts()


def generate_user_feed(user: dict) -> list[dict]:
    """Given a specific user, generate their ranked user feed."""
    if user['condition'] == 'engagement':
        return engagement_rank_posts_for_user(user)
    elif user['condition'] == 'representative_diversification':
        return representative_diversification_rank_posts_for_user(user)
    elif user['condition'] == 'reverse_chronological':
        return reverse_chronological_rank_posts_for_user(user)
    else:
        raise ValueError(f"Unknown condition: {user['condition']}")
    

def get_reverse_chronological_users(users: list[dict]) -> list[dict]:
    """Get the users in the reverse-chronological condition."""
    return [
        user for user in users if user['condition'] == 'reverse_chronological'
    ]


def create_only_reverse_chronological_feeds() -> dict:
    """Create feeds only for users in the reverse-chronological condition."""
    users: list[dict] = get_study_users()
    reverse_chronological_users: list[dict] = get_reverse_chronological_users(users)
    reverse_chronological_feed: list[dict] = create_generic_reverse_chronological_feed() # noqa
    return {
        user['bluesky_user_did']: reverse_chronological_feed
        for user in reverse_chronological_users
    }


def create_ranked_feeds_per_user() -> dict:
    """Ranks latest candidates per user.
    
    Returns a dict whose keys are user IDs and whose values are
    the list of posts for their feed.

    These are the posts that, in rank order, are the posts that we would
    recommend in the feed of that user.
    """
    users: list[dict] = get_study_users()
    return {
        user['bluesky_user_did']: generate_user_feed(user) for user in users
    }


if __name__ == "__main__":
    res = create_only_reverse_chronological_feeds()
