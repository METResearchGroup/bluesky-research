"""Classifies a post as possible spam.

For now we'll use a placeholder until we come back and properly classify spam.
"""
from services.preprocess_raw_data.update_bluesky_mute_lists.database import get_muted_users_as_list_dicts # noqa

muted_users: list[dict] = get_muted_users_as_list_dicts()
muted_users_dids = set([user["did"] for user in muted_users])


def classify(post: dict) -> bool:
    """Classifies a post as spam."""
    # at scale, we should use a bloom filter
    return post["author"] in muted_users_dids
