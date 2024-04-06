"""Classifies a possible post as from a bot account."""
from services.filter_raw_data.classify_bots.update_bot_accounts import load_bot_accounts # noqa


bot_user_ids: set = load_bot_accounts()

def classify(post: dict) -> bool:
    """Classifies a post as from a bot account."""
    return post["author_id"] in bot_user_ids
