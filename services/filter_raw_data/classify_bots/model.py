"""Classifies a possible post as from a bot account.

Bot classification is in-progress but not yet on the Bluesky roadmap (as of 2024-04-08)
- https://github.com/bluesky-social/atproto/issues/2167 suggests that it'll likely be implemented as a label.
- 
""" # noqa
from services.filter_raw_data.classify_bots.update_bot_accounts import load_bot_accounts # noqa


bot_user_dids: set = load_bot_accounts()

def classify(post: dict) -> bool:
    """Classifies a post as from a bot account."""
    #return post["author"] in bot_user_dids
    return False
