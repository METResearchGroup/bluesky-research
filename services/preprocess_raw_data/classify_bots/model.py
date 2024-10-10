"""Classifies a possible post as from a bot account.

Bot classification is in-progress but not yet on the Bluesky roadmap (as of 2024-04-08)
- https://github.com/bluesky-social/atproto/issues/2167 suggests that it'll likely be implemented as a label.
-
"""  # noqa

from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa

bot_user_dids: set = {}
bot_user_handles: set = {}


def classify(post: ConsolidatedPostRecordModel) -> bool:
    """Classifies a post as from a bot account."""
    # return post["author"] in bot_user_dids
    return False
