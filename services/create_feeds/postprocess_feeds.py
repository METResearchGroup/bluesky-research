"""Runs any postprocessing steps for feeds."""

def postprocess_feed(user: str, feed: list[dict]) -> list[dict]:
    """Postprocesses a feed for a given user."""
    pass


def postprocess_feeds(user_to_feed_dict: dict) -> dict:
    """Postprocesses a feed for a given user."""
    return {
        user: postprocess_feed(user, feed)
        for (user, feed) in user_to_feed_dict.items()
    }
