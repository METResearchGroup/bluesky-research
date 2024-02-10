"""Handler for providing the feed that we have curated for a given user."""
from services.manage_bluesky_feeds.helper import get_latest_feed_for_user


def main(event: dict, context: dict) -> list:
    """Main handler for providing Bluesky with the feed for a user."""
    user_did = event["user_did"]
    return get_latest_feed_for_user(user_did)


if __name__ == "__main__":
    event = {"user_did": "did:example"}
    context = {}
    main(event=event, context=context)
