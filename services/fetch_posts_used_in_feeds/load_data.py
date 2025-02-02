from typing import Literal


def load_feeds_from_remote() -> list[dict]:
    pass


def load_feeds_from_local() -> list[dict]:
    """Loads the feeds from local storage."""
    # root_path = "/Users/mark/Documents/work/bluesky-research/scripts/analytics/feeds"
    pass


def load_feeds(source: Literal["local", "s3"]) -> list[dict]:
    """Loads the feeds from the source."""
    if source == "local":
        return load_feeds_from_local()
    else:
        return load_feeds_from_remote()
