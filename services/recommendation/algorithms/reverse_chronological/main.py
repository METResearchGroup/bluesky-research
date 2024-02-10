"""Reverse-chronological ranking service."""
from atproto_client.models.app.bsky.feed.defs import SkeletonFeedPost

from services.ranking.reverse_chronological.helper import generate_feed


def main(event: dict, context: dict) -> list[SkeletonFeedPost]:
    return generate_feed()


if __name__ == "__main__":
    event = {}
    context = {}
