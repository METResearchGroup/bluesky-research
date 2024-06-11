"""Rank candidates posts for the feed of each user.

Returns the latest feeds per user, rank ordered based on the criteria in their
study condition.
"""
from services.create_feeds.condition.engagement import create_engagement_feeds
from services.create_feeds.condition.representative_diversification import (
    create_representative_diversification_feeds
)
from services.create_feeds.condition.reverse_chronological import (
    create_reverse_chronological_feeds
)
from services.create_feeds.load_data import (
    load_firehose_based_posts, load_most_liked_based_posts
)
from services.create_feeds.models import UserFeedModel
from services.ml_inference.models import RecordClassificationMetadataModel
from services.participant_data.models import UserToBlueskyProfileModel


def create_firehose_based_feeds(
    condition_to_user_map: dict[str, list[UserToBlueskyProfileModel]]
) -> list[UserFeedModel]:
    """Create feeds that are based off posts from the firehose."""
    reverse_chronological_users = condition_to_user_map["reverse_chronological"]  # noqa
    if len(reverse_chronological_users) > 0:
        posts: list[RecordClassificationMetadataModel] = (
            load_firehose_based_posts()
        )
        feeds: list[UserFeedModel] = create_reverse_chronological_feeds(
            users=reverse_chronological_users, posts=posts
        )
        return feeds
    else:
        print("No users in reverse_chronological condition.")
        return []


def create_most_liked_feeds(
    condition_to_user_map: dict[str, list[UserToBlueskyProfileModel]]
) -> list[UserFeedModel]:
    """Create feeds that are based off posts from the most liked feeds."""
    output_feeds = []

    engagement_users = condition_to_user_map["engagement"]
    representative_diversification_users = condition_to_user_map["representative_diversification"]  # noqa

    if len(engagement_users) > 0 or len(representative_diversification_users) > 0:
        posts: list[RecordClassificationMetadataModel] = load_most_liked_based_posts()  # noqa
        if len(engagement_users) > 0:
            feeds: list[UserFeedModel] = create_engagement_feeds(
                users=engagement_users, posts=posts
            )
            output_feeds.extend(feeds)
        if len(representative_diversification_users) > 0:
            feeds: list[UserFeedModel] = create_representative_diversification_feeds(  # noqa
                users=representative_diversification_users, posts=posts
            )
            output_feeds.extend(feeds)
    else:
        print("No users in engagement or representative condition condition.")
        return []


def create_feeds_per_condition(
    condition_to_user_map: dict[str, list[UserToBlueskyProfileModel]]
) -> list[UserFeedModel]:
    """Create feeds for each condition."""
    output_feeds: list[UserFeedModel] = []

    firehose_feeds: list[UserFeedModel] = create_firehose_based_feeds(condition_to_user_map)  # noqa
    output_feeds.extend(firehose_feeds)

    most_liked_feeds: list[UserFeedModel] = create_most_liked_feeds(condition_to_user_map)  # noqa
    output_feeds.extend(most_liked_feeds)

    return output_feeds


if __name__ == "__main__":
    pass
