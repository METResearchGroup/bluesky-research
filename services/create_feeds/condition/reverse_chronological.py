"""Logic for reverse-chronological feed creation."""
from services.create_feeds.condition.helper import DEFAULT_FEED_LENGTH
from services.create_feeds.models import UserFeedModel
from services.ml_inference.models import RecordClassificationMetadataModel
from services.participant_data.models import UserToBlueskyProfileModel


# TODO: for now, doesn't have personalization, everyone in the condition gets
# the same feed.
def create_reverse_chronological_feeds(
    users: list[UserToBlueskyProfileModel],
    posts: list[RecordClassificationMetadataModel]
) -> list[UserFeedModel]:
    """Returns a feed sorted by reverse chronological order."""
    feed = sorted(posts, key=lambda x: x.synctimestamp, reverse=True)
    feed = feed[:DEFAULT_FEED_LENGTH]
    user_feeds: list[UserFeedModel] = [
        UserFeedModel(user=user, feed=feed) for user in users
    ]
    return user_feeds


if __name__ == "__main__":
    pass
