"""Logic for representative diversification feed creation."""
from services.create_feeds.models import UserFeedModel
from services.create_feeds.score_posts import score_posts
from services.ml_inference.models import RecordClassificationMetadataModel
from services.participant_data.models import UserToBlueskyProfileModel


# TODO: for now, doesn't have personalization, everyone in the condition gets
# the same feed.
def create_representative_diversification_feeds(
    users: list[UserToBlueskyProfileModel],
    posts: list[RecordClassificationMetadataModel]
) -> list[UserFeedModel]:
    """Returns a feed sorted by representative diversification score."""
    post_scores: list[float] = score_posts(posts)
    sorted_posts = sorted(zip(post_scores, posts), reverse=True)
    feed = [post for _, post in sorted_posts]
    user_feeds: list[UserFeedModel] = [
        UserFeedModel(user=user, feed=feed) for user in users
    ]
    return user_feeds


if __name__ == "__main__":
    pass
