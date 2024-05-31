"""Helper functionalities for classifying records."""
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel
from services.ml_inference.models import (
    PerspectiveApiLabelsModel, RecordClassificationMetadataModel,
    SociopoliticalLabelsModel
)

MIN_TEXT_LENGTH = 8

# TODO: probably should be moved to database file?
def load_latest_preprocessed_posts(
    latest_classification_timestamp: str
) -> list[FilteredPreprocessedPostModel]:
    """Fetches latest preprocessed posts for classification.

    We identify these as the preprocessed posts whose "preprocessing_timestamp"
    is later than the latest classification timestamp for the given classifier.
    """
    pass


def get_post_metadata_for_classification(
    preprocessed_posts: list[FilteredPreprocessedPostModel]
) -> list[RecordClassificationMetadataModel]:
    """Fetch the text of each post and return a list of dicts with the
    post URIs as well as the text of the post.

    Also returns only the posts that will need to be labeled by the Perspective
    API. All others will be given a default label.
    """
    res: list[RecordClassificationMetadataModel] = []

    for post in preprocessed_posts:
        res.append(
            RecordClassificationMetadataModel(
                uri=post.uri,
                text=post.record.text,
                synctimestamp=post.synctimestamp,
                preprocessing_timestamp=post.preprocessing_timestamp,
                source=post.metadata.source,
                url=post.metadata.url,
                like_count=post.metrics.like_count,
                reply_count=post.metrics.reply_count,
                repost_count=post.metrics.repost_count,
            )
        )
    return res


def validate_posts(
    posts_to_classify: list[RecordClassificationMetadataModel]
) -> tuple[list[RecordClassificationMetadataModel], list[RecordClassificationMetadataModel]]: # noqa
    """Filter which posts should be sent to the Perspective API or not.

    For now, this'll just be a minimum character count. Mostly used as a simple
    filter for posts that don't have any words or might have 1-2 words.
    """
    valid_posts: list[RecordClassificationMetadataModel] = []
    invalid_posts: list[RecordClassificationMetadataModel] = []

    for post in posts_to_classify:
        if len(post.text) >= MIN_TEXT_LENGTH:
            valid_posts.append(post)
        else:
            invalid_posts.append(post)
    return (valid_posts, invalid_posts)

