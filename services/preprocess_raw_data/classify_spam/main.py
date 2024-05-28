"""Classify if a post has spam."""
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_spam.helper import (
    classify_if_posts_have_no_spam
)


def filter_posts_have_no_spam(
    posts: list[ConsolidatedPostRecordModel]
) -> list[dict]:
    return classify_if_posts_have_no_spam(posts=posts)


if __name__ == "__main__":
    filter_posts_have_no_spam()
