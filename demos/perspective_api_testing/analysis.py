"""Analyze the labeled data from the Perspective API."""
from demos.perspective_api_testing.helper import label_mongodb_collection
from demos.perspective_api_testing.models import PerspectiveAPIClassification
from lib.db.mongodb import load_collection
from services.preprocess_raw_data.classify_language.helper import text_is_english  # noqa


def load_labeled_data() -> list[PerspectiveAPIClassification]:
    """Loads labeled data from the MongoDB collection."""
    labeled_data: list[dict] = load_collection(
        collection=label_mongodb_collection, limit=None
    )
    transformed_labeled_data = [
        PerspectiveAPIClassification(**labeled_post) for labeled_post in labeled_data
    ]
    print(f"Loaded {len(transformed_labeled_data)} labeled posts from MongoDB.")  # noqa
    return transformed_labeled_data


def has_text(classification: PerspectiveAPIClassification) -> bool:
    """Returns True if the post has text."""
    return len(classification.text) > 0


def post_is_english(classification: PerspectiveAPIClassification) -> bool:
    """Returns True if the post is in English."""
    return text_is_english(classification.text)


filter_funcs = [has_text, post_is_english]


def filter_labeled_data(labeled_data: list[PerspectiveAPIClassification]) -> dict:  # noqa
    """Filters labeled data into passed and failed filters."""
    passed_filters: list[PerspectiveAPIClassification] = []
    failed_filters: list[PerspectiveAPIClassification] = []

    for post in labeled_data:
        if all([filter_func(post) for filter_func in filter_funcs]):
            passed_filters.append(post)
        else:
            failed_filters.append(post)

    print(f"Passed filters: {len(passed_filters)}")
    print(f"Failed filters: {len(failed_filters)}")

    return {
        "passed_filters": passed_filters,
        "failed_filters": failed_filters
    }
