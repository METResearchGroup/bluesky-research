from ml_tooling.perspective_api.model import run_batch_classification
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


def load_latest_label_timestamp() -> str:
    # need to think about what this would look like
    # also I should see if in the preprocessing pipeline if this is implemented
    # in the database file or in the helper file.
    pass


def load_latest_filtered_posts() -> list[FilteredPreprocessedPostModel]:
    pass


def classify_latest_posts():
    # Load posts
    posts: list[FilteredPreprocessedPostModel] = load_latest_filtered_posts()
    # TODO: get metadata and export to db
    # classify
    run_batch_classification(posts=posts)
