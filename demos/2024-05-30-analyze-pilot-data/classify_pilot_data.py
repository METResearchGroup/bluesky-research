"""Classifies data from pilot and stores result in DB.

These classifications will be used in production more generally, so I want to
store them in a database for future reference (so, not just a .csv file for
the pilot, though we also do want a .csv as well).
"""
import ast

import pandas as pd

from lib.constants import current_datetime_str
from lib.helper import create_batches
from pipelines.classify_records.helper import (
    get_post_metadata_for_classification, validate_posts
)
from services.ml_inference.models import (
    PerspectiveApiLabelsModel, RecordClassificationMetadataModel,
    SociopoliticalLabelsModel
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

csv_filename = "pilot_data_2024-05-30-12:11:48.csv"
batch_size = 100 # TODO: change this later.
MIN_TEXT_LENGTH = 8

def load_pilot_data() -> pd.DataFrame:
    df = pd.read_csv(csv_filename)
    return df


# TODO: change the output since I'm doing it differently now and splitting
# the labels and the metadata.
# TODO: I also need to check which posts already have labels and then make
# sure to not duplicate that labeling.
def batch_label_posts(posts: list[dict]) -> list[PerspectiveApiLabelsModel]:
    batch_labels: list[dict] = [] # TODO: get batch labels from concurrent requests
    return [
        PerspectiveApiLabelsModel(
            uri=post["uri"],
            text=post["text"],
            was_successfully_labeled=True,
            label_timestamp=current_datetime_str,
            **batch_label
        )
        for (post, batch_label) in zip(posts, batch_labels)
    ]


def classify_data(
    valid_posts: list[dict],
    invalid_posts: list[dict]
) -> list[PerspectiveApiLabelsModel]:
    """Classify the data using the Perspective API."""
    res: list[PerspectiveApiLabelsModel] = []
    print(f"Classifying {len(valid_posts)} posts via Perspective API...")
    print(f"Defaulting {len(invalid_posts)} posts to failed label...")
    for post in invalid_posts:
        res.append(
            PerspectiveApiLabelsModel(
                uri=post["uri"],
                text=post["text"],
                was_successfully_labeled=False,
                reason="text_too_short",
                label_timestamp=current_datetime_str,
            )
        )
    # TODO: need to batch classify a bunch of posts via Perspective API.
    # since the rate-limiting step is just us waiting for the requests.
    batches: list[list[dict]] = create_batches(valid_posts)
    for batch in batches:
        batch_labels: list[PerspectiveApiLabelsModel] = batch_label_posts(batch)
        res.extend(batch_labels)
    return res


def export_labels(labeled_posts: list[PerspectiveApiLabelsModel]):
    # export to DB
    # export to .csv
    pass


def main():
    df: pd.DataFrame = load_pilot_data()
    preprocessed_posts_dicts = df.to_dict(orient="records")
    transformed_preprocessed_posts_dicts = [
        {
            **post,
            **{
                "author": ast.literal_eval(post["author"]),
                "metadata": ast.literal_eval(post["metadata"]),
                "record": ast.literal_eval(post["record"]),
                "metrics": ast.literal_eval(post["metrics"]) if post["metrics"] else None, # noqa
            }
        }
        for post in preprocessed_posts_dicts
    ]
    filtered_preprocessed_posts: list[FilteredPreprocessedPostModel] = [
        FilteredPreprocessedPostModel(**post)
        for post in transformed_preprocessed_posts_dicts
    ]
    posts_to_classify: list[RecordClassificationMetadataModel] = (
        get_post_metadata_for_classification(filtered_preprocessed_posts)
    )
    valid_posts, invalid_posts = validate_posts(posts_to_classify)

    # classify data.
if __name__ == "__main__":
    main()
