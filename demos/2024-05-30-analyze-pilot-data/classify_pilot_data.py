"""Classifies data from pilot and stores result in DB.

These classifications will be used in production more generally, so I want to
store them in a database for future reference (so, not just a .csv file for
the pilot, though we also do want a .csv as well).
"""
import ast

import pandas as pd

from lib.constants import current_datetime_str
from services.ml_inference.models import PerspectiveApiLabelModel # noqa

csv_filename = "pilot_data_2024-05-30-12:11:48.csv"
batch_size = 100 # TODO: change this later.
MIN_TEXT_LENGTH = 8

def load_pilot_data() -> pd.DataFrame:
    df = pd.read_csv(csv_filename)
    return df


def get_texts_for_classification(df: pd.DataFrame) -> list[dict]:
    """Fetch the text of each post and return a list of dicts with the
    post URIs as well as the text of the post.

    Also returns only the posts that will need to be labeled by the Perspective
    API. All others will be given a default label.
    """
    df_dicts = df.to_dict(orient="records")
    uri_texts_list: list[dict] = [
        {
            "uri": post_dict["uri"],
            "text": ast.literal_eval(post_dict["record"])["text"]
        }
        for post_dict in df_dicts
    ]
    return uri_texts_list


def validate_posts(uri_texts_list: list[dict]) -> tuple[list[dict], list[dict]]:
    """Filter which posts should be sent to the Perspective API or not.

    For now, this'll just be a minimum character count. Mostly used as a simple
    filter for posts that don't have any words or might have 1-2 words.
    """
    valid_posts: list[dict] = []
    invalid_posts: list[dict] = []

    for uri_text_dict in uri_texts_list:
        if len(uri_text_dict["text"]) >= MIN_TEXT_LENGTH:
            valid_posts.append(uri_text_dict)
        else:
            invalid_posts.append(uri_text_dict)
    return (valid_posts, invalid_posts)


def generate_batches(posts: list[dict]) -> list[list[dict]]:
    """Generate batches of posts to be sent to the Perspective API."""
    return [
        posts[i:i + batch_size]
        for i in range(0, len(posts), batch_size)
    ]


def batch_label_posts(posts: list[dict]) -> list[PerspectiveApiLabelModel]:
    batch_labels: list[dict] = [] # TODO: get batch labels from concurrent requests
    return [
        PerspectiveApiLabelModel(
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
) -> list[PerspectiveApiLabelModel]:
    """Classify the data using the Perspective API."""
    res: list[PerspectiveApiLabelModel] = []
    print(f"Classifying {len(valid_posts)} posts via Perspective API...")
    print(f"Defaulting {len(invalid_posts)} posts to failed label...")
    for post in invalid_posts:
        res.append(
            PerspectiveApiLabelModel(
                uri=post["uri"],
                text=post["text"],
                was_successfully_labeled=False,
                reason="text_too_short",
                label_timestamp=current_datetime_str,
            )
        )
    # TODO: need to batch classify a bunch of posts via Perspective API.
    # since the rate-limiting step is just us waiting for the requests.
    batches: list[list[dict]] = generate_batches(valid_posts)
    for batch in batches:
        batch_labels: list[PerspectiveApiLabelModel] = batch_label_posts(batch)
        res.extend(batch_labels)
    return res


def export_labels(labeled_posts: list[PerspectiveApiLabelModel]):
    # export to DB
    # export to .csv
    pass


def main():
    df: pd.DataFrame = load_pilot_data()
    breakpoint()
    texts_to_classify: list[dict] = get_texts_for_classification(df)
    # TODO: see how to fetch the text
    breakpoint()

if __name__ == "__main__":
    main()