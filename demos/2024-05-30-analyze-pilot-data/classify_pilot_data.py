"""Classifies data from pilot and stores result in DB.

These classifications will be used in production more generally, so I want to
store them in a database for future reference (so, not just a .csv file for
the pilot, though we also do want a .csv as well).
"""
import ast
from typing import Literal

import pandas as pd

from lib.constants import current_datetime_str
from lib.db.sql.ml_inference_database import (
    batch_insert_metadata, batch_insert_perspective_api_labels,
    batch_insert_sociopolitical_labels, get_existing_perspective_api_uris,
    get_existing_sociopolitical_uris
)
from lib.helper import track_performance
from ml_tooling.llm.model import (
    run_batch_classification as sociopolitical_run_batch_classification
)
from ml_tooling.perspective_api.model import (
    run_batch_classification as perspective_run_batch_classification
)
from pipelines.classify_records.helper import (
    get_post_metadata_for_classification, validate_posts
)
from services.ml_inference.models import (
    PerspectiveApiLabelsModel, RecordClassificationMetadataModel,
    SociopoliticalLabelsModel
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

csv_filename = "pilot_data_2024-05-30-12:11:48.csv"
MIN_TEXT_LENGTH = 8

def load_pilot_data() -> pd.DataFrame:
    df = pd.read_csv(
        csv_filename,
        dtype={
            'uri': str, 
            'cid': str, 
            'indexed_at': str, 
            'author': str, 
            'metadata': str, 
            'record': str, 
            'metrics': str, 
            'passed_filters': bool, 
            'filtered_at': str, 
            'filtered_by_func': str, 
            'synctimestamp': str, 
            'preprocessing_timestamp': str
        }
    )
    df = df.where(pd.notnull(df), None)
    return df


@track_performance
def main(
    num_samples: int,
    classification_type: Literal["perspective", "sociopolitical"]
):
    df: pd.DataFrame = load_pilot_data()
    preprocessed_posts_dicts = df.to_dict(orient="records")
    print(f"Attempting to process {len(preprocessed_posts_dicts)} pilot posts.") # noqa
    preprocessed_posts_dicts = preprocessed_posts_dicts[:num_samples]
    print(f"Processing {len(preprocessed_posts_dicts)} pilot posts.")
    transformed_preprocessed_posts_dicts = [
        {
            **post,
            **{
                "author": ast.literal_eval(post["author"]),
                "metadata": ast.literal_eval(post["metadata"]),
                "record": ast.literal_eval(post["record"]),
                "metrics": ast.literal_eval(post["metrics"]) if post["metrics"] else None, # noqa
                "filtered_by_func": post["filtered_by_func"] if post["filtered_by_func"] is not None else None,  # Ensure it's None if NaN
                "indexed_at": post["indexed_at"] if post["indexed_at"] is not None else None  # Ensure it's None if NaN
            }
        }
        for post in preprocessed_posts_dicts
    ]
    filtered_preprocessed_posts: list[FilteredPreprocessedPostModel] = [
        FilteredPreprocessedPostModel(**post)
        for post in transformed_preprocessed_posts_dicts
    ]
    if classification_type == "perspective":
        existing_uris: set[str] = get_existing_perspective_api_uris()
        deduped_preprocessed_posts: list[FilteredPreprocessedPostModel] = [
            post for post in filtered_preprocessed_posts
            if post.uri not in existing_uris
        ]
        # sort by synctimestamp ascending so the oldest posts are classified first.
        # if we sort by preprocessing_timestamp, then a bunch of posts that were
        # all preprocessed at the same time will have the same timestamp.
        sorted_posts = sorted(
            deduped_preprocessed_posts,
            key=lambda x: x.synctimestamp,
            reverse=False
        )
        posts_to_classify: list[RecordClassificationMetadataModel] = (
            get_post_metadata_for_classification(sorted_posts)
        )
        batch_insert_metadata(posts_to_classify)

        # validate posts
        valid_posts, invalid_posts = validate_posts(posts_to_classify)
        print(f"Number of valid posts: {len(valid_posts)}")
        print(f"Number of invalid posts: {len(invalid_posts)}")
        print(f"Classifying {len(valid_posts)} posts via Perspective API...")
        print(f"Defaulting {len(invalid_posts)} posts to failed label...")

        # insert invalid posts into DB first, before running Perspective API
        # classification
        invalid_posts_models = []
        for post in invalid_posts:
            invalid_posts_models.append(
                PerspectiveApiLabelsModel(
                    uri=post.uri,
                    text=post.text,
                    was_successfully_labeled=False,
                    reason="text_too_short",
                    label_timestamp=current_datetime_str,
                )
            )
        print(f"Inserting {len(invalid_posts_models)} invalid posts into the DB.")
        batch_insert_perspective_api_labels(invalid_posts_models)
        print(f"Completed inserting {len(invalid_posts_models)} invalid posts into the DB.") # noqa
        # run inference on valid posts
        print(f"Running batch classification on {len(valid_posts)} valid posts.")
        perspective_run_batch_classification(posts=valid_posts)
        print("Completed batch classification.")
    if classification_type == "sociopolitical":
        existing_uris: set[str] = get_existing_sociopolitical_uris()
        deduped_preprocessed_posts: list[FilteredPreprocessedPostModel] = [
            post for post in filtered_preprocessed_posts
            if post.uri not in existing_uris
        ]
        sorted_posts = sorted(
            deduped_preprocessed_posts,
            key=lambda x: x.synctimestamp,
            reverse=False
        )
        posts_to_classify: list[RecordClassificationMetadataModel] = (
            get_post_metadata_for_classification(sorted_posts)
        )
        # these should already be inserted.
        # batch_insert_metadata(posts_to_classify)

        # validate posts
        valid_posts, invalid_posts = validate_posts(posts_to_classify)
        print(f"Number of valid posts: {len(valid_posts)}")
        print(f"Number of invalid posts: {len(invalid_posts)}")
        print(f"Classifying {len(valid_posts)} posts via Perspective API...")
        print(f"Defaulting {len(invalid_posts)} posts to failed label...")

        # insert invalid posts into DB first, before running LLM sociopolitical
        # classification
        invalid_posts_models = []
        for post in invalid_posts:
            invalid_posts_models.append(
                SociopoliticalLabelsModel(
                    uri=post["uri"],
                    text=post["text"],
                    was_successfully_labeled=False,
                    reason="text_too_short",
                    label_timestamp=current_datetime_str,
                )
            )

        print(f"Inserting {len(invalid_posts_models)} invalid posts into the DB.")
        batch_insert_sociopolitical_labels(invalid_posts_models)
        print(f"Completed inserting {len(invalid_posts_models)} invalid posts into the DB.") # noqa

        # run inference on valid posts
        print(f"Running batch classification on {len(valid_posts)} valid posts.")
        sociopolitical_run_batch_classification(posts=valid_posts)
        print("Completed batch classification.")


if __name__ == "__main__":
    num_samples = 500
    main(
        num_samples=num_samples,
        classification_type="sociopolitical"
    )
