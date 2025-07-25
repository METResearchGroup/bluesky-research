"""Use the Vader classifer to classify valence.

As per the Github page: https://github.com/cjhutto/vaderSentiment?tab=readme-ov-file#about-the-scoring
positive sentiment: compound score >= 0.05
neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
negative sentiment: compound score <= -0.05
"""

from typing import Any

import pandas as pd

from lib.helper import create_batches, generate_current_datetime_str
from lib.log.logger import get_logger
from ml_tooling.valence_classifier.inference import run_vader_on_posts
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)
from services.ml_inference.models import ValenceClassifierLabelModel

logger = get_logger(__file__)


def create_labels(posts: list[dict], output_df: pd.DataFrame) -> list[dict]:
    """
    Create label dicts from posts and VADER output DataFrame.

    Args:
        posts (list[dict]): list of input post dicts.
        output_df (pd.DataFrame): DataFrame with VADER results.

    Returns:
        list[dict]: list of label dicts for each post.
    """
    uri_to_row = {row["uri"]: row for _, row in output_df.iterrows()}
    labels = []
    label_timestamp = generate_current_datetime_str()
    for post in posts:
        uri = post.get("uri")
        row = uri_to_row.get(uri)
        if row is not None:
            labels.append(
                ValenceClassifierLabelModel(
                    uri=uri,
                    text=post.get("text", ""),
                    preprocessing_timestamp=post.get("preprocessing_timestamp", ""),
                    was_successfully_labeled=True,
                    label_timestamp=label_timestamp,
                    valence_label=row["valence_label"],
                    compound=row["compound"],
                ).model_dump()
            )
        else:
            labels.append(
                ValenceClassifierLabelModel(
                    uri=uri,
                    text=post.get("text", ""),
                    preprocessing_timestamp=post.get("preprocessing_timestamp", ""),
                    was_successfully_labeled=False,
                    label_timestamp=label_timestamp,
                    valence_label=None,
                    compound=None,
                ).model_dump()
            )
    return labels


def batch_classify_posts(posts: list[dict], batch_size: int = 100) -> dict[str, Any]:
    """
    Batch classify posts using VADER sentiment analysis.

    Args:
        posts (list[dict]): list of post dicts.
        batch_size (int): Batch size for processing.

    Returns:
        dict[str, Any]: dict with metadata, experiment_metrics, and labels.
    """
    if not posts:
        return {
            "metadata": {
                "total_batches": 0,
                "total_posts_successfully_labeled": 0,
                "total_posts_failed_to_label": 0,
            },
            "experiment_metrics": {},
        }

    batches = create_batches(posts, batch_size)

    total_labels_by_class = {
        "positive": 0,
        "neutral": 0,
        "negative": 0,
    }

    total_successful_labels = 0
    total_failed_labels = 0
    for batch in batches:
        output_df = run_vader_on_posts(batch)
        labels = create_labels(batch, output_df)

        successful_labels: list[dict] = []
        failed_labels: list[dict] = []

        for post, label in zip(batch, labels):
            post_batch_id = post["batch_id"]
            label["batch_id"] = post_batch_id
            if label["was_successfully_labeled"]:
                successful_labels.append(label)
                total_successful_labels += 1
            else:
                failed_labels.append(label)
                total_failed_labels += 1

        # Handle successful and failed labels separately
        if total_successful_labels > 0:
            logger.info(f"Successfully labeled {total_successful_labels} posts.")
            write_posts_to_cache(
                inference_type="valence_classifier",
                posts=successful_labels,
                batch_size=batch_size,
            )

        if total_failed_labels > 0:
            logger.error(
                f"Failed to label {total_failed_labels} posts. Re-inserting these into queue."
            )
            return_failed_labels_to_input_queue(
                inference_type="valence_classifier",
                failed_label_models=failed_labels,
                batch_size=batch_size,
            )

        for label in successful_labels:
            total_labels_by_class[label["valence_label"]] += 1

    metadata = {
        "total_batches": len(batches),
        "total_posts_successfully_labeled": total_successful_labels,
        "total_posts_failed_to_label": total_failed_labels,
    }

    experiment_metrics = {"label_distribution": total_labels_by_class}
    return {"metadata": metadata, "experiment_metrics": experiment_metrics}


def run_batch_classification(
    posts: list[dict], batch_size: int = 100
) -> dict[str, Any]:
    """
    High-level entrypoint for batch valence classification.
    Args:
        posts (list[dict]): list of post dicts.
        batch_size (int): Batch size for processing.
    Returns:
        dict[str, Any]: Output from batch_classify_posts.
    """
    return batch_classify_posts(posts=posts, batch_size=batch_size)
