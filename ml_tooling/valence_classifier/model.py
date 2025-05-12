"""Use the Vader classifer to classify valence.

As per the Github page: https://github.com/cjhutto/vaderSentiment?tab=readme-ov-file#about-the-scoring
positive sentiment: compound score >= 0.05
neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
negative sentiment: compound score <= -0.05
"""

from typing import Any

import pandas as pd

from lib.helper import create_batches
from ml_tooling.valence_classifier.inference import run_vader_on_posts
from ml_tooling.valence_classifier.constants import VALENCE_LABELS


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
    for post in posts:
        uri = post.get("uri")
        row = uri_to_row.get(uri)
        if row is not None:
            labels.append(
                {
                    "uri": uri,
                    "text": post.get("text", ""),
                    "valence_label": row["valence_label"],
                    "compound": row["compound"],
                    "was_successfully_labeled": True,
                }
            )
        else:
            labels.append(
                {
                    "uri": uri,
                    "text": post.get("text", ""),
                    "valence_label": None,
                    "compound": None,
                    "was_successfully_labeled": False,
                }
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
            "labels": [],
        }

    batches = create_batches(posts, batch_size)
    all_labels = []
    total_success = 0
    total_failed = 0
    for batch in batches:
        output_df = run_vader_on_posts(batch)
        labels = create_labels(batch, output_df)
        all_labels.extend(labels)
        total_success += sum(
            result_label["was_successfully_labeled"] for result_label in labels
        )
        total_failed += sum(
            not result_label["was_successfully_labeled"] for result_label in labels
        )
    metadata = {
        "total_batches": len(batches),
        "total_posts_successfully_labeled": total_success,
        "total_posts_failed_to_label": total_failed,
    }
    experiment_metrics = {
        "label_distribution": {
            label: sum(
                result_label["valence_label"] == label for result_label in all_labels
            )
            for label in VALENCE_LABELS
        }
    }
    return {
        "metadata": metadata,
        "experiment_metrics": experiment_metrics,
        "labels": all_labels,
    }


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
