"""Model class for deploying IME classifier.

This is named "model.py" but the model logic is actually in "classifier.py".
This is to keep up with the naming conventions used for the other integrations,
in which the batch classification logic is encapsulated in the "model.py" file
but not neccessarily the model inference itself.
"""

from typing import Optional

import numpy as np
import pandas as pd

from lib.helper import create_batches, track_performance, RUN_MODE
from lib.log.logger import get_logger
from ml_tooling.ime.constants import default_hyperparameters, default_model
from ml_tooling.ime.helper import get_device
from ml_tooling.ime.inference import (
    load_model_and_tokenizer,
    process_ime_batch,
)
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)
from services.ml_inference.models import ImeLabelModel

logger = get_logger(__file__)
device = get_device()

model, tokenizer = load_model_and_tokenizer(model_name=default_model, device=device)

# gets around errors related to importing cometml in tests.
if RUN_MODE == "test":

    def log_batch_classification_to_cometml(service="ml_inference_ime"):
        def decorator(func):
            return func

        return decorator
else:
    from lib.telemetry.cometml import log_batch_classification_to_cometml


def create_labels(posts: list[dict], output_df: pd.DataFrame) -> list[dict]:
    """Create label models from posts and responses.

    Takes a list of posts and a DataFrame of processed results, and creates label models
    for each post. For posts that were successfully processed (their URIs exist in the
    output DataFrame), creates a label model with the processed results and sets
    was_successfully_labeled=True. For posts that were not processed (either due to
    output_df being empty or their URIs not existing in it), creates a label model with
    just the post metadata and sets was_successfully_labeled=False.

    Args:
        posts (list[dict]): List of post dictionaries containing uri, text, and preprocessing_timestamp
        output_df (pd.DataFrame): DataFrame containing processed results, with uri column

    Returns:
        list[dict]: List of serialized ImeLabelModel objects for each input post
    """
    if output_df.empty:
        return [
            ImeLabelModel(
                uri=post["uri"],
                text=post["text"],
                preprocessing_timestamp=post["preprocessing_timestamp"],
                was_successfully_labeled=False,
                prob_emotion=None,
                prob_intergroup=None,
                prob_moral=None,
                prob_other=None,
                label_emotion=None,
                label_intergroup=None,
                label_moral=None,
                label_other=None,
                label_timestamp=None,
            ).model_dump()
            for post in posts
        ]

    # Create a mapping of URIs to their processed results
    processed_uris = set(output_df["uri"])

    labels = []
    for post in posts:
        if post["uri"] in processed_uris:
            # Post was successfully processed
            post_df = output_df[output_df["uri"] == post["uri"]]
            post_dict = post_df.iloc[0].to_dict()
            labels.append(
                ImeLabelModel(
                    uri=post["uri"],
                    text=post["text"],
                    preprocessing_timestamp=post["preprocessing_timestamp"],
                    was_successfully_labeled=True,
                    prob_emotion=post_dict["prob_emotion"],
                    prob_intergroup=post_dict["prob_intergroup"],
                    prob_moral=post_dict["prob_moral"],
                    prob_other=post_dict["prob_other"],
                    label_emotion=post_dict["label_emotion"],
                    label_intergroup=post_dict["label_intergroup"],
                    label_moral=post_dict["label_moral"],
                    label_other=post_dict["label_other"],
                    label_timestamp=post_dict["label_timestamp"],
                ).model_dump()
            )
        else:
            # Post was not processed
            labels.append(
                ImeLabelModel(
                    uri=post["uri"],
                    text=post["text"],
                    preprocessing_timestamp=post["preprocessing_timestamp"],
                    was_successfully_labeled=False,
                    prob_emotion=None,
                    prob_intergroup=None,
                    prob_moral=None,
                    prob_other=None,
                    label_emotion=None,
                    label_intergroup=None,
                    label_moral=None,
                    label_other=None,
                    label_timestamp=None,
                ).model_dump()
            )

    return labels


@track_performance
def batch_classify_posts(
    posts: list[dict],
    batch_size: int,
    minibatch_size: int,
) -> dict:
    """Run batch classification on the given posts."""
    batches: list[list[dict]] = create_batches(batch_list=posts, batch_size=batch_size)
    total_batches = len(batches)
    total_posts_successfully_labeled = 0
    total_posts_failed_to_label = 0

    experiment_metrics = {
        "average_text_length_per_batch": [],
        "average_prob_emotion_per_batch": [],
        "average_prob_intergroup_per_batch": [],
        "average_prob_moral_per_batch": [],
        "average_prob_other_per_batch": [],
        "prop_emotion_per_batch": [],  # proportion of samples with label=1
        "prop_intergroup_per_batch": [],
        "prop_moral_per_batch": [],
        "prop_other_per_batch": [],
        "labels_emotion": [],  # the actual labels (0/1) of each sample
        "labels_intergroup": [],
        "labels_moral": [],
        "labels_other": [],
        "prop_multi_label_samples_per_batch": [],  # proportion of samples with more than one label
    }

    # If there are no batches, return 0 for all metrics
    if not batches:
        global_experiment_metrics = {
            metric: 0
            for metric in experiment_metrics
            if not metric.startswith("labels_")
        }
        metadata = {
            "total_batches": 0,
            "total_posts_successfully_labeled": 0,
            "total_posts_failed_to_label": 0,
        }
        classification_breakdown = {
            "emotion": {
                "title": "Emotion",
                "description": "",
                "probs": [],
                "labels": [],
            },
            "intergroup": {
                "title": "Intergroup",
                "description": "",
                "probs": [],
                "labels": [],
            },
            "moral": {
                "title": "Moral",
                "description": "",
                "probs": [],
                "labels": [],
            },
            "other": {
                "title": "Other",
                "description": "",
                "probs": [],
                "labels": [],
            },
        }
        return {
            "metadata": metadata,
            "experiment_metrics": global_experiment_metrics,
            "classification_breakdown": classification_breakdown,
        }

    for i, batch in enumerate(batches):
        if i % 10 == 0:
            logger.info(f"Processing batch {i}/{total_batches}")

        output_df: pd.DataFrame = process_ime_batch(
            batch=batch,
            minibatch_size=minibatch_size,
            model=model,
            tokenizer=tokenizer,
        )
        labels: list[dict] = create_labels(posts=batch, output_df=output_df)

        total_failed_labels = 0
        total_successful_labels = 0
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
                inference_type="ime",
                posts=successful_labels,
                batch_size=batch_size,
            )
            total_posts_successfully_labeled += total_successful_labels

        if total_failed_labels > 0:
            logger.error(
                f"Failed to label {total_failed_labels} posts. Re-inserting these into queue."
            )
            return_failed_labels_to_input_queue(
                inference_type="ime",
                failed_label_models=failed_labels,
                batch_size=batch_size,
            )
            total_posts_failed_to_label += total_failed_labels

        # Calculate metrics for this batch

        if output_df.empty:
            logger.warning("No labels found for this batch.")
        else:
            label_cols = [
                "label_emotion",
                "label_intergroup",
                "label_moral",
                "label_other",
            ]

            experiment_metrics["average_text_length_per_batch"].append(
                output_df["text"].str.len().mean()
            )
            experiment_metrics["average_prob_emotion_per_batch"].append(
                output_df["prob_emotion"].mean()
            )
            experiment_metrics["average_prob_intergroup_per_batch"].append(
                output_df["prob_intergroup"].mean()
            )
            experiment_metrics["average_prob_moral_per_batch"].append(
                output_df["prob_moral"].mean()
            )
            experiment_metrics["average_prob_other_per_batch"].append(
                output_df["prob_other"].mean()
            )

            # Calculate percentage of each label type in batch
            experiment_metrics["prop_emotion_per_batch"].append(
                output_df["label_emotion"].mean() * 100
            )
            experiment_metrics["prop_intergroup_per_batch"].append(
                output_df["label_intergroup"].mean() * 100
            )
            experiment_metrics["prop_moral_per_batch"].append(
                output_df["label_moral"].mean() * 100
            )
            experiment_metrics["prop_other_per_batch"].append(
                output_df["label_other"].mean() * 100
            )

            # Calculate proportion of multi-label samples (more than one label)
            label_sums = output_df[label_cols].sum(axis=1)
            multi_label_prop = (label_sums > 1).mean()
            experiment_metrics["prop_multi_label_samples_per_batch"].append(
                multi_label_prop
            )

        del successful_labels
        del failed_labels

    # log metrics. Exclude the labels from the metrics.
    global_experiment_metrics = {
        metric: np.mean(experiment_metrics[metric])
        for metric in experiment_metrics
        if not metric.startswith("labels_")
    }
    metadata = {
        "total_batches": total_batches,
        "total_posts_successfully_labeled": total_posts_successfully_labeled,
        "total_posts_failed_to_label": total_posts_failed_to_label,
    }
    classification_breakdown = {
        "emotion": {
            "title": "Emotion",
            "description": "",
            "probs": experiment_metrics["average_prob_emotion_per_batch"],
            "labels": experiment_metrics["labels_emotion"],
        },
        "intergroup": {
            "title": "Intergroup",
            "description": "",
            "probs": experiment_metrics["average_prob_intergroup_per_batch"],
            "labels": experiment_metrics["labels_intergroup"],
        },
        "moral": {
            "title": "Moral",
            "description": "",
            "probs": experiment_metrics["average_prob_moral_per_batch"],
            "labels": experiment_metrics["labels_moral"],
        },
        "other": {
            "title": "Other",
            "description": "",
            "probs": experiment_metrics["average_prob_other_per_batch"],
            "labels": experiment_metrics["labels_other"],
        },
    }
    return {
        "metadata": metadata,
        "experiment_metrics": global_experiment_metrics,
        "classification_breakdown": classification_breakdown,
    }


@track_performance
@log_batch_classification_to_cometml(service="ml_inference_ime")
def run_batch_classification(
    posts: list[dict], hyperparameters: Optional[dict] = default_hyperparameters
) -> dict:
    """Run batch classification on the given posts and logs to W&B."""
    results: dict = batch_classify_posts(
        posts=posts,
        batch_size=hyperparameters["batch_size"],
        minibatch_size=hyperparameters["minibatch_size"],
    )
    run_metadata: dict = results["metadata"]
    classification_breakdown: dict = results["classification_breakdown"]
    telemetry_metadata = {
        "hyperparameters": hyperparameters,
        "metrics": results["experiment_metrics"],
        "classification_breakdown": classification_breakdown,
    }
    hydrated_metadata = {
        "run_metadata": run_metadata,
        "telemetry_metadata": telemetry_metadata,
    }
    return hydrated_metadata
