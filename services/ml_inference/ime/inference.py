"""Inference for the IME classification model."""

from datetime import datetime, timedelta, timezone
import time
from typing import Literal, Optional

from comet_ml import Experiment
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch

from lib.constants import current_datetime_str, timestamp_format
from lib.helper import COMET_API_KEY, generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from ml_tooling.ime_classification.model import (
    default_batch_size,
    default_minibatch_size,
    default_to_other,
    get_device,
    load_model_and_tokenizer,
    TextDataset,
)
from services.ml_inference.helper import get_posts_to_classify
from services.ml_inference.ime.export_data import export_results, write_posts_to_cache
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

# max_num_posts = 40_000 # can run quite a few at a time due to GPU speedups.
max_num_posts = 200_000  # can run quite a few at a time due to GPU speedups.

default_model = "distilbert"
model, tokenizer = load_model_and_tokenizer(default_model)
device = get_device()

logger = get_logger(__name__)

project_name = "IME classification inference"
workspace = "mtorres98"
# experiment_name = f"ime_classification_inference_{current_datetime_str}"

columns_to_keep = [
    "uri",
    "text",
    "prob_emotion",
    "prob_intergroup",
    "prob_moral",
    "prob_other",
    "label_emotion",
    "label_intergroup",
    "label_moral",
    "label_other",
    "label_timestamp",
]


def batch_classify_posts(
    df: pd.DataFrame, minibatch_size: int = default_minibatch_size
) -> pd.DataFrame:
    """Batch classifies a list of posts."""
    dataset = TextDataset(
        tokenizer=tokenizer, df=df, mode="test", batch_size=minibatch_size
    )
    total_batches = len(df) // minibatch_size

    all_probabilities: list[float] = []
    all_preds: list[int] = []

    model.eval()

    with torch.no_grad():
        for i, batch in enumerate(dataset):
            logger.info(f"Processing batch {i}/{total_batches}...")
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)

            # probs are of length nx4 (4 classes)
            probs = outputs.cpu().numpy()
            preds = (probs > 0.5).astype(int)
            preds = default_to_other(preds)

            all_probabilities.extend(probs)
            all_preds.extend(preds)

    probabilities = np.array(all_probabilities)
    preds = np.array(all_preds)

    probs_emotion = probabilities[:, 0]
    probs_intergroup = probabilities[:, 1]
    probs_moral = probabilities[:, 2]
    probs_other = probabilities[:, 3]

    labels_emotion = preds[:, 0]
    labels_intergroup = preds[:, 1]
    labels_moral = preds[:, 2]
    labels_other = preds[:, 3]

    df["prob_emotion"] = probs_emotion
    df["prob_intergroup"] = probs_intergroup
    df["prob_moral"] = probs_moral
    df["prob_other"] = probs_other

    df["label_emotion"] = labels_emotion
    df["label_intergroup"] = labels_intergroup
    df["label_moral"] = labels_moral
    df["label_other"] = labels_other
    df["label_timestamp"] = current_datetime_str

    df = df[columns_to_keep]

    return df


def plot_and_logs_probs(
    probs_emotion: np.ndarray,
    probs_intergroup: np.ndarray,
    probs_moral: np.ndarray,
    probs_other: np.ndarray,
    batch_num: int,
    experiment: Experiment,
) -> None:
    plt.figure(figsize=(10, 8))

    plt.subplot(2, 2, 1)
    plt.hist(probs_emotion, bins=50, alpha=0.75)
    plt.title(f"Batch {batch_num} - Emotion Probabilities")
    plt.xlabel("Probability")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.subplot(2, 2, 2)
    plt.hist(probs_intergroup, bins=50, alpha=0.75)
    plt.title(f"Batch {batch_num} - Intergroup Probabilities")
    plt.xlabel("Probability")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.subplot(2, 2, 3)
    plt.hist(probs_moral, bins=50, alpha=0.75)
    plt.title(f"Batch {batch_num} - Moral Probabilities")
    plt.xlabel("Probability")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.subplot(2, 2, 4)
    plt.hist(probs_other, bins=50, alpha=0.75)
    plt.title(f"Batch {batch_num} - Other Probabilities")
    plt.xlabel("Probability")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.tight_layout()
    fig = plt.gcf()
    experiment.log_figure(figure_name=f"Batch {batch_num} - Probabilities", figure=fig)
    plt.close(fig)


def plot_and_log_labels(
    labels_emotion: pd.Series,
    labels_intergroup: pd.Series,
    labels_moral: pd.Series,
    labels_other: pd.Series,
    batch_num: int,
    experiment: Experiment,
) -> None:
    """
    Plot and log bar charts of label frequencies.

    Args:
        labels_emotion (pd.Series): Series of emotion labels.
        labels_intergroup (pd.Series): Series of intergroup labels.
        labels_moral (pd.Series): Series of moral labels.
        labels_other (pd.Series): Series of other labels.
        batch_num (int): Batch number.
        experiment (Experiment): Comet.ml experiment object for logging.
    """
    plt.figure(figsize=(10, 8))

    plt.subplot(2, 2, 1)
    plt.bar(labels_emotion.value_counts().index, labels_emotion.value_counts().values)
    plt.title(f"Batch {batch_num} - Emotion Labels")
    plt.xlabel("Label")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.subplot(2, 2, 2)
    plt.bar(
        labels_intergroup.value_counts().index, labels_intergroup.value_counts().values
    )
    plt.title(f"Batch {batch_num} - Intergroup Labels")
    plt.xlabel("Label")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.subplot(2, 2, 3)
    plt.bar(labels_moral.value_counts().index, labels_moral.value_counts().values)
    plt.title(f"Batch {batch_num} - Moral Labels")
    plt.xlabel("Label")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.subplot(2, 2, 4)
    plt.bar(labels_other.value_counts().index, labels_other.value_counts().values)
    plt.title(f"Batch {batch_num} - Other Labels")
    plt.xlabel("Label")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.tight_layout()
    fig = plt.gcf()
    experiment.log_figure(
        figure_name=f"Batch {batch_num} - Label Frequencies", figure=fig
    )
    plt.close(fig)


def run_batch_classification(
    posts: pd.DataFrame,
    source_feed: Literal["firehose", "most_liked"],
    batch_size: int = default_batch_size,
    minibatch_size: int = default_minibatch_size,
) -> pd.DataFrame:
    """Run batch classification and log results with Wandb.

    Ingests all the posts and classifies them in batches.
    """
    hyperparams = {
        "model_name": default_model,
        "batch_size": batch_size,
        "minibatch_size": minibatch_size,
        "timestamp": current_datetime_str,
        "source_feed": source_feed,
    }

    experiment = Experiment(
        api_key=COMET_API_KEY, project_name=project_name, workspace=workspace
    )
    experiment.log_parameters(hyperparams)

    batched_dfs = [posts[i : i + batch_size] for i in range(0, len(posts), batch_size)]
    total_df_batches = len(batched_dfs)

    batch_times: list[float] = []

    total_start_time = time.time()

    dfs = []

    for i, batched_df in enumerate(batched_dfs):
        batch_start_time = time.time()

        # run inference.
        results_df: pd.DataFrame = batch_classify_posts(
            batched_df, minibatch_size=minibatch_size
        )

        results_df["source"] = source_feed

        # export to local cache.
        write_posts_to_cache(
            posts=results_df,
            source_feed=source_feed,
            classification_type="valid",
        )

        # add logs to Comet.
        probs_emotion = results_df["prob_emotion"]
        probs_intergroup = results_df["prob_intergroup"]
        probs_moral = results_df["prob_moral"]
        probs_other = results_df["prob_other"]

        labels_emotion = results_df["label_emotion"]
        labels_intergroup = results_df["label_intergroup"]
        labels_moral = results_df["label_moral"]
        labels_other = results_df["label_other"]

        plot_and_logs_probs(
            probs_emotion=probs_emotion,
            probs_intergroup=probs_intergroup,
            probs_moral=probs_moral,
            probs_other=probs_other,
            batch_num=i,
            experiment=experiment,
        )
        plot_and_log_labels(
            labels_emotion=labels_emotion,
            labels_intergroup=labels_intergroup,
            labels_moral=labels_moral,
            labels_other=labels_other,
            batch_num=i,
            experiment=experiment,
        )

        batch_end_time = time.time()
        batch_time = batch_end_time - batch_start_time
        batch_times.append(batch_time)
        logger.info(f"Batch {i}/{total_df_batches} finished in {batch_time} seconds.")
        dfs.append(results_df)

    if len(dfs) > 0:
        export_df = pd.concat(dfs)
    else:
        export_df = pd.DataFrame(columns=columns_to_keep)
        export_df["source"] = None
    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    logger.info(f"Total batch run finished in {total_time} seconds.")

    experiment.log_metrics(
        {
            "total_inference_time": sum(batch_times),
            "average_batch_time": np.mean(batch_times),
            "total_posts": len(posts),
        }
    )

    return export_df


@track_performance
def classify_latest_posts(
    backfill_period: Optional[str] = None,
    backfill_duration: Optional[int] = None,
    run_classification: bool = True,
) -> dict:
    """Classifies the latest preprocessed posts using the IME classifier."""
    if run_classification:
        if backfill_duration is not None and backfill_period in ["days", "hours"]:
            current_time = datetime.now(timezone.utc)
            if backfill_period == "days":
                backfill_time = current_time - timedelta(days=backfill_duration)
                logger.info(f"Backfilling {backfill_duration} days of data.")
            elif backfill_period == "hours":
                backfill_time = current_time - timedelta(hours=backfill_duration)
                logger.info(f"Backfilling {backfill_duration} hours of data.")
        else:
            backfill_time = None
        if backfill_time is not None:
            backfill_timestamp = backfill_time.strftime(timestamp_format)
            timestamp = backfill_timestamp
        else:
            timestamp = None
        posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(  # noqa
            inference_type="ime", timestamp=timestamp
        )
        logger.info(
            f"Classifying {len(posts_to_classify)} posts with IME classifier..."
        )
        if len(posts_to_classify) == 0:
            logger.info("No posts to classify. Exiting...")
            return {
                "inference_type": "ime",
                "inference_timestamp": generate_current_datetime_str(),
                "total_classified_posts": 0,
                "total_classified_posts_by_source": {
                    "firehose": 0,
                    "most_liked": 0,
                },
            }
        firehose_posts = [
            post for post in posts_to_classify if post.source == "firehose"
        ]
        most_liked_posts = [
            post for post in posts_to_classify if post.source == "most_liked"
        ]
        firehose_posts_df = pd.DataFrame([post.dict() for post in firehose_posts])
        most_liked_posts_df = pd.DataFrame([post.dict() for post in most_liked_posts])

        source_to_posts_tuples = [
            ("firehose", firehose_posts_df),
            ("most_liked", most_liked_posts_df),
        ]

        for source, posts_df in source_to_posts_tuples:
            print(f"For source {source}, there are {len(posts_df)} posts.")
            posts_df = posts_df[:max_num_posts]
            print(f"Truncating to {len(posts_df)} posts.")
            run_batch_classification(posts=posts_df, source_feed=source)
    else:
        logger.info("Skipping classification and exporting cached results...")
    timestamp = generate_current_datetime_str()
    results = export_results()
    labeling_session = {
        "inference_type": "ime",
        "inference_timestamp": timestamp,
        "total_classified_posts": results["total_classified_posts"],
        "total_classified_posts_by_source": results["total_classified_posts_by_source"],  # noqa
    }
    return labeling_session


if __name__ == "__main__":
    texts = [
        {"text": "I can't believe this is happening! #outrageous"},
        {"text": "This is absolutely unacceptable!"},
        {"text": "How can they allow this to happen?"},
        {"text": "I'm so angry right now!"},
    ] * 1024
    df = pd.DataFrame(texts)
    results_df = run_batch_classification(df, source_feed="firehose")
