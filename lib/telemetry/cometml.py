"""CometML telemetry."""

import time

import comet_ml
from comet_ml import Experiment
import matplotlib.pyplot as plt
import numpy as np

from lib.helper import COMET_API_KEY, generate_current_datetime_str
from lib.log.logger import get_logger

workspace = "mtorres98"
service_to_project_name_map = {"ml_inference_ime": "IME Classification"}

logger = get_logger(__file__)


def bin_probs(probs: np.ndarray, bins: int = 10) -> tuple[list, list]:
    """Bin probability values into histogram bins.

    Args:
        probs: Array of probability values
        bins: Number of bins to use (default 10)

    Returns:
        Tuple of (bin_edges, bin_counts) where bin_edges are the cutoff values
        and bin_counts are the number of values in each bin
    """
    hist, bin_edges = np.histogram(probs, bins=bins)
    return bin_edges.tolist(), hist.tolist()


# TODO: can remove once I've confirmed that graphs look as expected.
# def plot_binned_probs(bin_edges: list, bin_counts: list, title: str = "Probability Distribution"):
#     """Plot histogram of binned probability values.

#     Args:
#         bin_edges: List of bin cutoff values
#         bin_counts: List of counts per bin
#         title: Plot title
#     """
#     plt.figure(figsize=(10, 6))
#     bars = plt.bar(bin_edges[:-1], bin_counts, width=np.diff(bin_edges), align='edge', edgecolor='black')
#     plt.title(title)
#     plt.xlabel("Probability")
#     plt.ylabel("Count")

#     # Add count labels above each bar
#     for bar in bars:
#         height = bar.get_height()
#         plt.text(bar.get_x() + bar.get_width()/2., height,
#                 f'{int(height)}',
#                 ha='center', va='bottom')

#     return plt.gcf()


def plot_and_log_ime_probs(
    probs_emotion: np.ndarray,
    probs_intergroup: np.ndarray,
    probs_moral: np.ndarray,
    probs_other: np.ndarray,
    experiment: Experiment,
    bins: int = 10,
):
    """Plot histograms of probability distributions for each IME class.

    Args:
        probs_emotion: Array of emotion probabilities
        probs_intergroup: Array of intergroup probabilities
        probs_moral: Array of moral probabilities
        probs_other: Array of other probabilities
        experiment: Comet.ml experiment for logging
        bins: Number of histogram bins (default 10)
    """
    plt.figure(figsize=(10, 8))

    # Plot emotion probabilities
    plt.subplot(2, 2, 1)
    edges, counts = bin_probs(probs_emotion, bins=bins)
    bars = plt.bar(
        edges[:-1], counts, width=np.diff(edges), align="edge", edgecolor="black"
    )
    plt.title("Emotion Probabilities")
    plt.xlabel("Probability - Emotion")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    # Plot intergroup probabilities
    plt.subplot(2, 2, 2)
    edges, counts = bin_probs(probs_intergroup, bins=bins)
    bars = plt.bar(
        edges[:-1], counts, width=np.diff(edges), align="edge", edgecolor="black"
    )
    plt.title("Intergroup Probabilities")
    plt.xlabel("Probability - Intergroup")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    # Plot moral probabilities
    plt.subplot(2, 2, 3)
    edges, counts = bin_probs(probs_moral, bins=bins)
    bars = plt.bar(
        edges[:-1], counts, width=np.diff(edges), align="edge", edgecolor="black"
    )
    plt.title("Moral Probabilities")
    plt.xlabel("Probability - Moral")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    # Plot other probabilities
    plt.subplot(2, 2, 4)
    edges, counts = bin_probs(probs_other, bins=bins)
    bars = plt.bar(
        edges[:-1], counts, width=np.diff(edges), align="edge", edgecolor="black"
    )
    plt.title("Other Probabilities")
    plt.xlabel("Probability - Other")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    plt.tight_layout()
    fig = plt.gcf()
    experiment.log_figure(figure_name="Batch Probabilities", figure=fig)
    plt.close(fig)


def plot_and_log_ime_labels(
    labels_emotion: np.ndarray,
    labels_intergroup: np.ndarray,
    labels_moral: np.ndarray,
    labels_other: np.ndarray,
    experiment: Experiment,
):
    """Plot and log bar charts of label frequencies for IME classification."""
    plt.figure(figsize=(10, 8))

    # Plot emotion labels
    plt.subplot(2, 2, 1)
    unique_emotion, counts_emotion = np.unique(labels_emotion, return_counts=True)
    bars = plt.bar(unique_emotion, counts_emotion, edgecolor="black")
    plt.title("Emotion Labels")
    plt.xlabel("Label - Emotion")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    # Plot intergroup labels
    plt.subplot(2, 2, 2)
    unique_intergroup, counts_intergroup = np.unique(
        labels_intergroup, return_counts=True
    )
    bars = plt.bar(unique_intergroup, counts_intergroup, edgecolor="black")
    plt.title("Intergroup Labels")
    plt.xlabel("Label - Intergroup")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    # Plot moral labels
    plt.subplot(2, 2, 3)
    unique_moral, counts_moral = np.unique(labels_moral, return_counts=True)
    bars = plt.bar(unique_moral, counts_moral, edgecolor="black")
    plt.title("Moral Labels")
    plt.xlabel("Label - Moral")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    # Plot other labels
    plt.subplot(2, 2, 4)
    unique_other, counts_other = np.unique(labels_other, return_counts=True)
    bars = plt.bar(unique_other, counts_other, edgecolor="black")
    plt.title("Other Labels")
    plt.xlabel("Label - Other")
    plt.ylabel("Frequency")
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    plt.tight_layout()
    fig = plt.gcf()
    experiment.log_figure(figure_name="Batch Labels", figure=fig)
    plt.close(fig)


def plot_ime_classifications(classification_breakdown: dict, experiment: Experiment):
    """Plot and log IME classifications to CometML."""
    emotion_probs = classification_breakdown["emotion"]["probs"]
    emotion_labels = classification_breakdown["emotion"]["labels"]

    intergroup_probs = classification_breakdown["intergroup"]["probs"]
    intergroup_labels = classification_breakdown["intergroup"]["labels"]

    moral_probs = classification_breakdown["moral"]["probs"]
    moral_labels = classification_breakdown["moral"]["labels"]

    other_probs = classification_breakdown["other"]["probs"]
    other_labels = classification_breakdown["other"]["labels"]

    plot_and_log_ime_probs(
        probs_emotion=emotion_probs,
        probs_intergroup=intergroup_probs,
        probs_moral=moral_probs,
        probs_other=other_probs,
        experiment=experiment,
        bins=10,
    )

    plot_and_log_ime_labels(
        labels_emotion=emotion_labels,
        labels_intergroup=intergroup_labels,
        labels_moral=moral_labels,
        labels_other=other_labels,
        experiment=experiment,
    )


def log_telemetry_to_cometml(
    experiment: comet_ml.Experiment,
    run_metadata: dict,
    metrics: dict,
    classification_breakdown: dict,
):
    """Log all telemetry to CometML."""
    experiment.log_metrics(metrics)
    experiment.log_parameters(run_metadata)
    plot_ime_classifications(classification_breakdown, experiment)
    logger.info("Logged telemetry to CometML for batch classification.")


def log_batch_classification_to_cometml(service="ml_inference_ime"):
    """Decorator to log batch classification to CometML."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            experiment = Experiment(
                api_key=COMET_API_KEY,
                project_name=service_to_project_name_map[service],
                workspace=workspace,
            )
            hyperparameters = kwargs["hyperparameters"]
            hyperparameters["start_timestamp"] = generate_current_datetime_str()
            experiment.log_parameters(hyperparameters)

            total_start_time = time.time()
            metadata: dict = func(*args, **kwargs)
            total_end_time = time.time()
            total_time = total_end_time - total_start_time

            run_metadata: dict = metadata.pop("run_metadata")
            telemetry_metadata: dict = metadata.pop("telemetry_metadata")

            run_metadata["end_timestamp"] = generate_current_datetime_str()
            run_metadata["total_time_seconds"] = total_time
            run_metadata["total_time_minutes"] = total_time / 60
            logger.info(
                f"Total batch classification run finished in {total_time} seconds."
            )

            log_telemetry_to_cometml(
                experiment=experiment,
                run_metadata=run_metadata,
                metrics=telemetry_metadata["metrics"],
                classification_breakdown=telemetry_metadata["classification_breakdown"],
            )

            return metadata

        return wrapper

    return decorator
