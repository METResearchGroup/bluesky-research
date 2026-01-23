"""Experimenting with batch size using IntergroupClassifier.

This experiment tests the performance of IntergroupClassifier across different
batch sizes. At each batch size, we run the experiment multiple times and average
the results to get more reliable metrics.

The IntergroupClassifier runs each post as a separate concurrent request, so
the batch size directly corresponds to the number of concurrent requests.
We're interested in understanding:
- How does runtime scale with batch size (concurrent request count)?
- How does accuracy vary with batch size?
- What is the optimal batch size for balancing performance and accuracy?

In this script, we test a variety of batch sizes and run each configuration
multiple times to get averaged results.
"""

import json
import os
import statistics
import time

import pandas as pd
from pydantic import BaseModel
import matplotlib.pyplot as plt

from lib.datetime_utils import generate_current_datetime_str
from classifier import IntergroupClassifier
from load_data import load_posts, create_batch
from metrics import calculate_accuracy
from services.ml_inference.models import PostToLabelModel
from services.ml_inference.intergroup.models import IntergroupLabelModel

export_dir = os.path.join(
    os.path.dirname(__file__),
    "concurrent_request_count_experiment_results",
    generate_current_datetime_str(),
)

class ClassifierResultsModel(BaseModel):
    accuracy: float
    runtime_seconds: float

class ExperimentResultsModel(BaseModel):
    batch_size: int
    classifier_results: ClassifierResultsModel

BATCH_SIZES = [1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]

classifier = IntergroupClassifier()


def run_classifier_approach(
    batch: list[PostToLabelModel],
    ground_truth_labels: list[int],
) -> ClassifierResultsModel:
    start_time = time.time()
    try:
        labels: list[IntergroupLabelModel] = classifier.classify_batch(batch=batch)
    except Exception as e:
        batch_size = len(batch)
        print(f"Error classifying batch for batch size {batch_size}: {e}")
        labels = classifier._generate_failed_labels(batch=batch, reason=str(e))

    accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in labels],
    )
    end_time = time.time()
    runtime_seconds = end_time - start_time
    return ClassifierResultsModel(
        accuracy=accuracy,
        runtime_seconds=runtime_seconds,
    )

def run_experiment_for_single_batch_size(batch_size: int) -> ExperimentResultsModel:
    print(f"Running experiment for batch size {batch_size}")
    df: pd.DataFrame = load_posts()
    batch, ground_truth_labels = create_batch(posts=df, batch_size=batch_size)

    classifier_results = run_classifier_approach(
        batch=batch,
        ground_truth_labels=ground_truth_labels,
    )
    print(f"Experiment for batch size {batch_size} completed")
    return ExperimentResultsModel(
        batch_size=batch_size,
        classifier_results=classifier_results,
    )

def aggregate_experiment_results(results: list[ExperimentResultsModel]) -> ExperimentResultsModel:
    """Aggregate multiple experiment runs into a single averaged result.
    
    Args:
        results: List of experiment results from multiple runs of the same batch size.
                Must be non-empty and all results must have the same batch_size.
    
    Returns:
        A single ExperimentResultsModel with averaged accuracy and runtime values.
    
    Raises:
        ValueError: If results list is empty or batch sizes don't match.
    """
    if not results:
        raise ValueError("Cannot aggregate empty results list")
    
    batch_size: int = results[0].batch_size
    if not all(result.batch_size == batch_size for result in results):
        raise ValueError("All results must have the same batch_size")
    
    accuracies: list[float] = [result.classifier_results.accuracy for result in results]
    runtimes: list[float] = [result.classifier_results.runtime_seconds for result in results]
    
    avg_accuracy: float = statistics.mean(accuracies)
    avg_runtime: float = statistics.mean(runtimes)
    
    return ExperimentResultsModel(
        batch_size=batch_size,
        classifier_results=ClassifierResultsModel(
            accuracy=avg_accuracy,
            runtime_seconds=avg_runtime,
        ),
    )

def export_metrics(total_metrics: list[ExperimentResultsModel], export_dir: str):
    os.makedirs(export_dir, exist_ok=True)
    print(f"Exporting metrics to {export_dir}")
    dumped_metrics = [metric.model_dump() for metric in total_metrics]
    export_path = os.path.join(export_dir, "metrics.json")
    with open(export_path, "w") as f:
        json.dump(dumped_metrics, f)
    print(f"Metrics exported to {export_path}")

def _generate_runtime_vs_batch_size_plot(
    total_metrics: list[ExperimentResultsModel], 
    export_dir: str, 
    n_runs: int
) -> None:
    df = pd.DataFrame([metric.model_dump() for metric in total_metrics])
    batch_sizes = df["batch_size"]
    runtimes = df["classifier_results"].apply(lambda x: x["runtime_seconds"])
    
    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, runtimes, label="IntergroupClassifier", linestyle="-", color="blue")
    plt.xlabel("Batch Size (Concurrent Request Count)")
    plt.ylabel("Runtime (seconds)")
    plt.title(f"Runtime vs. Batch Size (averaged across {n_runs} runs)")
    plt.legend()
    plt.savefig(os.path.join(export_dir, "runtime_vs_batch_size.png"))
    plt.close()

def _generate_accuracy_vs_batch_size_plot(
    total_metrics: list[ExperimentResultsModel], 
    export_dir: str, 
    n_runs: int
) -> None:
    df = pd.DataFrame([metric.model_dump() for metric in total_metrics])
    batch_sizes = df["batch_size"]
    accuracy = df["classifier_results"].apply(lambda x: x["accuracy"])

    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, accuracy, label="IntergroupClassifier", linestyle="-", color="blue")
    plt.xlabel("Batch Size (Concurrent Request Count)")
    plt.ylabel("Accuracy")
    plt.title(f"Accuracy vs. Batch Size (averaged across {n_runs} runs)")
    plt.legend()
    plt.savefig(os.path.join(export_dir, "accuracy_vs_batch_size.png"))
    plt.close()

def generate_visualizations(
    total_metrics: list[ExperimentResultsModel], 
    export_dir: str, 
    n_runs: int
) -> None:
    os.makedirs(export_dir, exist_ok=True)
    print(f"Generating visualizations to {export_dir}")
    _generate_runtime_vs_batch_size_plot(total_metrics=total_metrics, export_dir=export_dir, n_runs=n_runs)
    _generate_accuracy_vs_batch_size_plot(total_metrics=total_metrics, export_dir=export_dir, n_runs=n_runs)
    print(f"Visualizations generated and exported to {export_dir}")

def run_experiment(batch_sizes: list[int], n_runs: int = 3) -> None:
    """Run experiments for multiple batch sizes, with each batch size run n_runs times.
    
    Args:
        batch_sizes: List of batch sizes to test.
        n_runs: Number of times to run each batch size experiment (default: 3).
                Results will be averaged across runs.
    """
    total_metrics: list[ExperimentResultsModel] = []
    for batch_size in batch_sizes:
        print(f"[BATCH SIZE {batch_size}]: Running {n_runs} experiments for batch size {batch_size}")
        results: list[ExperimentResultsModel] = []
        for run_num in range(1, n_runs + 1):
            print(f"  Run {run_num}/{n_runs} for batch size {batch_size}")
            result = run_experiment_for_single_batch_size(batch_size)
            results.append(result)
        print(f"[BATCH SIZE {batch_size}]: Completed {n_runs} runs for batch size {batch_size}, averaged results")

        aggregated_result: ExperimentResultsModel = aggregate_experiment_results(results)
        total_metrics.append(aggregated_result)
        print(f"Completed {n_runs} runs for batch size {batch_size}, averaged results")
    
    export_metrics(total_metrics=total_metrics, export_dir=export_dir)
    generate_visualizations(total_metrics=total_metrics, export_dir=export_dir, n_runs=n_runs)

if __name__ == "__main__":
    batch_sizes = BATCH_SIZES
    run_experiment(batch_sizes=batch_sizes)
