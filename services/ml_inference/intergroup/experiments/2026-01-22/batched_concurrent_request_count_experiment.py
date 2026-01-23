"""Experimenting with concurrent request count using prompt batching.

Now that we have an idea of the number of posts that we can put into a prompt
before degradation (5), let's now explore the number of requests that we can do
concurrently.

We're interested in seeing, for a fixed number of posts 'n', the tradeoff between:

1. (Our current approach): run N requests concurrently in parallel, where each
   request contains 1 post.
2. (The proposed new approach): run N / 5 requests concurrently, each with a
   prompt of batch size 5, in parallel.

Like before, we'll be comparing the runtime and accuracy of the two approaches.
"""

import json
import os
import statistics
import time
import math

import pandas as pd
from pydantic import BaseModel
import matplotlib.pyplot as plt

from lib.datetime_utils import generate_current_datetime_str
from classifier import IntergroupClassifier, IntergroupBatchedClassifier
from load_data import load_posts, create_batch
from metrics import calculate_accuracy
from services.ml_inference.models import PostToLabelModel
from services.ml_inference.intergroup.models import IntergroupLabelModel

export_dir = os.path.join(
    os.path.dirname(__file__),
    "batched_concurrent_request_count_experiment_results",
    generate_current_datetime_str(),
)

class ClassifierResultsModel(BaseModel):
    accuracy: float
    runtime_seconds: float

class ExperimentResultsModel(BaseModel):
    batch_size: int
    concurrent_request_count: int
    current_classifier_results: ClassifierResultsModel
    prompt_batched_classifier_results: ClassifierResultsModel

BATCH_SIZES = [1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]

current_classifier = IntergroupClassifier()
prompt_batched_classifier = IntergroupBatchedClassifier()


def calculate_concurrent_request_count(batch_size: int, prompt_batch_size: int) -> int:
    """Calculate concurrent request count based on batch size.
    
    For a given batch_size, we want to run batch_size / prompt_batch_size
    concurrent requests, each containing prompt_batch_size posts.
    
    Args:
        batch_size: Total number of posts in the batch
        prompt_batch_size: Number of posts per prompt
        
    Returns:
        Number of concurrent requests to make (rounded up to ensure all posts are covered)
    """
    return max(1, math.ceil(batch_size / prompt_batch_size))


def run_current_classifier_approach(
    batch: list[PostToLabelModel],
    ground_truth_labels: list[int],
) -> ClassifierResultsModel:
    """Run the current classifier approach (one request per post, all concurrent)."""
    start_time = time.time()
    try:
        current_labels: list[IntergroupLabelModel] = current_classifier.classify_batch(batch=batch)
    except Exception as e:
        batch_size = len(batch)
        print(f"Error classifying batch for batch size {batch_size}: {e}")
        current_labels = current_classifier._generate_failed_labels(batch=batch, reason=str(e))

    current_accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in current_labels],
    )
    end_time = time.time()
    runtime_seconds = end_time - start_time
    return ClassifierResultsModel(
        accuracy=current_accuracy,
        runtime_seconds=runtime_seconds,
    )


def run_new_classifier_approach(
    batch: list[PostToLabelModel],
    ground_truth_labels: list[int],
    concurrent_request_count: int,
    prompt_batch_size: int,
) -> ClassifierResultsModel:
    """Run the new classifier approach (prompt batching with concurrent requests)."""
    start_time = time.time()
    try:
        prompt_batched_labels: list[IntergroupLabelModel] = (
            prompt_batched_classifier.classify_batch_with_prompt_batching(
                batch=batch,
                concurrent_request_count=concurrent_request_count,
                prompt_batch_size=prompt_batch_size,
            )
        )
    except Exception as e:
        batch_size = len(batch)
        print(f"Error classifying batch for batch size {batch_size}: {e}")
        prompt_batched_labels = prompt_batched_classifier._generate_failed_labels(
            batch=batch, reason=str(e)
        )

    prompt_batched_accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in prompt_batched_labels],
    )
    end_time = time.time()
    runtime_seconds = end_time - start_time
    return ClassifierResultsModel(
        accuracy=prompt_batched_accuracy,
        runtime_seconds=runtime_seconds,
    )


def run_experiment_for_single_batch_size(batch_size: int, prompt_batch_size: int) -> ExperimentResultsModel:
    print(f"Running experiment for batch size {batch_size}")
    df: pd.DataFrame = load_posts()
    batch, ground_truth_labels = create_batch(posts=df, batch_size=batch_size)
    
    # Classifier 1: Run the normal classifier approach
    # (one request per post, all concurrent)
    current_classifier_results = run_current_classifier_approach(
        batch=batch,
        ground_truth_labels=ground_truth_labels,
    )

    # Classifier 2: Run the new classifier approach
    # (prompt batching with concurrent requests)
    concurrent_request_count = calculate_concurrent_request_count(batch_size=batch_size, prompt_batch_size=prompt_batch_size)
    prompt_batched_classifier_results = run_new_classifier_approach(
        batch=batch,
        ground_truth_labels=ground_truth_labels,
        concurrent_request_count=concurrent_request_count,
        prompt_batch_size=prompt_batch_size,
    )
    print(f"Experiment for batch size {batch_size} completed")
    return ExperimentResultsModel(
        batch_size=batch_size,
        concurrent_request_count=concurrent_request_count,
        current_classifier_results=current_classifier_results,
        prompt_batched_classifier_results=prompt_batched_classifier_results,
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
    concurrent_request_count: int = results[0].concurrent_request_count
    if not all(result.batch_size == batch_size for result in results):
        raise ValueError("All results must have the same batch_size")
    if not all(result.concurrent_request_count == concurrent_request_count for result in results):
        raise ValueError("All results must have the same concurrent_request_count")
    
    current_accuracies: list[float] = [result.current_classifier_results.accuracy for result in results]
    current_runtimes: list[float] = [result.current_classifier_results.runtime_seconds for result in results]
    prompt_batched_accuracies: list[float] = [result.prompt_batched_classifier_results.accuracy for result in results]
    prompt_batched_runtimes: list[float] = [result.prompt_batched_classifier_results.runtime_seconds for result in results]
    
    avg_current_accuracy: float = statistics.mean(current_accuracies)
    avg_current_runtime: float = statistics.mean(current_runtimes)
    avg_prompt_batched_accuracy: float = statistics.mean(prompt_batched_accuracies)
    avg_prompt_batched_runtime: float = statistics.mean(prompt_batched_runtimes)

    return ExperimentResultsModel(
        batch_size=batch_size,
        concurrent_request_count=concurrent_request_count,
        current_classifier_results=ClassifierResultsModel(
            accuracy=avg_current_accuracy,
            runtime_seconds=avg_current_runtime,
        ),
        prompt_batched_classifier_results=ClassifierResultsModel(
            accuracy=avg_prompt_batched_accuracy,
            runtime_seconds=avg_prompt_batched_runtime,
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
    current_runtimes = df["current_classifier_results"].apply(lambda x: x["runtime_seconds"])
    prompt_batched_runtimes = df["prompt_batched_classifier_results"].apply(lambda x: x["runtime_seconds"])
    
    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, current_runtimes, label="Current", linestyle="--", color="blue")
    plt.plot(batch_sizes, prompt_batched_runtimes, label="Prompt Batched", linestyle="-", color="blue")
    plt.xlabel("Batch Size")
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
    current_accuracy = df["current_classifier_results"].apply(lambda x: x["accuracy"])
    prompt_batched_accuracy = df["prompt_batched_classifier_results"].apply(lambda x: x["accuracy"])

    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, current_accuracy, label="Current", linestyle="--", color="blue")
    plt.plot(batch_sizes, prompt_batched_accuracy, label="Prompt Batched", linestyle="-", color="blue")
    plt.xlabel("Batch Size")
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


def run_experiment(batch_sizes: list[int], prompt_batch_size: int, n_runs: int = 3) -> None:
    """Run experiments for multiple batch sizes, with each batch size run n_runs times.
    
    Args:
        batch_sizes: List of batch sizes to test.
        prompt_batch_size: Number of posts per prompt.
        n_runs: Number of times to run each batch size experiment (default: 3).
                Results will be averaged across runs.
    """
    total_metrics: list[ExperimentResultsModel] = []
    for batch_size in batch_sizes:
        print(f"[BATCH SIZE {batch_size}]: Running {n_runs} experiments for batch size {batch_size}")
        results: list[ExperimentResultsModel] = []
        for run_num in range(1, n_runs + 1):
            print(f"  Run {run_num}/{n_runs} for batch size {batch_size}")
            result = run_experiment_for_single_batch_size(batch_size, prompt_batch_size)
            results.append(result)
        print(f"[BATCH SIZE {batch_size}]: Completed {n_runs} runs for batch size {batch_size}, averaged results")

        aggregated_result: ExperimentResultsModel = aggregate_experiment_results(results)
        total_metrics.append(aggregated_result)
        print(f"Completed {n_runs} runs for batch size {batch_size}, averaged results")
    
    export_metrics(total_metrics=total_metrics, export_dir=export_dir)
    generate_visualizations(total_metrics=total_metrics, export_dir=export_dir, n_runs=n_runs)


if __name__ == "__main__":
    batch_sizes = BATCH_SIZES[:4]
    prompt_batch_size = 5
    run_experiment(
        batch_sizes=batch_sizes,
        prompt_batch_size=prompt_batch_size,
    )
