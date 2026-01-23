"""Experimenting with prompt batch size.

We see that there is an inverse trade-off between the prompt batch size and the
quality of performance, especially as it relates to the current concurrent
implementation.

Let's say I have 20 posts:

- In the current approach, we run all 20 posts as 20 requests in parallel.
Then we have to wait for all the 20 requests to finish. After that, we process
them all in one go and then do the next batch. You can imagine that there are
some pain points that would seem to come up. As an example, if you run more
requests in parallel, you run into more edge-case tail latencies. You can also
imagine that there would be, in theory, more errors.
- I had assumed that if we had, instead of doing 20 requests in parallel, that
we can do one request with 20 posts inside of it.

I thought that that could be faster, but now that I think about it, it actually
makes sense why it's not faster. Here are some reasons:

- The runtime and the I/O for the large prompts is going to be larger than any
individual single prompt of the original implementation because there's going
to be more tokens that have to be processed. Therefore, the network latency for
one of the big prompt batch requests should, on average, take longer than the
20 concurrent parallel requests, each of which has only one post.
- Even though we do have 20 requests in parallel, they are kicked up at the
same time. So excluding and barring tail latencies, you generally can assume
that the average completion time for all of the requests would be about the
same time as it would take to complete one request, or the slowest request.
It seems like consistently the slowest request for the scenario of one post in
the query that the tail latency of 20 requests, the slowest request is still
more quick on average than a single large batch prompt. The tail latencies that
come from the network seem to still be quicker than the increase in latency that
you get from the increase in tokens from batching the post into a single prompt.

In this script, our plan is to try a variety of possible batch sizes 'n'. We'll be
comparing two possible approaches:
1. (The current approach): Run 'n' requests in parallel, where 'n' is the number of posts in the batch.
2. (The new approach): Run one request with 'n' posts inside of it.

Right now, our goal is to pin down at what point the tradeoff becomes more
apparent, where the performance of one prompt with n posts inside of it begins
to degrade as compared to having 'n' posts run as 'n' requests in parallel.

I'd like, if possible, to be able to find the sweet spot for the prompt
batch size, where a single request with 'n' posts inside of it performs on
par with having 'n' posts run as 'n' requests in parallel, both from a runtime
and an accuracy perspective.
"""

import json
import os
import time

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
    "prompt_batch_size_experiment_results",
    generate_current_datetime_str(),
)

class ClassifierResultsModel(BaseModel):
    accuracy: float
    runtime_seconds: float

class ExperimentResultsModel(BaseModel):
    batch_size: int
    current_classifier_results: ClassifierResultsModel
    prompt_batched_classifier_results: ClassifierResultsModel

BATCH_SIZES = [1, 2, 3, 5, 8, 10, 12, 15, 20, 25, 30]

current_classifier = IntergroupClassifier()
prompt_batched_classifier = IntergroupBatchedClassifier()


def run_current_classifier_approach(
    batch: list[PostToLabelModel],
    ground_truth_labels: list[int],
) -> ClassifierResultsModel:
    start_time = time.time()
    current_labels: list[IntergroupLabelModel] = current_classifier.classify_batch(batch=batch)
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
) -> ClassifierResultsModel:
    start_time = time.time()
    prompt_batched_labels: list[IntergroupLabelModel] = prompt_batched_classifier.classify_batch(batch=batch)
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

def run_experiment_for_single_batch_size(batch_size: int) -> ExperimentResultsModel:
    df: pd.DataFrame = load_posts()
    batch, ground_truth_labels = create_batch(posts=df, batch_size=batch_size)

    current_classifier_results = run_current_classifier_approach(
        batch=batch,
        ground_truth_labels=ground_truth_labels,
    )
    prompt_batched_classifier_results = run_new_classifier_approach(
        batch=batch,
        ground_truth_labels=ground_truth_labels,
    )
    return ExperimentResultsModel(
        batch_size=batch_size,
        current_classifier_results=current_classifier_results,
        prompt_batched_classifier_results=prompt_batched_classifier_results,
    )

def export_metrics(total_metrics: list[ExperimentResultsModel], export_dir: str):
    os.makedirs(export_dir, exist_ok=True)
    print(f"Exporting metrics to {export_dir}")
    dumped_metrics = [metric.model_dump() for metric in total_metrics]
    export_path = os.path.join(export_dir, "metrics.json")
    with open(export_path, "w") as f:
        json.dump(dumped_metrics, f)
    print(f"Metrics exported to {export_path}")

def _generate_runtime_vs_batch_size_plot(total_metrics: list[ExperimentResultsModel], export_dir: str):
    df = pd.DataFrame([metric.model_dump() for metric in total_metrics])
    batch_sizes = df["batch_size"]
    current_runtimes = df["current_classifier_results"].apply(lambda x: x["runtime_seconds"])
    prompt_batched_runtimes = df["prompt_batched_classifier_results"].apply(lambda x: x["runtime_seconds"])
    
    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, current_runtimes, label="Current", linestyle="--", color="blue")
    plt.plot(batch_sizes, prompt_batched_runtimes, label="Prompt Batched", linestyle="-", color="blue")
    plt.xlabel("Batch Size")
    plt.ylabel("Runtime (seconds)")
    plt.title("Runtime vs. Batch Size")
    plt.legend()
    plt.savefig(os.path.join(export_dir, "runtime_vs_batch_size.png"))
    plt.close()

def _generate_accuracy_vs_batch_size_plot(total_metrics: list[ExperimentResultsModel], export_dir: str):
    df = pd.DataFrame([metric.model_dump() for metric in total_metrics])
    batch_sizes = df["batch_size"]
    current_accuracy = df["current_classifier_results"].apply(lambda x: x["accuracy"])
    prompt_batched_accuracy = df["prompt_batched_classifier_results"].apply(lambda x: x["accuracy"])

    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, current_accuracy, label="Current", linestyle="--", color="blue")
    plt.plot(batch_sizes, prompt_batched_accuracy, label="Prompt Batched", linestyle="-", color="blue")
    plt.xlabel("Batch Size")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs. Batch Size")
    plt.legend()
    plt.savefig(os.path.join(export_dir, "accuracy_vs_batch_size.png"))
    plt.close()

def generate_visualizations(total_metrics: list[ExperimentResultsModel], export_dir: str):
    os.makedirs(export_dir, exist_ok=True)
    print(f"Generating visualizations to {export_dir}")
    _generate_runtime_vs_batch_size_plot(total_metrics=total_metrics, export_dir=export_dir)
    _generate_accuracy_vs_batch_size_plot(total_metrics=total_metrics, export_dir=export_dir)
    print(f"Visualizations generated and exported to {export_dir}")

def run_experiment():
    total_metrics: list[ExperimentResultsModel] = []
    for batch_size in BATCH_SIZES:
        metrics = run_experiment_for_single_batch_size(batch_size)
        total_metrics.append(metrics)
    export_metrics(total_metrics=total_metrics, export_dir=export_dir)
    generate_visualizations(total_metrics=total_metrics, export_dir=export_dir)
