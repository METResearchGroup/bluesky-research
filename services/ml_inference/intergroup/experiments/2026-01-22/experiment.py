import os
import time

import pandas as pd

from services.ml_inference.models import PostToLabelModel
from services.ml_inference.intergroup.models import IntergroupLabelModel

from .classifier import IntergroupClassifier, IntergroupBatchedClassifier

serial_classifier = IntergroupClassifier()
batched_classifier = IntergroupBatchedClassifier()

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
labeled_posts_filename = "intergroup_eval_dataset.csv"
labeled_posts_fp = os.path.join(
    parent_dir, "2026-01-10", labeled_posts_filename
)

# load in the ground truth labels and randomly sample posts from the
# ground truth labels.
def load_posts():
    df = pd.read_csv(labeled_posts_fp)
    return df

def create_batch(
    posts: pd.DataFrame,
    batch_size: int,
) -> tuple[list[PostToLabelModel], list[int]]:
    """Creates batch of posts and corresponding ground truth labels.
    
    Returns:
        list[PostToLabelModel]: List of posts.
        list[int]: List of ground truth labels.
    """
    posts = posts.sample(n=batch_size)
    posts_list: list[PostToLabelModel] = []
    ground_truth_labels: list[int] = []
    for post in posts.to_dict(orient="records"):
        posts_list.append(PostToLabelModel(**post))
        ground_truth_labels.append(post["label"])
    return posts_list, ground_truth_labels

def calculate_accuracy(ground_truth_labels: list[int], labels: list[int]) -> float:
    """Calculates accuracy of labels compared to ground truth labels.
    
    Returns:
        float: Accuracy of labels compared to ground truth labels.
    """
    correct = 0
    for ground_truth_label, label in zip(ground_truth_labels, labels):
        if ground_truth_label == label:
            correct += 1
    return correct / len(ground_truth_labels)

def run_experiment():
    """Experimenting between the serial and prompt-batched approaches.

    Some things we're considering:
    - What is the difference in runtime?
    - What is the difference in accuracy?
    
    Some notes:
    - For the batch implementation, we're putting the entire batch into
    a single prompt. We already have two batch parameters, the batch and
    minibatch size, and we use the minibatch size to create the prompt batch.
    So, if there's 20 posts in the minibatch, we'll put all 20 posts into a
    single prompt. This helps us avoid having multiple batch parameters, which
    would increase complexity.

    The batch size used by the intergroup classifier is defined in
    services/ml_inference/intergroup/constants.py (currently defined as 20).

    This is the intergroup-specifiv batch size, which is different from the
    batch size used by the input queue. So, in the input queue, iirc, we have
    1000 posts per input queue batch. We would then split that into batches
    of 20 (as this is the intergroup-specific batch size). We'll take that entire
    batch of 20 and put it into a single prompt, rather than specifying a
    "prompt batch size" parameter.
    """
    df = load_posts()
    batch, ground_truth_labels = create_batch(posts=df, batch_size=10)

    # Experiment 1: Run with serial classifier.
    serial_start_time = time.time()
    serial_labels: list[IntergroupLabelModel] = serial_classifier.classify_batch(batch=batch)
    serial_end_time = time.time()
    serial_runtime = serial_end_time - serial_start_time
    print(f"Serial runtime: {serial_runtime} seconds")
    serial_accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in serial_labels],
    )
    print(f"Serial accuracy: {serial_accuracy}")
    # Experiment 2: Run with batched classifier.
    batched_start_time = time.time()
    batched_labels: list[IntergroupLabelModel] = batched_classifier.classify_batch(batch=batch)
    batched_end_time = time.time()
    batched_runtime = batched_end_time - batched_start_time
    print(f"Batched runtime: {batched_runtime} seconds")
    batched_accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in batched_labels],
    )
    print(f"Batched accuracy: {batched_accuracy}")

    return serial_accuracy, batched_accuracy

if __name__ == "__main__":
    run_experiment()
