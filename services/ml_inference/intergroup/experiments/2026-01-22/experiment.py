"""Initial experiment comparing the serial and prompt-batched approaches.

From trying this, we discovered a need to do 2 separate experiments:
- Experimenting with prompt batch size (see `prompt_batch_size_experiment.py`)
- Experimenting with concurrent request count (see `concurrent_request_count_experiment.py`)
"""

from services.ml_inference.intergroup.constants import DEFAULT_BATCH_SIZE
from services.ml_inference.intergroup.models import IntergroupLabelModel

from classifier import IntergroupClassifier, IntergroupBatchedClassifier
from load_data import load_posts, create_batch
from metrics import calculate_accuracy

serial_classifier = IntergroupClassifier()
batched_classifier = IntergroupBatchedClassifier()

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
    batch, ground_truth_labels = create_batch(posts=df, batch_size=DEFAULT_BATCH_SIZE)

    # Experiment 1: Run with serial classifier.
    serial_labels: list[IntergroupLabelModel] = serial_classifier.classify_batch(batch=batch)
    serial_accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in serial_labels],
    )
    print(f"Serial accuracy: {serial_accuracy}")

    # Experiment 2: Run with batched classifier.
    batched_labels: list[IntergroupLabelModel] = batched_classifier.classify_batch(batch=batch)
    batched_accuracy = calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[label.label for label in batched_labels],
    )
    print(f"Batched accuracy: {batched_accuracy}")

if __name__ == "__main__":
    run_experiment()
