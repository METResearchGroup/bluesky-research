from .classifier import IntergroupClassifier, IntergroupBatchedClassifier

serial_classifier = IntergroupClassifier()
batched_classifier = IntergroupBatchedClassifier()

prompt_batch_size = 10

# load in the ground truth labels and randomly sample posts from the
# ground truth labels.
def load_posts():
    pass

# NOTE: I can also use whatever the minibatch size is as the prompt batch size.
def run_experiment():
    pass

# TODO: also experiment with the number of posts that can be stuffed
# into a single prompt. I had said 10, but can we do 20?

if __name__ == "__main__":
    run_experiment()
