import os

current_file_directory = os.path.dirname(os.path.abspath(__file__))
default_num_classes = 4
default_batch_size = 512
default_minibatch_size = 32
default_model = "distilbert"
# max_num_posts = 40_000 # can run quite a few at a time due to GPU speedups.
max_num_posts = 200_000  # can run quite a few at a time due to GPU speedups.
default_hyperparameters = {
    "model_name": default_model,
    "batch_size": default_batch_size,
    "minibatch_size": default_minibatch_size,
}

model_to_asset_paths_map = {
    "distilbert": {
        "model_name": "distilbert-base-uncased",
        "pretrained_weights_path": os.path.join(
            current_file_directory,
            "models",
            "benchmark_distilbert-base-uncased",
            "distilbert-base-uncased",
            "model",
            "distilbert-base-uncased_multilabel_classifier.pth",
        ),
        "tokenizer": os.path.join(
            current_file_directory,
            "models",
            "benchmark_distilbert-base-uncased",
            "distilbert-base-uncased",
            "tokenizer",
        ),
    },
    "roberta": {
        "model_name": "roberta-base",
        "pretrained_weights_path": os.path.join(
            current_file_directory,
            "models",
            "benchmark_roberta",
            "roberta-base",
            "model",
            "roberta-base_multilabel_classifier.pth",
        ),
        "tokenizer": os.path.join(
            current_file_directory,
            "models",
            "benchmark_roberta",
            "roberta-base",
            "tokenizer",
        ),
    },
}
