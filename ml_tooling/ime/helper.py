"""Helper functions for the IME model."""

import torch
from transformers import AutoTokenizer

from ml_tooling.ime.classes import MultiLabelClassifier
from ml_tooling.ime.constants import default_num_classes, model_to_asset_paths_map


def get_device():
    if torch.cuda.is_available():
        print("CUDA backend available.")
        device = torch.device("cuda")
    elif torch.backends.mps.is_available() and torch.backends.mps.is_built():
        print("Arm mac GPU available, using GPU.")
        device = torch.device("mps")  # for Arm Macs
    else:
        print("GPU not available, using CPU")
        device = torch.device("cpu")
        # raise ValueError("GPU not available, using CPU")
    return device


def load_model_and_tokenizer(
    model_name: str, device: torch.device
) -> tuple[MultiLabelClassifier, AutoTokenizer]:
    """Load the model and tokenizer for the given model name, from
    local storage."""
    model = MultiLabelClassifier(n_classes=default_num_classes, model=model_name)
    model.to(device)
    model.eval()

    tokenizer_path = model_to_asset_paths_map[model_name]["tokenizer"]
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

    return model, tokenizer
