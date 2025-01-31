"""Helper functions for the IME model."""

import torch


def get_device():
    if torch.cuda.is_available():
        print("CUDA backend available.")
        device = torch.device("cuda")
    elif torch.backends.mps.is_available() and torch.backends.mps.is_built():
        print("Arm mac GPU available, using GPU.")
        device = torch.device("mps")  # for Arm Macs
    else:
        # print("GPU not available, using CPU")
        # device = torch.device("cpu")
        raise ValueError("GPU not available, using CPU")
    return device
