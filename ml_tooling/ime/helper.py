"""Helper functions for the IME model."""

def get_device():
    # Import torch lazily so importing IME modules doesn't require the heavy ML
    # dependency stack unless/until inference is actually invoked.
    try:
        import torch  # type: ignore
    except ModuleNotFoundError as e:  # pragma: no cover
        raise ModuleNotFoundError(
            "IME requires the optional 'ml' dependencies. Install with "
            "`pip install .[ml]` (or `bluesky-research[ml]`) to enable IME inference."
        ) from e

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
