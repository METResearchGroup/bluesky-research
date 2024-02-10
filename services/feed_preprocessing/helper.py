"""Helper functions for feed preprocessing."""

class DataLoader:
    """Loads raw data from S3 in batches, then yields them to the lambda
    so that they can be processed incrementally without having to be all
    loaded into memory.
    """

    def __init__(self) -> None:
        pass

    def __iter__(self) -> None:
        pass


    def load_data(self) -> None:
        pass


    def get_next_batch(self) -> None:
        pass


def preprocess_raw_data() -> None:
    pass
