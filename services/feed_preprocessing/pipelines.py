"""Feed preprocessing pipeline."""
from typing import Optional

def example_preprocessing_function(post: dict) -> dict:
    """Example preprocessing function."""
    return post


def example_filtering_function(post: dict) -> bool:
    """Example filtering function.

    Returns the post if it passes filtering. Otherwise returns None.
    """
    if not post:
        return False
    return True


filtering_pipeline: list[callable] = [example_filtering_function]
feed_preprocessing_pipeline: list[callable] = [example_preprocessing_function]
