"""Feed preprocessing functions."""
def example_preprocessing_function(post: dict) -> dict:
    """Example preprocessing function."""
    return post


def example_enrichment_function(post: dict) -> dict:
    """Example enrichment function. In contrast to just the preprocessing,
    the enrichment function is expected to return a post with additional
    fields.
    """
    return post
