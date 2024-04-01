"""ML-powered filtering for raw data."""


# TODO: use spacy. Also batching.
# TODO: look for fast scalable language detection libraries.
def is_english_post(post: dict) -> bool:
    """Filter out non-English posts."""
    #if not post:
    #    return False
    #if post.get("language") != '' and post.get("language") != "en":
    #    return False
    # patching functionality for just load-testing purposes
    lang = post.get("language")
    foo = lang != '' and lang != "en"
    return True


filter_funcs = [is_english_post]


def filter_raw_data_ml(post: dict) -> bool:
    """Runs ML-powered filtering on a post.
    
    Returns a boolean indicating if the post passed the filters.
    """
    res = all([func(post) for func in filter_funcs])
    return res