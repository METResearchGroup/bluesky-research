"""Non-ML filters for raw data."""
from lib.constants import BLOCKED_AUTHORS, INAPPROPRIATE_TERMS


def has_no_explicit_terms(post: dict) -> bool:
    """Determines if a post has inappropriate content.
    
    Looks at both the text and the labels.
    """
    text = post["text"]
    labels = post["labels"]

    for term in INAPPROPRIATE_TERMS:
        if labels and term in labels:
            return False
        if term in text:
            return False
    return True


def author_is_not_blocked(post: dict) -> bool:
    """Determines if an author is blocked."""
    return post["author"] not in BLOCKED_AUTHORS


filter_funcs = [has_no_explicit_terms, author_is_not_blocked]


def filter_raw_data_non_ml(post: dict) -> bool:
    """Runs non-ML-powered filtering on a post.
    
    Returns a boolean indicating if the post passed the filters.
    """
    res = all([func(post) for func in filter_funcs])
    return res