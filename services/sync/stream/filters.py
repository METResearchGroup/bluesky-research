"""Filters used during streaming."""
from services.transform.transform_raw_data import flatten_firehose_post


# TODO: I should track all URIs, which would simplify determining which posts
# to write/delete. I can load it into memory for this function.
def filter_incoming_posts(operations_by_type: dict) -> list[dict]:
    """Performs filtering on incoming posts and determines which posts have
    to be created or deleted.

    Returns a dictionary of the format:
    {
        "posts_to_create": list[dict],
        "posts_to_delete": list[dict]
    }
    """
    posts_to_create: list[dict] = []
    posts_to_delete: list[str] = [
        p['uri'] for p in operations_by_type['posts']['deleted']
    ]

    for created_post in operations_by_type['posts']['created']:
        if created_post is not None:
            flattened_post = flatten_firehose_post(created_post)
            posts_to_create.append(flattened_post)

    return {
        "posts_to_create": posts_to_create,
        "posts_to_delete": posts_to_delete
    }
