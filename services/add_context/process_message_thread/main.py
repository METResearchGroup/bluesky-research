"""Processes any context related to the thread that a post is in."""

def process_parent_post(post: dict) -> None:
    """If a post is in response to another post, hydrate that parent post
    and add its text as context.
    
    NOTE: later on, we can add more context to the parent post, but as a start
    we can start with just the text of the parent post.
    """
    pass


def process_message_thread() -> None:
    pass
