"""Classifies a post as having hate speech

For now we'll use a placeholder until we come back and properly implement.

Bluesky already has some means for doing this, e.g., by filtering slurs:
- https://github.com/bluesky-social/atproto/pull/1319/files

We'll need to think about how this interacts with toxicity. We
want to be careful about how we treat toxicity since we will address it
later on in the pipeline.
"""


def classify(post: dict) -> bool:
    """Classifies a post as having hate speech."""
    return False
