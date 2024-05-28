"""Create Bluesky classes from dict.

Based on https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/post.py#L22
"""  # noqa
from atproto_client.models.app.bsky.feed.post import Entity, Record, TextSlice


def create_record(post_dict: dict) -> Record:
    pass


def create_text_slice(text_slice_dict: dict) -> TextSlice:
    return TextSlice(
        start=text_slice_dict["start"],
        end=text_slice_dict["end"]
    )


def create_entity(entity_dict: dict) -> Entity:
    """Based on https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/post.py#L29C1-L38C6"""  # noqa
    return Entity(
        index=create_text_slice(entity_dict["index"]),
        type=entity_dict["type"],
        value=entity_dict["value"]
    )
