"""Create Bluesky classes from dict.

Based on https://github.com/MarshalX/atproto/tree/main/packages/atproto_client/models/app/bsky/embed
"""  # noqa
from atproto_client.models.app.bsky.embed.external import External, Main as ExternalEmbed  # noqa
from atproto_client.models.app.bsky.embed.images import AspectRatio, Image, Main as ImageEmbed  # noqa
from atproto_client.models.app.bsky.embed.record import Main as RecordEmbed
from atproto_client.models.app.bsky.embed.record_with_media import Main as RecordWithMediaEmbed  # noqa

from transform.create_bsky_objects.strongRef import create_strong_ref


def create_external(external_dict: dict) -> External:
    return External(
        description=external_dict["description"],
        title=external_dict["title"],
        uri=external_dict["uri"],
        thumb=None
    )


def create_external_embed(embed_dict: dict) -> ExternalEmbed:
    return ExternalEmbed(external=create_external(embed_dict["external"]))


def create_image(image_dict: dict) -> Image:
    return Image(
        alt=image_dict["alt"],
        aspect_ratio=AspectRatio(
            height=image_dict["aspect_ratio"]["height"],
            width=image_dict["aspect_ratio"]["width"]
        ),
    )


def create_image_embed(embed_dict: dict) -> list[ImageEmbed]:
    images: list[dict] = embed_dict["images"]
    return [create_image(image) for image in images]


def create_record_embed(embed_dict: dict) -> RecordEmbed:
    return RecordEmbed(
        record=create_strong_ref(embed_dict["record"])
    )


def create_record_with_media_embed(embed_dict: dict) -> RecordWithMediaEmbed:
    media_dict = embed_dict["media"]
    if media_dict["py_type"] == "app.bsky.embed.images":
        media = create_image_embed(media_dict)
    else:
        media = create_external_embed(media_dict)
    record = create_record_embed(embed_dict["record"])
    return RecordWithMediaEmbed(media=media, record=record)


def create_embed(embed_dict: dict):
    if embed_dict["py_type"] == "app.bsky.embed.external":
        return create_external_embed(embed_dict)
    elif embed_dict["py_type"] == "app.bsky.embed.images":
        return create_image_embed(embed_dict)
    elif embed_dict["py_type"] == "app.bsky.embed.record":
        return create_record_embed(embed_dict)
    elif embed_dict["py_type"] == "app.bsky.embed.recordWithMedia":
        return create_record_with_media_embed(embed_dict)
