"""The models for embeds in Bluesky.

Based on https://github.com/MarshalX/atproto/tree/main/packages/atproto_client/models/app/bsky/embed

We take only the fields from the different embeds that we are able to actually
use, not all the fields available.
"""  # noqa
from typing import Optional, Union

from pydantic import BaseModel, Field, validator


class ImagesContextModel(BaseModel):
    """Pydantic model for the images context.

    Since we don't have OCR, we can't extract text from images, but we can
    extract the alt texts of the images in the post.
    """
    image_alt_texts: Optional[str] = Field(
        default=None,
        description="The alt texts of the images in the post."
    )


class RecordContextModel(BaseModel):
    """Pydantic model for the record context, which are records that are
    referenced in a post. Records are just another name for posts, such as if
    a post links to another Bluesky post."""
    text: Optional[str] = Field(
        default=None,
        description="The text of the post."
    )
    embed_image_alt_text: Optional[ImagesContextModel] = Field(
        default=None,
        description="The alt text of the embedded image in the post."
    )


class RecordWithMediaContextModel(BaseModel):
    """Pydantic model for the record with media context.

    This is a record that has media, such as an image or video.
    """
    images_context: Optional[ImagesContextModel] = Field(
        default=None,
        description="The images context of the post."
    )
    embedded_record_context: Optional[RecordContextModel] = Field(
        default=None,
        description="The record context of the embedded post."
    )


class ExternalEmbedContextModel(BaseModel):
    """Pydantic model for the external embed context, which is some externally
    linked content plus its preview card.

    External embeds are links to external content, like a YouTube video or a
    news article, which also has a preview card showing its content.
    """
    title: Optional[str] = Field(
        default=None,
        description="The title of the external embed."
    )
    description: Optional[str] = Field(
        default=None,
        description="The description of the external embed."
    )


class EmbeddedContextContextModel(BaseModel):
    """Pydantic model for any embedded content of a post
    (i.e., images, links, attachments)"""
    has_embedded_content: Optional[bool] = Field(
        default=False,
        description="Whether the post has embedded content."
    )
    embedded_content_type: Optional[str] = Field(
        default=None,
        description="The type of embedded content in the post."
    )
    embedded_record_with_media_context: Optional[
        Union[ExternalEmbedContextModel, ImagesContextModel, RecordContextModel, RecordWithMediaContextModel]  # noqa
    ] = Field(
        default=None,
        description="The record with media context of the embedded content."
    )

    @validator('embedded_content_type')
    def validate_embedded_content_type(cls, v):
        if v not in ["external_embed", "images", "record", "record_with_media"]:  # noqa
            raise ValueError("embedded_content_type must be one of 'external_embed', 'images', 'record', 'record_with_media'")  # noqa


class ProcessedRecordEmbed(BaseModel):
    """Pydantic model for processing record embeds.

    Right now this just manages references to other records.
    """
    cid: Optional[str] = Field(
        default=None,
        description="The CID of the record."
    )
    uri: Optional[str] = Field(
        default=None,
        description="The URI of the record."
    )


class ProcessedExternalEmbed(BaseModel):
    """Pydantic model for an external embed."""
    description: Optional[str] = Field(
        default=None,
        description="Description of the external embed."
    )
    title: Optional[str] = Field(
        default=None,
        description="Title of the external embed."
    )
    uri: Optional[str] = Field(
        default=None,
        description="URI of the external embed."
    )


class ProcessedRecordWithMediaEmbed(BaseModel):
    """Pydantic model for a record with media embedded."""
    image_alt_text: Optional[str] = Field(
        default=None,
        description="The alt text of the image in the post."
    )
    embedded_record: Optional[ProcessedRecordEmbed] = Field(
        default=None,
        description="The embedded record, if any."
    )


class ProcessedEmbed(BaseModel):
    """Pydantic model for the processed embed."""
    has_image: bool = Field(default=False, description="Whether the post has an image.")  # noqa
    image_alt_text: Optional[str] = Field(
        default=None,
        description="The alt text of the image in the post."
    )
    has_embedded_record: bool = Field(
        default=False,
        description="Whether the post has an embedded record."
    )
    embedded_record: Optional[ProcessedRecordEmbed] = Field(
        default=None,
        description="The embedded record, if any."
    )
    has_external: bool = Field(
        default=False,
        description="Whether the post has an external embed."
    )
    external: Optional[ProcessedExternalEmbed] = Field(
        default=None,
        description="External embed, if any."
    )
