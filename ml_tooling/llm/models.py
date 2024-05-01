"""Model classes for our LLM inference."""

from pydantic import BaseModel, Field, validator
from typing import Optional, Union


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


class AuthorContextModel(BaseModel):
    """Pydantic model for the author context."""
    post_author_is_reputable_news_org: Optional[bool] = Field(
        default=False,
        description="Whether the author is a reputable news organization."
    )


class ThreadPostContextModel(BaseModel):
    """Pydantic model for posts in the thread context.

    For our case, we'll use this to valid the parent post of the post we're
    analyzing as well as the root post of the thread.
    """
    text: Optional[str] = Field(
        default='',
        description="The text of the post."
    )
    embedded_image_alt_text: Optional[str] = Field(
        default='',
        description="The alt text of the embedded image in the post."
    )


class ThreadContextModel(BaseModel):
    """Pydantic model for the thread context."""
    thread_root_post: Optional[ThreadPostContextModel] = Field(
        default=None,
        description="The root post of the thread."
    )
    thread_parent_post: Optional[ThreadPostContextModel] = Field(
        default=None,
        description="The parent post of the post we're analyzing."
    )


class ContextEmbedUrlModel(BaseModel):
    """Pydantic model for the embed URL context.

    Adds context for URLs that are added as embeds in the post.

    These are for links that are embeds in the post, not just linked to in
    the text of the post.

    Example: https://bsky.app/profile/parismarx.bsky.social/post/3kpvlrkcr6m2q
    """
    url: Optional[str] = Field(
        default=None,
        description="The URL in the embed of the post."
    )
    is_trustworthy_news_article: Optional[bool] = Field(
        default=False,
        description="Whether the URL in the embed is a trustworthy news article."
    )


class ContextUrlInTextModel(BaseModel):
    """Pydantic model for URLs that are linked to in the text of the post.

    URLs are included in the text of the post as "facets", as this is how
    the Bluesky platform does links in lieu of markdown."""
    has_trustworthy_news_links: Optional[bool] = Field(
        default=False,
        description="Whether the post has trustworthy news links."
    )


class PostLinkedUrlsContextModel(BaseModel):
    """Pydantic model for the post linked URLs context."""
    url_in_text_context: ContextUrlInTextModel = Field(
        default=None,
        description="Information about the URLs that are linked to in the text of the post."  # noqa
    )
    embed_url_context: ContextEmbedUrlModel = Field(
        default=None,
        description="Information about the URL in the embed of the post."
    )


class PostTagsLabelsContextModel(BaseModel):
    """Pydantic model for the post tags and labels context."""
    post_tags: Optional[str] = Field(
        default=None,
        description="The tags of the post."
    )
    post_labels: Optional[str] = Field(
        default=None,
        description="The labels of the post."
    )
