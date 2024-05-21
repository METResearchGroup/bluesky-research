"""Models for context."""
from typing import Optional

from pydantic import BaseModel, Field

from lib.db.bluesky_models.embed import EmbeddedContextContextModel


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


class ContextModel(BaseModel):
    content_referenced_in_post: EmbeddedContextContextModel = Field(
        ..., description="The context referenced in the post."
    )
    urls_in_post: PostLinkedUrlsContextModel = Field(
        ..., description="The URLs in the post."
    )
    post_thread: ThreadContextModel = Field(
        ..., description="The context of the thread of the post."
    )
    post_tags_labels: PostTagsLabelsContextModel = Field(
        ..., description="The tags and labels of the post."
    )
    post_author_context: AuthorContextModel = Field(
        ..., description="The context of the author of the post."
    )


class FullPostContextModel(BaseModel):
    """Model for the context of a post."""
    uri: str = Field(..., description="The URI of the post.")
    context: ContextModel = Field(..., description="The context of the post.")
    text: str = Field(..., description="The text of the post.")
    timestamp: str = Field(..., description="The timestamp of the post.")
