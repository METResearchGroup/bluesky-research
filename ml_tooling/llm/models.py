"""Model classes for our LLM inference."""

from pydantic import BaseModel, Field, validator
from typing import Optional, Union


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
