"""Models for raw Bluesky records."""

import typing_extensions as te
from typing import Optional

from pydantic import BaseModel, Field


class RawPostRecord(BaseModel):
    """Raw post.

    Corresponds to https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.post.json.

    Based on the schema that is available from crawling the PDSes (the firehose
    might have a slightly different schema. TODO: consolidate this with
    `process_firehose_post`).
    """

    py_type: str = Field(default="app.bsky.feed.post", alias="$type", frozen=True)
    createdAt: str = Field(
        ...,
        description="Client-declared timestamp when this post was originally created.",
    )  # noqa
    text: str = Field(
        ...,
        description="The primary post content. May be an empty string, if there are embeds.",
    )
    embed: Optional[str] = Field(
        default=None,
        description="Embed content (images, video, external link, record, or record with media) as a JSON string.",
    )
    entities: Optional[str] = Field(
        default=None,
        description="DEPRECATED: replaced by app.bsky.richtext.facet. Stored as a JSON string.",
    )
    facets: Optional[str] = Field(
        default=None,
        description="Annotations of text (mentions, URLs, hashtags, etc). Stored as a JSON string.",
    )
    reply: Optional[str] = Field(
        default=None,
        description="Reference to a post this post is replying to, stored as a JSON string.",
    )
    labels: Optional[str] = Field(
        default=None,
        description="Self-label values for this post. Effectively content warnings. Stored as a JSON string.",
    )
    langs: Optional[str] = Field(
        default=None,
        description="Indicates human language of post primary text content. Stored as a JSON string.",
    )
    tags: Optional[str] = Field(
        default=None,
        description="Additional hashtags, in addition to any included in post text and facets. Stored as a JSON string.",
    )


class RawPostReference(BaseModel):
    """A raw post reference. Contains enough information to identify a post
    (uri and cid).

    Corresponds to https://github.com/MarshalX/atproto/blob/main/lexicons/com.atproto.repo.strongRef.json#L4
    """  # noqa

    cid: str = Field(..., description="The CID of the post.")
    uri: str = Field(..., description="The URI of the post.")
    py_type: te.Literal["com.atproto.repo.strongRef"] = Field(
        default="com.atproto.repo.strongRef", alias="$type", frozen=True
    )  # noqa


class RawReplyRef(BaseModel):
    """A raw reply reference. Refers to the posts in the thread that the
    post is replying to, the parent post (direct post that is being replied to)
    and root (the first post in the thread). These can be the same (i.e., if
    the post is a reply to one post).

    Corresponds to https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/post.py#L20
    """

    parent: RawPostReference = Field(
        ..., description="The parent post that is being replied to."
    )  # noqa
    root: RawPostReference = Field(
        ..., description="The root post that is being replied to."
    )  # noqa
    py_type: te.Literal["app.bsky.feed.defs#replyRef"] = Field(
        default="app.bsky.feed.defs#replyRef", alias="$type", frozen=True
    )  # noqa


class RawReply(RawPostRecord):
    """A raw reply.

    Same as a raw post but has ReplyRef in the "reply" field.
    """

    reply: RawReplyRef = Field(..., description="The reply reference.")
    py_type: te.Literal["app.bsky.feed.post"] = Field(
        default="app.bsky.feed.post", alias="$type", frozen=True
    )  # noqa


class RawRepost(BaseModel):
    """A raw repost.

    Corresponds to https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.repost.json
    """

    createdAt: str = Field(
        ..., description="The timestamp of when the repost was created."
    )
    subject: RawPostReference = Field(
        ..., description="Reference to the post being reposted."
    )
    py_type: te.Literal["app.bsky.feed.repost"] = Field(
        default="app.bsky.feed.repost", alias="$type", frozen=True
    )


class RawLikeRecord(BaseModel):
    """Model for a raw like record (the record itself). This is a component of
    the actual like, which has both the record and some metadata.

    Example:
        Record(
                created_at='2024-07-02T14:05:23.807Z',
                subject=Main(
                    cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                    uri='at://did:plc:ucfj5xnywoxbdaxqelvpzyqz/app.bsky.feed.post/3kvkbi7yfb22z',
                    py_type='com.atproto.repo.strongRef'
                ),
                py_type='app.bsky.feed.like'
        )

    Based on https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.like.json.
    """

    createdAt: str = Field(
        ..., description="The timestamp of when the record was created on Bluesky."
    )  # noqa
    subject: RawPostReference = Field(
        ..., description="The actual post record that was liked."
    )  # noqa
    py_type: te.Literal["app.bsky.feed.like"] = Field(
        default="app.bsky.feed.like", alias="$type", frozen=True
    )  # noqa


class RawLike(BaseModel):
    """Model for a raw like from the firehose.

    Example input from the firehose:
    {
        'author': 'did:plc:aq45jcquopr4joswmfdpsfnh',
        'cid': 'bafyreihus4wvodsdmhsschvb57dn7qsl6wxanu5fv6httkq2njd7zqadri',
        'record': Record(
            created_at='2024-07-02T14:05:23.807Z',
            subject=Main(
                cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                uri='at://did:plc:ucfj5xnywoxbdaxqelvpzyqz/app.bsky.feed.post/3kvkbi7yfb22z',
                py_type='com.atproto.repo.strongRef'
            ),
            py_type='app.bsky.feed.like'
        ),
        'uri': 'at://did:plc:aq45jcquopr4joswmfdpsfnh/app.bsky.feed.like/3kwckubmt342n'
    }
    """  # noqa

    author: str = Field(..., description="The DID of the author of the post.")
    cid: str = Field(..., description="The CID of the record.")
    record: RawLikeRecord = Field(
        ..., description="The actual post (and metadata) that was liked."
    )  # noqa
    uri: str = Field(..., description="The URI of the like record.")


class RawFollowRecord(BaseModel):
    """Model for a raw follow record (the record itself). This is a component of
    the actual follow, which has both the record and some metadata.

    Example:
        Record(
            created_at='2024-07-02T17:48:48.627Z',
            subject='did:plc:vjoaculzgxuqa3gdtqkmqawn',
            py_type='app.bsky.graph.follow'
        )
    """  # noqa

    createdAt: str = Field(
        ..., description="The timestamp of when the record was created on Bluesky."
    )  # noqa
    subject: str = Field(..., description="The DID of the user being followed.")  # noqa
    py_type: te.Literal["app.bsky.graph.follow"] = Field(
        default="app.bsky.graph.follow", alias="$type", frozen=True
    )  # noqa


class RawFollow(BaseModel):
    """Model for a raw follow from the firehose.

    Example:

    {
        'created': [
            {
                'record': Record(
                    created_at='2024-07-02T17:48:48.627Z',
                    subject='did:plc:vjoaculzgxuqa3gdtqkmqawn',
                    py_type='app.bsky.graph.follow'
                ),
                'uri': 'at://did:plc:qqdx6sgha4cqqhxs564g43zq/app.bsky.graph.follow/3kwcxduaskd2p',
                'cid': 'bafyreibwn4kwlezxabt2bzpopwfh7lbo56n4xb62wlbm5moqliwl4pzum4',
                'author': 'did:plc:qqdx6sgha4cqqhxs564g43zq'
            }
        ],
        'deleted': []
    }

    The author is the entity who is following, and the record.subject is the
    user who is being followed. For example, if A follows B, then the author is
    the DID of A and the record.subject is the DID of B.
    """  # noqa

    uri: str = Field(..., description="The URI of the follow record.")
    cid: str = Field(..., description="The CID of the record.")
    record: RawFollowRecord = Field(..., description="The actual follow record.")  # noqa
    author: str = Field(
        ..., description="The DID of the author of the follow. Matches follower_did."
    )  # noqa
    follower_did: str = Field(
        ..., description="The DID of the user doing the following."
    )  # noqa
    followee_did: str = Field(..., description="The DID of the user being followed.")  # noqa


class RawBlock(BaseModel):
    """A raw block.

    Corresponds to https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.graph.block.json
    """

    createdAt: str = Field(
        ..., description="The timestamp of when the block was created."
    )
    subject: str = Field(..., description="The DID of the user being blocked.")
    py_type: te.Literal["app.bsky.graph.block"] = Field(
        default="app.bsky.graph.block", alias="$type", frozen=True
    )


class FirehoseSubscriptionStateCursorModel(BaseModel):
    """Model for the cursor in the firehose subscription state."""

    service: str = Field(..., description="The service that the cursor is for.")  # noqa
    cursor: int = Field(..., description="The cursor value.")
    timestamp: str = Field(
        ..., description="The timestamp that the cursor was inserted."
    )  # noqa
