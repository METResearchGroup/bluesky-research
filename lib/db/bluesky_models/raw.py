"""Models for raw Bluesky records."""
import typing_extensions as te

from pydantic import BaseModel, Field


class RawPostReference(BaseModel):
    """A raw post reference. Contains enough information to identify a post
    (uri and cid).

    Corresponds to https://github.com/MarshalX/atproto/blob/main/lexicons/com.atproto.repo.strongRef.json#L4
    """  # noqa
    cid: str = Field(..., description="The CID of the post.")
    uri: str = Field(..., description="The URI of the post.")
    py_type: te.Literal["com.atproto.repo.strongRef"] = Field(default="com.atproto.repo.strongRef", frozen=True)  # noqa


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
    """
    created_at: str = Field(..., description="The timestamp of when the record was created on Bluesky.")  # noqa
    subject: RawPostReference = Field(..., description="The actual post record that was liked.")  # noqa
    py_type: te.Literal["app.bsky.feed.like"] = Field(default="app.bsky.feed.like", frozen=True)  # noqa


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
    record: RawLikeRecord = Field(..., description="The actual post (and metadata) that was liked.")  # noqa
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
    created_at: str = Field(..., description="The timestamp of when the record was created on Bluesky.")  # noqa
    subject: str = Field(..., description="The DID of the user being followed.")  # noqa
    py_type: te.Literal["app.bsky.graph.follow"] = Field(default="app.bsky.graph.follow", frozen=True)  # noqa


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
    author: str = Field(..., description="The DID of the author of the follow. Matches follower_did.")  # noqa
    follower_did: str = Field(..., description="The DID of the user doing the following.")  # noqa
    followee_did: str = Field(..., description="The DID of the user being followed.")  # noqa


class FirehoseSubscriptionStateCursorModel(BaseModel):
    """Model for the cursor in the firehose subscription state."""
    service: str = Field(..., description="The service that the cursor is for.")  # noqa
    cursor: int = Field(..., description="The cursor value.")
    timestamp: str = Field(..., description="The timestamp that the cursor was inserted.")  # noqa
