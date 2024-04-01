"""Transforms raw data.

Based on https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.defs.json
"""
from typing import Optional, TypedDict, Union

from atproto_client.models.app.bsky.actor.defs import (
    ProfileView, ProfileViewBasic, ProfileViewDetailed
)
from atproto_client.models.app.bsky.feed.post import Entity, Main as Record, ReplyRef
from atproto_client.models.app.bsky.feed.defs import FeedViewPost, PostView
from atproto_client.models.app.bsky.richtext.facet import Main as Facet
from atproto_client.models.com.atproto.label.defs import SelfLabel
from atproto_client.models.dot_dict import DotDict


def hydrate_feed_view_post(feed_post: dict) -> FeedViewPost:
    """Hydrate a FeedViewPost from a dictionary."""
    return FeedViewPost(**feed_post)


def get_post_author_info(post_author: ProfileViewBasic) -> dict:
    """Get author information from a post."""
    assert isinstance(post_author, ProfileViewBasic)
    return {
        "author_did": post_author["did"],
        "author_handle": post_author["handle"],
        "author_display_name": post_author["display_name"],
    }


def get_post_record_info(post_record: Union[DotDict, Record]) -> dict:
    assert isinstance(post_record, DotDict) or isinstance(post_record, Record)
    return {
        "created_at": post_record["created_at"],
        "text": post_record["text"],
        "langs": post_record["langs"],
    }


def flatten_post(post: PostView) -> dict:
    """Flattens the post, grabs engagement information, and other identifiers
    for a given post."""
    assert isinstance(post, PostView)
    post_author_info = get_post_author_info(post.author)
    post_record_info = get_post_record_info(post.record)
    other_info = {
        "cid": post.cid,
        "indexed_at": post.indexed_at,
        "like_count": post.like_count,
        "reply_count": post.reply_count,
        "repost_count": post.repost_count,
    }
    return {**post_author_info, **post_record_info, **other_info}


class FlattenedFirehosePost(TypedDict):
    uri: str
    created_at: str
    text: str
    langs: str
    entities: str
    facets: str  # https://www.pfrazee.com/blog/why-facets
    labels: str
    reply: str
    reply_parent: str
    reply_root: str
    tags: str
    py_type: str
    cid: str
    author: str


def process_facet(facet: Facet) -> str:
    """Processes a facet.

    A facet is a richtext element. This is Bluesky's way of not having to
    explicitly support Markdown.

    See the following:
    - https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py#L64
    - https://www.pfrazee.com/blog/why-facets

    Examples:
    {
        'record': Main(
            created_at='2024-02-07T12:13:04.826Z',
            text='@horhaa37m.bsky.social 温泉♨️の後の🥛美味いよ。こちらでもよろしくお願いします。',
            embed=Main(images=[Image(alt='', image=BlobRef(mime_type='image/jpeg', size=799055, ref='bafkreidtepeeezypyvsk4yi7vmlv3gclpe5aizuerzkj2ok4karm5pgydm', py_type='blob'), aspect_ratio=AspectRatio(height=2000, width=1500, py_type='app.bsky.embed.images#aspectRatio'),py_type='app.bsky.embed.images#image')],py_type='app.bsky.embed.images'),
            entities=None,
            facets=[
                Main(
                    features=[Mention(did='did:plc:beitbj4for3pe4babgntgfjc', py_type='app.bsky.richtext.facet#mention')],
                    index=ByteSlice(byte_end=22, byte_start=0, py_type='app.bsky.richtext.facet#byteSlice'),
                    py_type='app.bsky.richtext.facet'
                )
            ],
            labels=None,
            langs=['ja'],
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ), 
        'uri': 'at://did:plc:wxx27pahlilipwya5pnfdtvm/app.bsky.feed.post/3kktaiztebx2b',
        'cid': 'bafyreifralpf5msynbyix2r2beenubmcz5lzktejzvviq4bwx2ehqt6pzi',
        'author': 'did:plc:wxx27pahlilipwya5pnfdtvm'
    }
    {
        'record': Main(
            created_at='2024-02-07T12:13:03.916Z',
            text='soner\n\n#furry #furryart',
            embed=Main(images=[Image(alt='', image=BlobRef(mime_type='image/jpeg', size=234804, ref='bafkreiaobrt6spib7ijvk3pximy4o7ijvcistrwdyg6canqixo6xxzsv4u', py_type='blob'), aspect_ratio=AspectRatio(height=719, width=1280, py_type='app.bsky.embed.images#aspectRatio'), py_type='app.bsky.embed.images#image')], py_type='app.bsky.embed.images'),
            entities=None,
            facets=[
                Main(
                    features=[Tag(tag='furry', py_type='app.bsky.richtext.facet#tag')],
                    index=ByteSlice(byte_end=13, byte_start=7, py_type='app.bsky.richtext.facet#byteSlice'),
                    py_type='app.bsky.richtext.facet'
                ), Main(
                    features=[Tag(tag='furryart', py_type='app.bsky.richtext.facet#tag')],
                    index=ByteSlice(byte_end=23, byte_start=14, py_type='app.bsky.richtext.facet#byteSlice'),
                    py_type='app.bsky.richtext.facet'
                )
            ],
            labels=None,
            langs=['en'],
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:joynts37i4ez3qpavk3p3gzj/app.bsky.feed.post/3kktaiyxqfv2c',
        'cid': 'bafyreid6ef2yjudqlqgrloxyqfduqfqjerfeqr65moj4bvufypodpjsgee',
        'author': 'did:plc:joynts37i4ez3qpavk3p3gzj'
    }
    """ # noqa
    # tags
    if facet.py_type == "app.bsky.richtext.facet":
        features = facet["features"]
        features_list = []
        for feature in features:
            if feature.py_type == "app.bsky.richtext.facet#tag":
                features_list.append(f"tag:{feature['tag']}")
            # links
            if feature.py_type == "app.bsky.richtext.facet#link":
                features_list.append(f"link:{feature['uri']}")
            # mentions
            if feature.py_type == "app.bsky.richtext.facet#mention":
                features_list.append(f"mention:{feature['did']}")
    return ",".join(features_list)


def process_facets(facets: list[Facet]) -> str:
    """Processes a list of facets."""
    return ','.join([process_facet(facet) for facet in facets])


def process_label(label: SelfLabel) -> str:
    """Processes a single label.
    
    Example: 
    SelfLabel(val='porn', py_type='com.atproto.label.defs#selfLabel'

    Returns a single label.
    """
    return label.val


def process_labels(labels: list[SelfLabel]) -> str:
    """Processes labels.
    
    Example:
    SelfLabels(
        values=[SelfLabel(val='porn', py_type='com.atproto.label.defs#selfLabel')],
        py_type='com.atproto.label.defs#selfLabels'
    )
    """
    return ','.join([process_label(label) for label in labels.values])

def process_entity(entity: Entity) -> str:
    """Returns the value of an entity.

    Example:

    Entity(
        index=TextSlice(end=81, start=39, py_type='app.bsky.feed.post#textSlice'),
        type='link',
        value='https://song.link/s/2Zh97yLVZeOpwzFoXtkfBt',
        py_type='app.bsky.feed.post#entity'
    )
    """
    return entity.value


def process_entities(entities: list[Entity]) -> str:
    """Processes entities.

    Example:
    [
        Entity(
            index=TextSlice(end=81, start=39, py_type='app.bsky.feed.post#textSlice'),
            type='link',
            value='https://song.link/s/2Zh97yLVZeOpwzFoXtkfBt',
            py_type='app.bsky.feed.post#entity'
        )
    ]
    """ # noqa
    return ','.join([process_entity(entity) for entity in entities])


def process_replies(reply: Optional[ReplyRef]) -> dict:
    """Manages any replies if the post is part of a larger thread.

    Returns the parent comment that the reply is responding to as well as the
    root post of the reply.
    """
    reply_parent = None
    reply_root = None

    if reply is not None:
        if hasattr(reply, "parent"):
            reply_parent: str = reply["parent"]["uri"]
    if reply is not None:
        if hasattr(reply, "root"):
            reply_root: str = reply["root"]["uri"]

    return {
        "reply_parent": reply_parent,
        "reply_root": reply_root,
    }


def flatten_firehose_post(post: dict) -> FlattenedFirehosePost:
    """Flattens a post from the firehose.

    For some reason, the post format from the firehose is different from the
    post format when the post is part of a feed?

    Here is an example of this format:

    {
        'record': Main(
            created_at='2024-02-07T05:10:02.159Z',
            text='こんなポストするとBANされそうで怖いです',
            embed=Main(
                record=Main(
                    cid='bafyreidy6bxkwxbjvw6mqfxivp7rjywk3gpnzvbg2vaks2qhljzs6manyq',
                    uri='at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/3kksirfddwa2z',
                    py_type='com.atproto.repo.strongRef'
                ),
                py_type='app.bsky.embed.record'
            ),
            entities=None,
            facets=None,
            labels=None,
            langs=['ja'],
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/3kksiuknorv2u',
        'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
        'author': 'did:plc:sjeosezgc7mpqn6sfc7neabg'
    }
    """ # noqa
    processed_replies: dict = process_replies(post["record"].reply)
    if post["record"]["entities"] or post["record"]["labels"] or post["record"]["tags"]:
        entities = post["record"]["entities"]
        labels = post["record"]["labels"]
        tags = post["record"]["tags"]
        print("Either entities or labels to be managed.")
        breakpoint()
    try:
        # flatten the post
        flattened_firehose_dict = {
            "uri": post["uri"],
            "created_at": post["record"]["created_at"],
            "text": post["record"]["text"],
            "langs": (
                ','.join(post["record"]["langs"])
                if post["record"]["langs"] else None
            ),
            "entities": (
                process_entities(post["record"]["entities"])
                if post["record"]["entities"] else None
            ),
            "facets": (
                process_facets(post["record"]["facets"])
                if post["record"]["facets"] else None
            ),
            "labels": (
                process_labels(post["record"]["labels"])
                if post["record"]["labels"] else None
            ),
            "reply_parent": processed_replies["reply_parent"],
            "reply_root": processed_replies["reply_root"],
            "tags": (
                ','.join(post["record"]["tags"])
                if post["record"]["tags"] else None
            ),
            "py_type": post["record"]["py_type"],
            "cid": post["cid"],
            "author": post["author"],
        }
    except Exception as e:
        print(f"Exception in flattening post: {e}")
    return flattened_firehose_dict


class FlattenedFeedViewPost(TypedDict):
    author_did: str
    author_handle: str
    author_display_name: str
    created_at: str
    text: str
    langs: str
    cid: str
    indexed_at: str
    like_count: int
    reply_count: int
    repost_count: int


def flatten_feed_view_post(post: FeedViewPost) -> dict:
    assert isinstance(post, FeedViewPost)
    post: FlattenedFeedViewPost = flatten_post(post.post)
    return post


def flatten_user_profile(
    user: Union[ProfileViewDetailed, ProfileView]
) -> dict:
    """Flattens a user profile. This is a profile view from Bluesky.

    To flatten, we just remove the `viewer` field, which is a `ViewerState`,
    especially since it doesn't give us any relevant information.
    """
    return {
        field: getattr(user, field)
        for field in user.__dict__.keys()
        if field != "viewer"
    }
