"""Transforms raw data.

Based on https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.defs.json
"""  # noqa

from typing import Optional, Union

from atproto_client.models.app.bsky.actor.defs import ProfileViewBasic
from atproto_client.models.app.bsky.embed.external import (
    External,
    Main as ExternalEmbed,
)  # noqa
from atproto_client.models.app.bsky.embed.images import Image, Main as ImageEmbed  # noqa
from atproto_client.models.app.bsky.embed.record import Main as RecordEmbed
from atproto_client.models.app.bsky.embed.record_with_media import (
    Main as RecordWithMediaEmbed,
)  # noqa
from atproto_client.models.app.bsky.feed.post import Entity, Record, ReplyRef
from atproto_client.models.app.bsky.feed.defs import FeedViewPost, PostView
from atproto_client.models.app.bsky.richtext.facet import (
    Link,
    Main as Facet,
    Mention,
    Tag,
)  # noqa
from atproto_client.models.com.atproto.label.defs import SelfLabel, SelfLabels
from atproto_client.models.com.atproto.repo.strong_ref import Main as StrongRef
from atproto_client.models.dot_dict import DotDict

from lib.helper import generate_current_datetime_str
from lib.db.bluesky_models.transformations import (
    PostMetadataModel,
    TransformedFeedViewPostModel,
    TransformedProfileViewBasicModel,
    TransformedRecordWithAuthorModel,
    TransformedRecordModel,
)
from lib.db.bluesky_models.embed import (
    ProcessedEmbed,
    ProcessedExternalEmbed,
    ProcessedRecordEmbed,
    ProcessedRecordWithMediaEmbed,
)


# we use a semicolon since it's unlikely for text to use semicolons.
LIST_SEPARATOR_CHAR = ";"
# some old data might use commas, so this is here for backwards compatibility.
LEGACY_CHAR_SEPARATORS = [";", ","]


def get_object_type_str(obj: object) -> str:
    """Get the object type as a string.

    The objects are often class instances (e.g., External), but they're also
    denoted either as "$type" or "py_type")
    """
    if hasattr(obj, "$type"):
        return getattr(obj, "$type")
    elif hasattr(obj, "py_type"):
        return obj.py_type
    raise ValueError(f"Object does not have a type attribute: {obj}.")


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


def process_mention(mention: Mention) -> str:
    """Processes a mention of another Bluesky user.

    See https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py
    """  # noqa
    return f"mention:{mention.did}"


def process_link(link: Link) -> str:
    """Processes a link. The URI here is the link itself.

    See https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py
    """  # noqa
    return f"link:{link.uri}"


def process_hashtag(tag: Tag) -> str:
    """Processes a hashtag. This is a tag that starts with a #, but the tag
    won't have a '#' in the value. (e.g., if the hashtag is #red, the tag value
    would be 'red').

    See https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py
    """  # noqa
    return f"tag:{tag.tag}"


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
    },
    {
        'created_at': '2024-03-30T16:25:14.902Z',
        'text': 'A newspaper editor — Chris Quinn of the Cleveland Plain Dealer — says it as clearly as he can:\n\n"Our Trump reporting upsets some readers, but there aren’t two sides to facts." www.cleveland.com/news/2024/03...\n\nQuinn writes of a different kind of access. Access to our own eyes and ears:',
        'embed': Main(images=[Image(alt='', image=BlobRef(mime_type='image/jpeg', size=399062, ref=IpldLink(link='bafkreiagbvatb77ka6hxsknjqpomcgfo7n3ext6fouzh66rlizt52aok6y'), py_type='blob'), aspect_ratio=AspectRatio(height=1040, width=1096, py_type='app.bsky.embed.images#aspectRatio'), py_type='app.bsky.embed.images#image')], py_type='app.bsky.embed.images'),
        'entities': None,
        'facets': [Main(features=[Link(uri='https://www.cleveland.com/news/2024/03/our-trump-reporting-upsets-some-readers-but-there-arent-two-sides-to-facts-letter-from-the-editor.html', py_type='app.bsky.richtext.facet#link')], index=ByteSlice(byte_end=215, byte_start=182, py_type='app.bsky.richtext.facet#byteSlice'), py_type='app.bsky.richtext.facet')],
        'labels': None,
        'langs': ['en'],
        'reply': None,
        'tags': None,
        'py_type': 'app.bsky.feed.post'
    }
    """  # noqa
    features: list = facet.features
    features_list = []

    for feature in features:
        # noticed a #Tag, so lowering manually to avoid errors while maintaining
        # backwards compatibility.
        if (
            isinstance(feature, Tag)
            or get_object_type_str(feature).lower() == "app.bsky.richtext.facet#tag"
            or get_object_type_str(feature).lower() == "app.bsky.richtext.facet.tag"
        ):
            features_list.append(process_hashtag(feature))
        elif (
            isinstance(feature, Link)
            or get_object_type_str(feature) == "app.bsky.richtext.facet#link"
            or get_object_type_str(feature) == "app.bsky.richtext.facet.link"
        ):
            features_list.append(process_link(feature))
        elif (
            isinstance(feature, Mention)
            or get_object_type_str(feature) == "app.bsky.richtext.facet#mention"  # noqa
            or get_object_type_str(feature) == "app.bsky.richtext.facet.mention"
        ):
            features_list.append(process_mention(feature))
        else:
            object_type = get_object_type_str(feature)
            raise ValueError(f"Unknown feature type: {object_type}")
    return LIST_SEPARATOR_CHAR.join(features_list)


def process_facets(facets: Optional[list[Facet]]) -> Optional[str]:
    """Processes a list of facets."""
    if not facets:
        return None
    return LIST_SEPARATOR_CHAR.join([process_facet(facet) for facet in facets])


def process_label(label: SelfLabel) -> str:
    """Processes a single label.

    Example:
    SelfLabel(val='porn', py_type='com.atproto.label.defs#selfLabel'

    Returns a single label.
    """  # noqa
    return label.val


def process_labels(labels: Optional[SelfLabels]) -> Optional[str]:
    """Processes labels.

    Example:
    SelfLabels(
        values=[SelfLabel(val='porn', py_type='com.atproto.label.defs#selfLabel')],
        py_type='com.atproto.label.defs#selfLabels'
    )

    Based off https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/label/defs.py#L38
    """  # noqa
    if not labels:
        return None
    label_values: list[SelfLabel] = labels.values
    return LIST_SEPARATOR_CHAR.join([process_label(label) for label in label_values])  # noqa


def process_entity(entity: Entity) -> str:
    """Returns the value of an entity.

    Example:

    Entity(
        index=TextSlice(end=81, start=39, py_type='app.bsky.feed.post#textSlice'),
        type='link',
        value='https://song.link/s/2Zh97yLVZeOpwzFoXtkfBt',
        py_type='app.bsky.feed.post#entity'
    )
    """  # noqa
    return entity.value


def process_entities(entities: Optional[list[Entity]]) -> Optional[str]:
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
    """  # noqa
    if not entities:
        return None
    return LIST_SEPARATOR_CHAR.join([process_entity(entity) for entity in entities])  # noqa


def process_replies(reply: Optional[ReplyRef]) -> dict:
    """Manages any replies if the post is part of a larger thread.

    Returns the parent comment that the reply is responding to as well as the
    root post of the reply.
    """
    reply_parent = None
    reply_root = None

    if reply is not None:
        if hasattr(reply, "parent"):
            parent: StrongRef = reply.parent
            reply_parent: str = parent.uri
    if reply is not None:
        if hasattr(reply, "root"):
            root: StrongRef = reply.root
            reply_root: str = root.uri

    return {
        "reply_parent": reply_parent,
        "reply_root": reply_root,
    }


def process_image(image: Image) -> str:
    """Processes an image added to a post.

    Follows specs in https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/embed/images.py
    and https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.embed.images.json

    Note: the only relevant field to us is the alt text. There's no way to get the
    actual link of the image, and the link that I did find is a hash of the CID.
    (as far as I can tell).
    """  # noqa
    return image.alt


def process_images(image_embed: ImageEmbed) -> str:
    """Processes images

    For now, we just return the alt texts of the images, separated by ;
    (since , is likely used in the text itself).
    """
    return LIST_SEPARATOR_CHAR.join(
        [process_image(image) for image in image_embed.images]
    )  # noqa


def process_strong_ref(strong_ref: StrongRef) -> dict:
    """Processes strong reference (a reference to another record)

    Follows specs in https://github.com/MarshalX/atproto/blob/main/lexicons/com.atproto.repo.strongRef.json#L4
    and https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/repo/strong_ref.py#L15
    """  # noqa
    return {
        "cid": strong_ref.cid,
        "uri": strong_ref.uri,
    }


def process_record_embed(record_embed: RecordEmbed) -> ProcessedRecordEmbed:
    """Processes record embeds.

    Record embeds are posts that are embedded in other posts. This is a way to
    reference another post in a post.
    """
    res: dict = process_strong_ref(record_embed.record)
    return ProcessedRecordEmbed(**res)


def process_external_embed(external_embed: ExternalEmbed) -> ProcessedExternalEmbed:  # noqa
    """Processes an "external" embed, which is some externally linked content
    plus its preview card.

    External embeds are links to external content, like a YouTube video or a
    news article, which also has a preview card showing its content.

    We don't need to include the image or other blobs since we don't have a way
    to hydrate them anyways.
    """
    external: External = external_embed.external
    res = {
        "description": external.description,
        "title": external.title,
        "uri": external.uri,
    }
    return ProcessedExternalEmbed(**res)


def process_record_with_media_embed(
    record_with_media_embed: RecordWithMediaEmbed,
) -> ProcessedRecordWithMediaEmbed:
    """Processes a record with media embed, which is when a post both
    references another record as well as has media (i.e., image) attached.

    Follows spec in https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/embed/record_with_media.py

    Media is normally an image, but it can also support other embeds
    like links to songs or videos. We currently only process for now if it's an
    image.
    """  # noqa
    media: Union[ExternalEmbed, ImageEmbed] = record_with_media_embed.media
    record_embed: RecordEmbed = record_with_media_embed.record

    image_alt_text = ""
    if (
        isinstance(media, ImageEmbed)
        or get_object_type_str(media) == "app.bsky.embed.images"
    ):
        image_alt_text: str = process_images(media)
    processed_record: ProcessedRecordEmbed = process_record_embed(record_embed)

    res = {
        "image_alt_text": image_alt_text,
        "embedded_record": processed_record,
    }
    return ProcessedRecordWithMediaEmbed(**res)


def process_embed(
    embed: Union[ExternalEmbed, ImageEmbed, RecordEmbed, RecordWithMediaEmbed],
) -> ProcessedEmbed:
    """Processes embeds.

    Follows specs in https://github.com/MarshalX/atproto/tree/main/packages/atproto_client/models/app/bsky/embed

    Image embed class is a container class for an arbitrary amount of attached images (max=4)
    """  # noqa
    res = {
        "has_image": False,
        "image_alt_text": None,
        "has_embedded_record": False,
        "embedded_record": None,
        "has_external": False,
        "external": None,
    }
    if embed is None:
        return ProcessedEmbed(**res)

    if isinstance(embed, ImageEmbed) or embed.py_type == "app.bsky.embed.images":
        res["has_image"] = True
        image_alt_text: str = process_images(embed)
        res["image_alt_text"] = image_alt_text

    if isinstance(embed, RecordEmbed) or embed.py_type == "app.bsky.embed.record":
        res["has_embedded_record"] = True
        embedded_record: ProcessedRecordEmbed = process_record_embed(embed)
        res["embedded_record"] = embedded_record

    if isinstance(embed, ExternalEmbed) or embed.py_type == "app.bsky.embed.external":
        res["has_external"] = True
        external_embed: ProcessedExternalEmbed = process_external_embed(embed)
        res["external"] = external_embed

    if (
        isinstance(embed, RecordWithMediaEmbed)
        or embed.py_type == "app.bsky.embed.recordWithMedia"
    ):
        res["has_image"] = True
        res["has_embedded_record"] = True
        processed_embed: ProcessedRecordWithMediaEmbed = (
            process_record_with_media_embed(embed)
        )  # noqa
        image_alt_text: str = processed_embed.image_alt_text
        embedded_record: ProcessedRecordEmbed = processed_embed.embedded_record
        res["image_alt_text"] = image_alt_text
        res["embedded_record"] = embedded_record

    return ProcessedEmbed(**res)


def process_langs(langs: Optional[list[str]]) -> Optional[str]:
    """Processes languages.

    Example:
    ['ja']
    """  # noqa
    if not langs:
        return None
    return LIST_SEPARATOR_CHAR.join(langs)


def process_tags(tags: Optional[list[str]]) -> Optional[str]:
    """Processes tags.

    Example:
    ['furry', 'furryart']
    """  # noqa
    if not tags:
        return None
    return LIST_SEPARATOR_CHAR.join(tags)


# NOTE: 'Main' refs were changed to 'Record'
# https://github.com/MarshalX/atproto/pull/323
def process_firehose_post(post: dict) -> Optional[TransformedRecordWithAuthorModel]:  # noqa
    """Processes a post from the firehose.

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
    """  # noqa
    record: Record = post["record"]
    transformed_record: TransformedRecordModel = transform_post_record(record)
    metadata_dict = {
        "url": "",
        "source_feed": "firehose",
        "synctimestamp": generate_current_datetime_str(),
    }
    metadata: PostMetadataModel = PostMetadataModel(**metadata_dict)
    try:
        flattened_firehose_dict = {
            "uri": post["uri"],
            "cid": post["cid"],
            "author": post["author"],
            "metadata": metadata,
            "record": transformed_record,
        }
        return TransformedRecordWithAuthorModel(**flattened_firehose_dict)
    except Exception as e:
        print(f"Exception in flattening post: {e}")
        return None


def get_feedviewpost_metadata(
    post: FeedViewPost, enrichment_data: dict
) -> PostMetadataModel:
    metadata = {}
    handle = post.post.author.handle
    uri = post.post.uri.split("/")[-1]
    metadata["url"] = f"https://bsky.app/profile/{handle}/post/{uri}"
    metadata["source_feed"] = enrichment_data["source_feed"]
    metadata["feed_url"] = enrichment_data["feed_url"]
    metadata["synctimestamp"] = generate_current_datetime_str()
    return metadata


def transform_post_record(record: Record) -> TransformedRecordModel:
    """Transforms the raw post record and returns a transformed version."""
    processed_replies: dict = process_replies(record.reply)
    res = {
        "created_at": record.created_at,
        "text": record.text,
        "embed": process_embed(record.embed),
        "entities": process_entities(record.entities),
        "facets": process_facets(record.facets),
        "labels": process_labels(record.labels),
        "langs": process_langs(record.langs),
        "reply_parent": processed_replies["reply_parent"],
        "reply_root": processed_replies["reply_root"],
        "tags": process_tags(record.tags),
        "py_type": record.py_type,
    }
    return TransformedRecordModel(**res)


def transform_author_profile(
    author: ProfileViewBasic,
) -> TransformedProfileViewBasicModel:  # noqa
    res = {
        "did": author.did,
        "handle": author.handle,
        "avatar": author.avatar,
        "display_name": author.display_name,
        "py_type": author.py_type,
    }
    return TransformedProfileViewBasicModel(**res)


def transform_feedview_post(
    post: FeedViewPost, enrichment_data: dict
) -> TransformedFeedViewPostModel:
    """Transforms a feed view post."""
    metadata: PostMetadataModel = get_feedviewpost_metadata(
        post=post, enrichment_data=enrichment_data
    )
    raw_author: ProfileViewBasic = post.post.author
    transformed_author: TransformedProfileViewBasicModel = transform_author_profile(
        author=raw_author
    )
    transformed_record: TransformedRecordModel = transform_post_record(
        record=post.post.record
    )
    res = {
        "metadata": metadata,
        "author": transformed_author,
        "cid": post.post.cid,
        "indexed_at": post.post.indexed_at,
        "record": transformed_record,
        "uri": post.post.uri,
        "like_count": post.post.like_count,
        "reply_count": post.post.reply_count,
        "repost_count": post.post.repost_count,
    }
    return TransformedFeedViewPostModel(**res)


def transform_feedview_posts(
    posts: list[FeedViewPost], enrichment_data: dict
) -> list[TransformedFeedViewPostModel]:
    return [
        transform_feedview_post(post=post, enrichment_data=enrichment_data)
        for post in posts
    ]
