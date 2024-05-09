# Using LLMs to classify if social media posts are political or not (Part V).

I'm working on a project that involves gathering social media posts from [Bluesky](https://bsky.app/) and analyzing them. Part of that project requires knowing which posts are about political or social topics, and if so, what political side they support. Current ML classifiers don't work that well out of the box, so I'm trying to create our own classification scheme using LLMs. I'm trying to use LLMs in order to classify [Bluesky](https://bsky.app/) posts as either having political content or not, and if so, the political ideology, and I've found that LLMs work quite well for this task. I've used Llama3-8b and Llama3-70b via [Groq](https://groq.com/) so far, but are also open to experimenting with other open-source models as well (I have the on-prem infrastructure to host our own models, which is much cheaper at scale).

[Previously](https://markptorres.com/research/llm-experiments-pt-i), I confirmed that LLMs are promising for our classification task. We now want to replicate this. We [previously](https://markptorres.com/research/llm-experiments-pt-iv) synced the data for annotation. Now we want to clean up our code for syncing the posts and doing quality checks using Pydantic.

## Defining our high-level ETL pipeline
At a high level, our ETL pipeline consists of the following steps:
- Extract the raw posts from the Bluesky API.
- Transform (and filter) the raw posts into a format useful for us.
- Loading the posts into both local storage and MongoDB.

We already went over extracting the raw posts [previously](https://markptorres.com/research/llm-experiments-pt-iv), so I'll focus on transforming and filtering the raw posts.

## Creating pydantic models
When we synced the data for annotation, we did not do any quality checks on the data itself. As a result, our downstream code will expect a certain schema and for certain values to exist, when that might not be true in the first place. Since Bluesky is ever-evolving, it's bad practice for us to expect a certain schema to always be true. Therefore, we want to create pydantic models in order to ensure that we get data in the exact format that we expect.

Our posts come in a `FeedViewPost` format from Bluesky, as defined [here](https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/defs.py#L62). We reliably know that the ingested data comes in this format. However, we want to transform this raw input data into a format that is useful for us downstream.

We'll create the following Pydantic models to help us with this (each is explained in context later on):

```python
"""Models for transformed fields."""
from typing import Optional
import typing_extensions as te

from pydantic import BaseModel, Field


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

class TransformedProfileViewBasicModel(BaseModel):
    """Model for the transformed profile view."""
    did: str = Field(..., description="The DID of the user.")
    handle: str = Field(..., description="The handle of the user.")
    avatar: Optional[str] = None
    display_name: Optional[str] = Field(
        default=None, max_length=640, description="Display name of the user."
    )
    py_type: te.Literal["app.bsky.actor.defs#profileViewBasic"] = Field(
        default="app.bsky.actor.defs#profileViewBasic", alias="$type", frozen=True # noqa
    )


class TransformedRecordModel(BaseModel):
    """Model for the transformed record."""
    created_at: str = Field(..., description="The timestamp of when the record was created on Bluesky.") # noqa
    text: str = Field(..., description="The text of the record.")
    embed: Optional[ProcessedEmbed] = Field(default=None, description="The embeds in the record, if any.") # noqa
    entities: Optional[str] = Field(default=None, description="The entities of the record, if any. Separated by a separator.") # noqa
    facets: Optional[str] = Field(default=None, description="The facets of the record, if any. Separated by a separator.") # noqa
    labels: Optional[str] = Field(default=None, description="The labels of the record, if any. Separated by a separator.") # noqa
    langs: Optional[str] = Field(default=None, description="The languages of the record, if specified.") # noqa
    reply_parent: Optional[str] = Field(default=None, description="The parent post that the record is responding to in the thread, if any.") # noqa
    reply_root: Optional[str] = Field(default=None, description="The root post of the thread, if any.") # noqa
    tags: Optional[str] = Field(default=None, description="The tags of the record, if any.") # noqa
    py_type: te.Literal["app.bsky.feed.post"] = Field(default="app.bsky.feed.post", frozen=True) # noqa


class TransformedFirehosePostModel(BaseModel):
    """Model for the transformed firehose post."""
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    author: str = Field(..., description="The DID of the author of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.") # noqa
    created_at: str = Field(..., description="The timestamp of when the record was created on Bluesky.") # noqa
    text: str = Field(..., description="The text of the record.")
    embed: Optional[ProcessedEmbed] = Field(default=None, description="The embeds in the record, if any.") # noqa
    entities: Optional[str] = Field(default=None, description="The entities of the record, if any. Separated by a separator.") # noqa
    facets: Optional[str] = Field(default=None, description="The facets of the record, if any. Separated by a separator.") # noqa
    labels: Optional[str] = Field(default=None, description="The labels of the record, if any. Separated by a separator.") # noqa
    langs: Optional[str] = Field(default=None, description="The languages of the record, if specified.") # noqa
    reply_parent: Optional[str] = Field(default=None, description="The parent post that the record is responding to in the thread, if any.") # noqa
    reply_root: Optional[str] = Field(default=None, description="The root post of the thread, if any.") # noqa
    tags: Optional[str] = Field(default=None, description="The tags of the record, if any.") # noqa
    py_type: te.Literal["app.bsky.feed.post"] = Field(default="app.bsky.feed.post", frozen=True) # noqa


class FeedViewPostMetadata(BaseModel):
    url: str = Field(..., description="The URL of the post.")
    source_feed: str = Field(..., description="The source feed of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.") # noqa


class TransformedFeedViewPostModel(BaseModel):
    metadata: FeedViewPostMetadata = Field(..., description="The metadata of the post.") # noqa
    author: TransformedProfileViewBasicModel = Field(..., description="The author of the post.") # noqa
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: str = Field(..., description="The timestamp of when the post was indexed by Bluesky.") # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.") # noqa
    uri: str = Field(..., description="The URI of the post.")
    like_count: Optional[int] = Field(default=None, description="The like count of the post.") # noqa
    reply_count: Optional[int] = Field(default=None, description="The reply count of the post.") # noqa
    repost_count: Optional[int] = Field(default=None, description="The repost count of the post.") # noqa
```

## Transforming the raw Bluesky posts
We can use these models along with some transformation code in order to transform the raw posts into a format useful for us.

### Preprocessing steps
We want to do the following preprocessing steps:
- Getting the author information that we care about
- Processing any embeds, entities, facets, labels, languages, and tags
- Adding metadata

We'll do each step individually.

#### Getting the author information that we care about
Each post contains an object that tells us information about the author of the post. We only need a subset of the information, enough to identify the author reliably, so let's fetch just that subset:

```python
class TransformedProfileViewBasicModel(BaseModel):
    """Model for the transformed profile view."""
    did: str = Field(..., description="The DID of the user.")
    handle: str = Field(..., description="The handle of the user.")
    avatar: Optional[str] = None
    display_name: Optional[str] = Field(
        default=None, max_length=640, description="Display name of the user."
    )
    py_type: te.Literal["app.bsky.actor.defs#profileViewBasic"] = Field(
        default="app.bsky.actor.defs#profileViewBasic", alias="$type", frozen=True # noqa
    )


def transform_author_profile(author: ProfileViewBasic) -> TransformedProfileViewBasicModel: # noqa
    res = {
        "did": author.did,
        "handle": author.handle,
        "avatar": author.avatar,
        "display_name": author.display_name,
        "py_type": author.py_type
    }
    return TransformedProfileViewBasicModel(**res)
```

#### Processing any embeds, entities, facets, labels, languages, and tags

##### Processing embeds
An "embed" in Bluesky is any embedded content. There are four types of embedded content:
- Media: any media attached to a post (currently just images, though this will change in the future to include things like videos and GIFs, as per the [Bluesky 2024 roadmap](https://docs.bsky.app/blog/2024-protocol-roadmap#product-features)).
- Record: a reference to another post on Bluesky.
- External: a reference to an external resource (e.g., a link, a Tweet, etc.).
- Record with Media Embed: a combination of both a media embed and a record embed (e.g., if a post references another Bluesky post and also attaches a picture).

We will need to process these separately:

###### Processing media
Currently, we're only concerned with image embeds (though we may want to support other formats like videos and GIFs). Because of that, we'll only process the images. We don't have anything like OCR yet to actually get the contents of an image, so we'll also only extract any alt text in the image. We'll join the alt text of multiple images together if there are multiple images.

```python
def process_images(image_embed: ImageEmbed) -> str:
    """Processes images

    For now, we just return the alt texts of the images, separated by ;
    (since , is likely used in the text itself).
    """
    return LIST_SEPARATOR_CHAR.join([process_image(image) for image in image_embed.images]) # noqa
```

###### Processing records
Records are posts that are embedded in other posts. For these, we'll just grab the URI and CID of the post and we'll hydrate the referenced post whenever we need to use it somehow.

```python
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
```

###### Processing external resources
Whenever someone links to an external resource (e.g., a link, a news article, a Tweet, etc.), they can choose to create an "embed card" that shows a preview of that link. This is how, for example, news links are rendered by default on many social media platforms, and why we can see the preview text of the articles without clicking into them. For these, we need to process both the URI (the link, in this case) of the external embed, as well as the title and the description of the embed card:

```python
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

def process_external_embed(external_embed: ExternalEmbed) -> ProcessedExternalEmbed: # noqa
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
        "uri": external.uri
    }
    return ProcessedExternalEmbed(**res)
```

###### Processing records with media embeds
Some posts both respond to another post on Bluesky and also attach images as well. For these, we just combine the processing that we did for records and for images.

```python
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


def process_record_with_media_embed(
    record_with_media_embed: RecordWithMediaEmbed
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
```

###### Putting all the embed processing together
Now, we can combine all of these individual steps into one function:

```python
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


def process_embed(
    embed: Union[ExternalEmbed, ImageEmbed, RecordEmbed, RecordWithMediaEmbed]
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

    if (
        isinstance(embed, ImageEmbed)
        or embed.py_type == "app.bsky.embed.images"
    ):
        res["has_image"] = True
        image_alt_text: str = process_images(embed)
        res["image_alt_text"] = image_alt_text

    if (
        isinstance(embed, RecordEmbed)
        or embed.py_type == "app.bsky.embed.record"
    ):
        res["has_embedded_record"] = True
        embedded_record: ProcessedRecordEmbed = process_record_embed(embed)
        res["embedded_record"] = embedded_record

    if (
        isinstance(embed, ExternalEmbed)
        or embed.py_type == "app.bsky.embed.external"
    ):
        res["has_external"] = True
        external_embed: ProcessedExternalEmbed = process_external_embed(embed)
        res["external"] = external_embed

    if (
        isinstance(embed, RecordWithMediaEmbed)
        or embed.py_type == "app.bsky.embed.recordWithMedia"
    ):
        res["has_image"] = True
        res["has_embedded_record"] = True
        processed_embed: ProcessedRecordWithMediaEmbed = process_record_with_media_embed(embed) # noqa
        image_alt_text: str = processed_embed.image_alt_text
        embedded_record: ProcessedRecordEmbed = processed_embed.embedded_record
        res["image_alt_text"] = image_alt_text
        res["embedded_record"] = embedded_record

    return ProcessedEmbed(**res)
```

##### Processing entities
Entities are a [deprecated](https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/post.py#L29) precursor to [Bluesky facets](https://docs.bsky.app/docs/advanced-guides/post-richtext), which handle rich text in posts (e.g., links). Our way of processing these is to just extract the value of the entity:

```python
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
    return LIST_SEPARATOR_CHAR.join([process_entity(entity) for entity in entities]) # noqa
```

##### Processing facets
[Bluesky facets](https://docs.bsky.app/docs/advanced-guides/post-richtext) are the Bluesky way of embedding rich text without using Markdown. One of the Bluesky core devs wrote a [writeup](https://www.pfrazee.com/blog/why-facets) explaining why Bluesky decided to go this route instead of using Markdown.

There are technically three types of facets:
1. Mention: mentions another Bluesky user (e.g., @jack)
2. Link: a link to a website
3. Hashtag: a hashtag

For these, we just extract the relevant value (for mentions, the DID of the referenced user, and then the values of the link and the hashtags):

```python
def process_mention(mention: Mention) -> str:
    """Processes a mention of another Bluesky user.

    See https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py
    """ # noqa
    return f"mention:{mention.did}"


def process_link(link: Link) -> str:
    """Processes a link. The URI here is the link itself.

    See https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py
    """ # noqa
    return f"link:{link.uri}"


def process_hashtag(tag: Tag) -> str:
    """Processes a hashtag. This is a tag that starts with a #, but the tag
    won't have a '#' in the value. (e.g., if the hashtag is #red, the tag value
    would be 'red').

    See https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py
    """ # noqa
    return f"tag:{tag.tag}"
```

We then process these like we do entities, by grabbing the values of the facets:

```python
def process_facet(facet: Facet) -> str:
    """Processes a facet.

    A facet is a richtext element. This is Bluesky's way of not having to
    explicitly support Markdown.

    See the following:
    - https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/richtext/facet.py#L64
    - https://www.pfrazee.com/blog/why-facets
    """
    features: list = facet.features
    features_list = []

    for feature in features:
        if (
            isinstance(feature, Tag)
            or get_object_type_str(feature) == "app.bsky.richtext.facet#tag"
        ):
            features_list.append(process_hashtag(feature))
        elif (
            isinstance(feature, Link)
            or get_object_type_str(feature) == "app.bsky.richtext.facet#link"
        ):
            features_list.append(process_link(feature))
        elif (
            isinstance(feature, Mention)
            or get_object_type_str(feature) == "app.bsky.richtext.facet#mention" # noqa
        ):
            features_list.append(process_mention(feature))
        else:
            object_type = get_object_type_str(feature)
            raise ValueError(f"Unknown feature type: {object_type}")
    return LIST_SEPARATOR_CHAR.join(features_list)
```

##### Processing labels
Labels can be added to posts (either by other users or the author themselves) to mark posts as having certain context. For example, there are communnities (e.g,. sex workers, furries, etc.) who use Bluesky and have their own dedicated communities, but they mark the posts so that other users who don't want to see their content can simply filter out content with the related labels. For these, we just extract the label text:

```python
def process_label(label: SelfLabel) -> str:
    """Processes a single label.

    Example:
    SelfLabel(val='porn', py_type='com.atproto.label.defs#selfLabel'

    Returns a single label.
    """ # noqa
    return label.val


def process_labels(labels: Optional[SelfLabels]) -> Optional[str]:
    """Processes labels.

    Example:
    SelfLabels(
        values=[SelfLabel(val='porn', py_type='com.atproto.label.defs#selfLabel')],
        py_type='com.atproto.label.defs#selfLabels'
    )

    Based off https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/label/defs.py#L38
    """ # noqa
    if not labels:
        return None
    label_values: list[SelfLabel] = labels.values
    return LIST_SEPARATOR_CHAR.join([process_label(label) for label in label_values]) # noqa
```

##### Processing languages
We can also process the language of the post itself. The language for posts hasn't always been available, although a recent [PR](https://github.com/bluesky-social/atproto/pull/2301) now automatically classifies the language of a post, if any. We extract these values and return a string:

```python
def process_langs(langs: Optional[list[str]]) -> Optional[str]:
    """Processes languages.

    Example:
    ['ja']
    """  # noqa
    if not langs:
        return None
    return LIST_SEPARATOR_CHAR.join(langs)
```

##### Processing tags
Tags serve a similar purpose to labels, and we can process them similarly:

```python
def process_tags(tags: Optional[list[str]]) -> Optional[str]:
    """Processes tags.

    Example:
    ['furry', 'furryart']
    """  # noqa
    if not tags:
        return None
    return LIST_SEPARATOR_CHAR.join(tags)
```

#### Adding metadata
Each time that we sync, we want to store some metadata. We want to store information such as when we synced the data and where we fetched it from. We can define a function, `get_feedviewpost_metadata`, and a corresponding pydantic model, `FeedViewPostMetadata`, to generate said metadata:

```python
class FeedViewPostMetadata(BaseModel):
    url: str = Field(..., description="The URL of the post.")
    source_feed: str = Field(..., description="The source feed of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.") # noqa


def get_feedviewpost_metadata(
    post: FeedViewPost, enrichment_data: dict
) -> FeedViewPostMetadata:
    metadata = {}
    handle = post.post.author.handle
    uri = post.post.uri.split("/")[-1]
    metadata["url"] = f"https://bsky.app/profile/{handle}/post/{uri}"
    metadata["source_feed"] = enrichment_data["source_feed"]
    metadata["feed_url"] = enrichment_data["feed_url"]
    metadata["synctimestamp"] = current_datetime_str
    return metadata
```

### Putting it all together
Now, combining all of these preprocessing steps, we now can create a function, `transform_feedview_post`, that takes the raw post and transforms it into the format that we care about:

```python
def transform_feedview_post(
    post: FeedViewPost, enrichment_data: dict
) -> TransformedFeedViewPostModel:
    """Transforms a feed view post."""
    metadata: FeedViewPostMetadata = get_feedviewpost_metadata(
        post=post, enrichment_data=enrichment_data
    )
    raw_author: ProfileViewBasic = post.post.author
    transformed_author: TransformedProfileViewBasicModel = (
        transform_author_profile(author=raw_author)
    )
    transformed_record: TransformedRecordModel = (
        transform_post_record(record=post.post.record)
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
```

This now gives us the tooling needing to transform posts into a format useful for us.

## Transforming the posts upon ingestion
Now that we have functions to perform the transformations on the raw data, we can bypass saving the raw outputs of the API call and just work with transformed versions of the posts:

```python
def get_and_transform_latest_most_liked_posts() -> list[TransformedFeedViewPostModel]:
    """Get the latest batch of most liked posts and transform them."""
    res: list[TransformedFeedViewPostModel] = []
    for feed in ["today", "week"]:
        feed_url = feed_to_info_map[feed]["url"]
        enrichment_data = {
            "source_feed": feed, "feed_url": feed_url
        }
        print(f"Getting most liked posts from {feed} feed with URL={feed_url}")
        posts: list[FeedViewPost] = get_posts_from_custom_feed_url(
            feed_url=feed_url, limit=None
        )
        transformed_posts: list[TransformedFeedViewPostModel] = (
            transform_feedview_posts(
                posts=posts, enrichment_data=enrichment_data
            )
        )
        res.extend(transformed_posts)
        print(f"Finished processing {len(posts)} posts from {feed} feed")
    return res
```

## Filtering undesired posts
Now that we can get and transform the posts, we now want to pass these posts through a filtering step. Right now, we only need to do language detection, though later on we might add more filters (such as spam filters, hate speech filters, etc.).

### Exploring various options for language detection
I benchmarked a few options for language detection. I used a previous set of ~50,000 posts in order to benchmark how the models do at scale:

#### Option 1: Langdetect
[Langdetect](https://github.com/Mimino666/langdetect) is a Python package (ported from [Java](https://www.slideshare.net/shuyo/language-detection-library-for-java)). It powers spacy-langdetect and is also commonly used in language detection tasks.


```python
from langdetect import detect
from langdetect.detector import LangDetectException

def text_is_english_langdetect(text):
    return detect(text) == "en"

>>> detect("This is an example post")
'en'
```

If we run this, we get the following results:
```python
def clf_post_langdetect(post: dict) -> dict:
    """Classify if a post is in English using the langdetect library."""
    try:
        label = text_is_english_langdetect(post["text"])
    except LangDetectException as e:
        # if unable to detect language, classify as False by default.
        label = False
    return {
        "id": post["id"],
        "uri": post["uri"],
        "text": post["text"],
        "is_english_label": label,
    }


langdetect_labels: list[dict] = classify_posts(
    posts=preprocessed_posts, clf_func=clf_post_langdetect,
    batch_size=BATCH_SIZE, rate_limit_per_minute=None
)
```

```plaintext
Execution time for classify_posts: 0 minutes, 58 seconds
Memory usage for classify_posts: 79.4375 MB
```

`langdetect` was really inefficient - it took 58 seconds and used ~80MB of memory.

#### Option 2: Langid
langid is a Python package designed specifically for language detection. According to the [docs](https://github.com/saffsd/langid.py), it's supposed to be fast, minimalistic, pre-trained, and not sensitive to domain-specific features (like markup text).

```python
import langid

def text_is_english_langid(text):
    return langid.classify(text)[0] == "en"


def clf_post_langid(post: dict) -> dict:
    """Classify if a post is in English using the langid library."""
    return {
        "id": post["id"],
        "uri": post["uri"],
        "text": post["text"],
        "is_english_label": text_is_english_langid(post["text"]),
    }


langid_labels: list[dict] = classify_posts(
    posts=preprocessed_posts, clf_func=clf_post_langid,
    batch_size=BATCH_SIZE, rate_limit_per_minute=None
)
```

```plaintext
Execution time for classify_posts: 2 minutes, 36 seconds
Memory usage for classify_posts: 53.046875 MB
```

It took 2 minutes and 36 seconds (156 seconds) to classify 50,000 posts. Used >50MB of memory. The `langid` model was more inefficient than `langdetect`, so we'll see if there's something faster.

### Option 3: fasttext
`fasttext` is a [package](https://github.com/facebookresearch/fastText) developed at Facebook for fast, scalable word representation and language learning. They have a specific fine-tuned version, [fasttext-language-identification](https://huggingface.co/facebook/fasttext-language-identification) used for language detection.

There are two ways to use fasttext
#### Option 3.1 fasttext via Hugging Face
We can download the model from the Hugging Face Hub.

```python
import fasttext
from huggingface_hub import hf_hub_download

model_path = hf_hub_download(repo_id="facebook/fasttext-language-identification", filename="model.bin")
model: fasttext.FastText._FastText = fasttext.load_model(model_path)

def text_is_english_hf_fasttext(text):
    return model.predict(text)[0][0] == "__label__eng_Latn"

def clf_post_hf_fasttext(post: dict) -> dict:
    """Classify if a post is in English using the fasttext model from
    Hugging Face Hub."""
    return {
        "id": post["id"],
        "uri": post["uri"],
        "text": post["text"],
        "is_english_label": text_is_english_hf_fasttext(post["text"]),
    }

hf_fasttext_labels: list[dict] = classify_posts(
    posts=preprocessed_posts, clf_func=clf_post_hf_fasttext,
    batch_size=BATCH_SIZE, rate_limit_per_minute=None
)
```

```plaintext
Execution time for classify_posts: 0 minutes, 4 seconds
Memory usage for classify_posts: 8.4375 MB
```

Notably, on retries, the classification is much faster. That's because a majority of the execution time (~3 seconds) is network-related and due to us loading the model from Hugging Face. When we retry, the model is already cached, so it performs just as fast as running with a local binary (see below).

#### Option 3.2 fasttext via local binary
We can download the binary classifier model [here](https://fasttext.cc/docs/en/language-identification.html) and load it for inference.

```python
# need to download the model; this is >100MB which is OK for local storage
# but too large for Github (unless we use Github LFS).
fasttext_model_bin = fasttext.load_model('lid.176.bin')

def text_is_english_local_fasttext(text):
    return fasttext_model_bin.predict(text)[0][0] == "__label__eng_Latn"

def clf_post_local_fasttext(post: dict) -> dict:
    """Classify if a post is in English using a local binary of the fasttext
    model."""
    return {
        "id": post["id"],
        "uri": post["uri"],
        "text": post["text"],
        "is_english_label": text_is_english_local_fasttext(post["text"]),
    }

local_fasttext_labels: list[dict] = classify_posts(
    posts=preprocessed_posts, clf_func=clf_post_local_fasttext,
    batch_size=BATCH_SIZE, rate_limit_per_minute=None
)
```

The `fasttext` model is by far the fastest out of all the ones that I tried. Given that this was developed at Facebook and that they would need to massively classify language at scale, the `fasttext` model being fastest doesn't surprise me.

### What I ended up going with
For our use case, language detection is actually made relatively simple, courtesy of the [PR](https://github.com/bluesky-social/atproto/pull/2301) to automatically add language detection. We can just filter on this field now in our models. Just in case there are some that haven't been classified (this can be the case with older posts), we can default to using our `fasttext` detection (though this should be a very rare occurrence, if at all; I encountered it in ~15-20 posts out of 2,000 in my latest sync).

```python
def record_is_english(record: TransformedRecordModel) -> bool:
    langs: str = record.langs
    if langs:
        return "en" in langs
    return classify(record.text)
```

I had done the benchmarks prior to this PR going in, and had built tooling to support fast language detection (it takes <5 seconds for ~12,000 posts). However, since now language detection is done upstream in atproto, we don't need to do language detection ourselves.

## Putting it all together
We can now put together our ETL pipeline:

```python
def post_passed_filters(post: TransformedFeedViewPostModel) -> bool:
    record: TransformedRecordModel = post.record
    return record_is_english(record=record)


def filter_posts(
    posts: list[TransformedFeedViewPostModel]
) -> list[TransformedFeedViewPostModel]:
    """Filter the posts."""
    return list(filter(post_passed_filters, posts))


def get_and_transform_latest_most_liked_posts() -> list[TransformedFeedViewPostModel]:
    """Get the latest batch of most liked posts and transform them."""
    res: list[TransformedFeedViewPostModel] = []
    for feed in ["today", "week"]:
        feed_url = feed_to_info_map[feed]["url"]
        enrichment_data = {
            "source_feed": feed, "feed_url": feed_url
        }
        print(f"Getting most liked posts from {feed} feed with URL={feed_url}")
        posts: list[FeedViewPost] = get_posts_from_custom_feed_url(
            feed_url=feed_url, limit=None
        )
        transformed_posts: list[TransformedFeedViewPostModel] = (
            transform_feedview_posts(
                posts=posts, enrichment_data=enrichment_data
            )
        )
        res.extend(transformed_posts)
        print(f"Finished processing {len(posts)} posts from {feed} feed")
    return res


def export_posts(
    posts: list[TransformedFeedViewPostModel],
    store_local: bool = True,
    store_remote: bool = True,
    bulk_write_remote: bool = True,
    bulk_chunk_size: int = 100
) -> None:
    """Export the posts to a file, either locally as a JSON or remote in a
    MongoDB collection."""
    if store_local:
        print(f"Writing {len(posts)} posts to {sync_fp}")
        with open(sync_fp, "w") as f:
            for post in posts:
                post_json = json.dumps(post)
                f.write(post_json + "\n")
        num_posts = len(posts)
        print(f"Wrote {num_posts} posts locally to {sync_fp}")

    if store_remote:
        duplicate_key_count = 0
        total_successful_inserts = 0
        total_posts = len(posts)
        print(f"Inserting {total_posts} posts to MongoDB collection {mongo_collection_name}")  # noqa
        formatted_posts_mongodb = [
            {"_id": post["uri"], **post}
            for post in posts
        ]
        if bulk_write_remote:
            print("Inserting into MongoDB in bulk...")
            total_successful_inserts, duplicate_key_count = chunk_insert_posts(
                posts=formatted_posts_mongodb,
                mongo_collection=mongo_collection,
                chunk_size=bulk_chunk_size
            )
            print("Finished bulk inserting into MongoDB.")
        else:
            for idx, post in enumerate(posts):
                if idx % 100 == 0:
                    print(f"Inserted {idx}/{total_posts} posts")
                try:
                    post_uri = post["uri"]
                    # set the URI as the primary key.
                    # NOTE: if this doesn't work, check if the IP address has
                    # permission to access the database.
                    mongo_collection.insert_one(
                        {"_id": post_uri, **post},
                    )
                    total_successful_inserts += 1
                except DuplicateKeyError:
                    duplicate_key_count += 1
        if duplicate_key_count > 0:
            print(f"Skipped {duplicate_key_count} duplicate posts")
        print(f"Inserted {total_successful_inserts} posts to remote MongoDB collection {mongo_collection_name}")  # noqa

posts: list[TransformedFeedViewPostModel] = (
    get_and_transform_latest_most_liked_posts()
)
filtered_posts = filter_most_liked_posts(posts=posts)
post_dicts = [post.dict() for post in filtered_posts]
export_posts(
    posts=post_dicts, store_local=True, store_remote=True,
    bulk_write_remote=True
)
```

We get the following counts before and after filtering:

```plaintext
(Pdb) len(transformed_posts)
1984
(Pdb) len(filtered_posts)
1595
```

So, this appears to have removed ~400 (~20\%) non-English posts.

## Next steps
Now that we've cleaned up the code for loading posts and have more confidence in the robustness of our ETL pipeline, I want to be able to classify them at scale. Then, I'd like to go back to other improvements for the process. What I'd like to do is:
- Updating and refactoring the LLM pipeline to label the posts efficiently at scale.
- How does our model perform with other LLMs (e.g., Mixtral)?
- Can we experiment with optimizing the prompt (e.g, with [dspy](https://github.com/stanfordnlp/dspy))?

I'd also like to revisit some of the points related to improving how to add context about current events:
- For determining when to get context for a post, investigate various strategies such as:
    - Keyword matching: see if a keyword (e.g., a name of an event) comes up. Need to figure out keywords that describe topics that are in the news (this is easiest if it is the name of a notable event, place, person, piece of legislature, etc.) and then we can easily pattern match that against posts that have that keyword.
    - Posts that the LLM knows is political but isn't sure what the political ideology is.
- Determine how to format the context that's given to the LLM prompt.
    - An interesting frame could be first asking the LLM to distill the sentiments and thoughts of each political party about a certain topic, based on the articles that we have for each topic, and then passing this distilled summary to the LLM itself.
- Only insert into the vector store if it doesn’t already exist there.
- At some point, add a maximum distance measure so we get only relevant articles (will take some experimentation in order to see what a good distance is).
