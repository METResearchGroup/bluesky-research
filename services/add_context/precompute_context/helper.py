"""Helper functions for adding context to posts."""
from typing import Optional

from atproto_client.models.app.bsky.feed.post import GetRecordResponse

from lib.constants import current_datetime_str
from lib.db.bluesky_models.embed import (
    EmbeddedContextContextModel,
    ExternalEmbedContextModel,
    ImagesContextModel,
    RecordContextModel,
    RecordWithMediaContextModel,
)
from lib.db.bluesky_models.transformations import (
    TransformedRecordWithAuthorModel
)
from services.add_context.precompute_context.models import (
    AuthorContextModel,
    ContextEmbedUrlModel,
    ContextUrlInTextModel,
    FullPostContextModel,
    PostLinkedUrlsContextModel,
    PostTagsLabelsContextModel,
    ThreadContextModel,
    ThreadPostContextModel
)
from services.add_context.current_events_enrichment.bsky_news_orgs import bsky_did_to_news_org_name  # noqa
from services.add_context.current_events_enrichment.newsapi_context import url_is_to_news_domain  # noqa
from services.add_context.precompute_context.database import (
    get_post_context, insert_post_context
)
from services.sync.stream.database import get_record_as_dict_by_uri, insert_new_record  # noqa
from transform.bluesky_helper import get_record_with_author_given_post_uri
from transform.transform_raw_data import LIST_SEPARATOR_CHAR

embedded_content_type_to_preamble_map = {
    "record": "The post references another post with the following details:",
    "external": "The post contains a external link to content with the following details:",  # noqa
    "images": "The post contains the following alt text for its images:",
    "labels": "The post contains the following labels:",
    "tags": "The post contains the following tags:",
    "linked_urls": "The post links to external URLs:",
    "parent_reply": "The post is a reply to another post with the following details:",  # noqa
    "parent_root": "The post is part of a thread that starts with the following post:",  # noqa
    "post_author": "We have context about the post's author:"
}


def process_images_embed(embed_dict: dict) -> ImagesContextModel:
    """Processes image information and adds it to the prompt.

    Currently, we only can get info about images from its alt text,
    and we combine all the alt text across images together, so we
    combine the alt text together and return the results (which is a
    step that is done during preprocessing).
    """
    return ImagesContextModel(image_alt_texts=embed_dict["image_alt_text"])


def process_record_context(record_uri: str) -> RecordContextModel:
    """Processes the context for any records that are referenced in a post.

    This includes posts that are being referenced by another post as well as
    posts that are part of the thread that a post is a part of.

    For now we'll just return the text and the image of these posts.

    Doesn't recursively process embedded records (e.g., if the post references
    another post that references another post, etc., the processing only goes
    down 2 layers deep). We enforce this by getting only the text and image
    of the embedded record, not if the embedded record has its own embedded
    record within it.
    """
    if not record_uri:
        return RecordContextModel()
    # TODO: check what type the value is and then see if it's one
    # that exists already in the DB.
    # TODO: I should move all of the DBs to a single location, and then
    # just change the name of the DB based on the input type (e.g., FeedViewPost)
    # or the source type (e.g., firehose, feedview, context, etc.)
    record: dict = get_or_fetch_embedded_record_with_author(record_uri)
    text = record.text
    embed_dict = record.embed
    embed_image_alt_text = None
    if embed_dict:
        if embed_dict["has_image"]:
            embed_image_alt_text: ImagesContextModel = (
                process_images_embed(embed_dict)
            )
    return RecordContextModel(
        text=text, embed_image_alt_text=embed_image_alt_text
    )


def get_or_fetch_embedded_record_with_author(
    record_uri: str, insert_new_record_bool: bool = True
) -> dict:
    """Fetches the record associated with a record URI from the DB or, if it
    doesn't exist, fetches it from the Bluesky API and then stores it in DB.
    """
    record_dict: dict = get_record_as_dict_by_uri(record_uri)
    if record_dict:
        print(f"Fetched record with URI {record_uri} from DB.")
        record_with_author: TransformedRecordWithAuthorModel = (
            TransformedRecordWithAuthorModel(**record_dict)
        )
    else:
        print(f"Fetching record with URI {record_uri} from Bluesky API.")
        record_with_author: TransformedRecordWithAuthorModel = (
            get_record_with_author_given_post_uri(record_uri)
        )
        if insert_new_record_bool:
            insert_new_record(record_with_author)
    return record_with_author.dict()


def process_record_embed(embed_dict: dict) -> RecordContextModel:
    """Processes record embeds.

    Record embeds are posts that are embedded in other posts. This is a way to
    reference another post in a post.

    For any records embedded in another post, we want to give enough
    information to tell the model about the original post and content that the
    current post is referencing.
    """
    embedded_record_dict: dict = embed_dict["embedded_record"]
    embedded_record_uri: str = embedded_record_dict["uri"]
    # for now let's just add a subset of the fields from the embedded
    # record to the context, since if we add too many fields, the model
    # might confuse which information refers to the embedded record vs
    # the post that needs to be classified. We can also add other fields
    # as context if we want to, but I think that the fields that people
    # see when they see a post being replied to are the text of that
    # post and any images included.
    return process_record_context(record_uri=embedded_record_uri)


def process_external_embed(embed_dict: dict) -> ExternalEmbedContextModel:
    """Processes an "external" embed, which is some externally linked content
    plus its preview card.

    External embeds are links to external content, like a YouTube video or a
    news article, which also has a preview card showing its content.
    """
    external_content: dict = embed_dict["external"]
    title: str = external_content["title"]
    description: str = external_content["description"]
    return ExternalEmbedContextModel(title=title, description=description)


def process_record_embed_with_media_embed(embed_dict: dict) -> RecordWithMediaContextModel:  # noqa
    """Processes a record with a media embed.

    This refers to the scenario where a post both has an image(s) attached
    and also refers to another post. In this case, we want to give context
    on both the image(s) and the embedded record.

    This returns a dict with the image and the embedded record context
    """
    images_context: ImagesContextModel = process_images_embed(embed_dict)
    embedded_record_context: RecordContextModel = process_record_embed(embed_dict)  # noqa
    return RecordWithMediaContextModel(
        images_context=images_context,
        embedded_record_context=embedded_record_context
    )


def embedded_content_context(post: dict) -> EmbeddedContextContextModel:
    """Context of the images and attachments of a post, if any."""
    embed_dict: dict = post["embed"]
    if not embed_dict:
        return EmbeddedContextContextModel(has_embedded_content=False)
    if embed_dict["has_embedded_record"]:
        # records can either be plain records or be records that also reference
        # images as well.
        if embed_dict["has_image"]:
            # these have both images and embedded context, so we need to grab
            # both of these.
            context: RecordWithMediaContextModel = (
                process_record_embed_with_media_embed(embed_dict)
            )
            content_type = "record_with_media"
        else:
            context: RecordContextModel = process_record_embed(embed_dict)
            content_type = "record"
    elif embed_dict["has_image"]:
        context: ImagesContextModel = process_images_embed(embed_dict)
        content_type = "images"
    elif embed_dict["has_external"]:
        context: ExternalEmbedContextModel = process_external_embed(embed_dict)
        content_type = "external_embed"
    else:
        return EmbeddedContextContextModel(has_embedded_content=False)
    return EmbeddedContextContextModel(
        has_embedded_content=True,
        embedded_content_type=content_type,
        embedded_record_with_media_context=context
    )


def get_parent_reply_context(parent_reply_uri: str) -> ThreadPostContextModel:
    """Gets the context of a post that a given post is replying to (the
    post that is being replied to is the 'parent_reply').
    """
    record_context: RecordContextModel = process_record_context(parent_reply_uri)  # noqa
    text: Optional[str] = record_context.text
    embed_image_alt_text: Optional[ImagesContextModel] = (
        record_context.embed_image_alt_text.image_alt_texts
        if record_context.embed_image_alt_text
        else None
    )
    return ThreadPostContextModel(
        text=text, embedded_image_alt_text=embed_image_alt_text
    )


def get_parent_root_context(parent_root_uri: str) -> ThreadPostContextModel:
    """Gets the context of the root post in a thread that a given post is
    a part of (the first post in a thread is the "parent root").
    """
    record_context = process_record_context(parent_root_uri)
    text = record_context["text"]
    embed_image_alt_text = record_context["embed_image_alt_text"]
    return ThreadPostContextModel(
        text=text, embedded_image_alt_text=embed_image_alt_text
    )


def define_thread_context(post: dict) -> ThreadContextModel:
    """Context of a thread that the post is a part of, if any."""
    # check if there is a reply parent or a reply root, and then check
    # if that post's URI exists in our filtered DB.
    reply_parent: str = post["reply_parent"]
    reply_root: str = post["reply_root"]
    parent_reply_context: ThreadPostContextModel = (
        get_parent_reply_context(reply_parent)
    )
    # we can do this check this way because the existence of a reply root
    # implies the existence of a reply parent, so we only need to check for
    # the existence of a reply root if it's not the same as the reply parent.
    parent_root_context: ThreadPostContextModel = (
        get_parent_root_context(reply_root)
        if reply_root and reply_root != reply_parent
        else parent_reply_context
    )

    return ThreadContextModel(
        thread_root_post=parent_root_context,
        thread_parent_post=parent_reply_context
    )


def context_embed_url(post: dict) -> ContextEmbedUrlModel:
    """Adds context for URLs that are added as embeds in the post.

    These are for links that are embeds in the post, not just linked to in
    the text of the post.

    Example: https://bsky.app/profile/parismarx.bsky.social/post/3kpvlrkcr6m2q
    """
    embed_dict: dict = post["embed"]
    if not embed_dict:
        return ContextEmbedUrlModel()
    url = ""
    if embed_dict["has_external"]:
        external_content: dict = embed_dict["external"]
        url = external_content["uri"]
    is_link_to_trustworthy_news_article = (
        url_is_to_news_domain(url) if url else False
    )
    return ContextEmbedUrlModel(
        url=url,
        is_trustworthy_news_article=is_link_to_trustworthy_news_article
    )


def context_url_in_text(post: dict) -> ContextUrlInTextModel:
    """Adds context for URLs that are linked to in the text of the post.

    URLs are included in the text of the post as "facets", as this is how
    the Bluesky platform does links in lieu of markdown.

    Example: https://bsky.app/profile/donmoyn.bsky.social/post/3kqfra54rt52z
    """
    facets_str: str = post["facets"]
    links: list[str] = []
    if facets_str:
        facets: list[str] = facets_str.split(LIST_SEPARATOR_CHAR)
        for facet in facets:
            if facet.startswith("link:"):
                link = facet.replace("link:", "")
                links.append(link)
    if not links:
        return ContextUrlInTextModel()

    return ContextUrlInTextModel(
        has_trustworthy_news_links=any(
            [url_is_to_news_domain(link) for link in links]
        )
    )


def post_linked_urls(post: dict) -> PostLinkedUrlsContextModel:
    """Context if the post refers to any URLs in the text."""
    url_in_text_context: ContextUrlInTextModel = context_url_in_text(post)
    embed_url_context: ContextEmbedUrlModel = context_embed_url(post)

    return PostLinkedUrlsContextModel(
        url_in_text_context=url_in_text_context,
        embed_url_context=embed_url_context
    )


def post_tags_labels(post: dict) -> PostTagsLabelsContextModel:
    """Context based on tags/labels provided about the post, if any."""
    tags = []
    if post["tags"]:
        tags: list[str] = post["tags"].split(LIST_SEPARATOR_CHAR)
    labels = []
    if post["labels"]:
        labels: list[str] = post["labels"].split(LIST_SEPARATOR_CHAR)

    return PostTagsLabelsContextModel(
        post_tags=','.join(tags), post_labels=','.join(labels)
    )


def additional_current_events_context(post: dict) -> dict:
    """Additional context based on the post if it references
    something related to current events (which likely isn't in the training
    set of the model).
    """
    return {}


def post_author_context(post: dict) -> AuthorContextModel:
    """Returns contextual information about the post's author.

    For now, we just return if the post author is a news org, but we can add to
    this later on.
    """
    return AuthorContextModel(
        post_author_is_reputable_news_org=(
            post["author"] in bsky_did_to_news_org_name
        )
    )


post_context_and_funcs = [
    ("content_referenced_in_post", embedded_content_context),
    ("urls_in_post", post_linked_urls),
    ("post_thread", define_thread_context),
    ("post_tags_labels", post_tags_labels),
    # ("current_events_context", additional_current_events_context),
    ("post_author_context", post_author_context)
]


def generate_context_details_list(post: dict) -> list[tuple]:
    """Generates a list of tuples of (context_type, context_details) for a
    post."""
    context_details_list = []
    for context_name, context_func in post_context_and_funcs:
        context = context_func(post)
        if context:
            context_details_list.append((context_name, context))
    return context_details_list


def generate_post_context(post: dict) -> FullPostContextModel:
    """Creates context of the post."""
    context_details_list: list[tuple] = generate_context_details_list(post)
    context_dict = {
        # convert each Pydantic model to dict
        context_type: context_details.dict()
        for (context_type, context_details) in context_details_list
    }
    full_context_dict = {
        "uri": post["uri"],
        "text": post["text"],
        "context": context_dict,
        "timestamp": current_datetime_str
    }
    return FullPostContextModel(**full_context_dict)


def get_or_create_post_and_context_json(post: dict) -> dict:
    """Gets or creates the context for a post."""
    uri: str = post["uri"]
    post_context: Optional[FullPostContextModel] = get_post_context(uri=uri)
    if not post_context:
        print(f"Creating context for post {uri}")
        post_context: FullPostContextModel = generate_post_context(post)
        insert_post_context(post_context)
    return post_context.dict()
