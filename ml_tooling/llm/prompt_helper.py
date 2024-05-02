"""Helper tools for generating prompts.

Resource for all things prompting: https://github.com/dair-ai/Prompt-Engineering-Guide?tab=readme-ov-file
"""  # noqa
import json
from pprint import pformat
from typing import Optional

from ml_tooling.llm.models import (
    AuthorContextModel,
    ContextEmbedUrlModel,
    ContextUrlInTextModel,
    EmbeddedContextContextModel,
    ExternalEmbedContextModel,
    ImagesContextModel,
    PostLinkedUrlsContextModel,
    PostTagsLabelsContextModel,
    RecordContextModel,
    RecordWithMediaContextModel,
    ThreadContextModel,
    ThreadPostContextModel
)
from ml_tooling.llm.task_prompts import task_name_to_task_prompt_map
from services.add_context.current_events_enrichment.bsky_news_orgs import bsky_did_to_news_org_name  # noqa
from services.add_context.current_events_enrichment.newsapi_context import url_is_to_news_domain  # noqa
from transform.bluesky_helper import convert_post_link_to_post, get_post_record_given_post_uri  # noqa
from transform.transform_raw_data import flatten_firehose_post, LIST_SEPARATOR_CHAR  # noqa


base_prompt = """
{task_prompt}

Here is the post text that needs to be classified:
```
<text>
{post_text}
```

{context}
"""

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
    record = get_post_record_given_post_uri(record_uri)
    hydrated_embedded_record_dict: dict = {
        "record": record.value,
        "uri": "",
        "cid": "",
        "author": ""
    }
    flattened_firehose_post: dict = (
        flatten_firehose_post(hydrated_embedded_record_dict)
    )
    text = flattened_firehose_post["text"]
    embed = flattened_firehose_post["embed"]
    embed_image_alt_text = None
    if embed:
        embed_dict = json.loads(embed)
        if embed_dict["has_image"]:
            embed_image_alt_text: ImagesContextModel = (
                process_images_embed(embed_dict)
            )
    return RecordContextModel(
        text=text, embed_image_alt_text=embed_image_alt_text
    )


def process_record_embed(embed_dict: dict) -> RecordContextModel:
    """Processes record embeds.

    Record embeds are posts that are embedded in other posts. This is a way to
    reference another post in a post.

    For any records embedded in another post, we want to give enough
    information to tell the model about the original post and content that the
    current post is referencing.
    """
    embedded_record_dict: dict = json.loads(embed_dict["embedded_record"])
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
    external_content_json: str = embed_dict["external"]
    external_content: dict = json.loads(external_content_json)
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
    embed: str = post["embed"]
    if not embed:
        return EmbeddedContextContextModel(has_embedded_content=False)
    embed_dict: dict = json.loads(embed)
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
        raise ValueError("Unknown embedded content type.")
    return EmbeddedContextContextModel(
        has_embedded_content=True,
        embedded_content_type=content_type,
        embedded_record_with_media_context=context
    )


def get_parent_reply_context(parent_reply_uri: str) -> ThreadPostContextModel:
    """Gets the context of a post that a given post is replying to (the
    post that is being replied to is the 'parent_reply').
    """
    record_context: RecordContextModel = process_record_context(parent_reply_uri)
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
    embed: str = post["embed"]
    if not embed:
        return ContextEmbedUrlModel()
    embed_dict = json.loads(embed)
    url = ""
    if embed_dict["has_external"]:
        external_content_json = embed_dict["external"]
        external_content: dict = json.loads(external_content_json)
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


def generate_post_and_context_json(post: dict) -> dict:
    """Creates a JSON object with the post and its context.

    The JSON object has the following format:
    {
        "post": {
            "text": "The text of the post"
        },
        "context": {
            "context_type": "context_details"
        }
    }
    """
    context_details_list: list[tuple] = generate_context_details_list(post)
    context_dict = {
        # convert each Pydantic model to dict
        context_type: context_details.dict()
        for (context_type, context_details) in context_details_list
    }
    return {
        "text": post["text"],
        "context": context_dict
    }


def generate_context_string(
    post: dict,
    justify_result: bool = False,
    only_json_format: bool = False,
    pformat_json: bool = True
) -> str:
    """Given a list of (context_type, context_details) tuples, generate
    the context string for the prompt."""
    json_context: dict = generate_post_and_context_json(post)
    if pformat_json:
        json_context = pformat(json_context, width=200)
    full_context = f"""
The following JSON object contains the post and its context:
```
{json_context}
```
"""
    if justify_result:
        full_context += "\nAfter giving your label, start a new line and then justify your answer in 1 sentence."  # noqa
    else:
        full_context += "\nJustifications are not necessary."
    if only_json_format:
        full_context += "\nReturn ONLY the JSON. I will parse the string result in JSON format."
    return full_context


def generate_complete_prompt(
    post: dict,
    task_prompt: str,
    is_text_only: bool = False,
    justify_result: bool = False,
    only_json_format: bool = False
) -> str:
    """Given a task prompt and the details of the context, generate
    the resulting complete prompt.
    """
    post_text = post["text"]
    if is_text_only:
        full_context = ""
    else:
        full_context = generate_context_string(
            post=post,
            justify_result=justify_result,
            only_json_format=only_json_format
        )
    return base_prompt.format(
        task_prompt=task_prompt, post_text=post_text, context=full_context
    )


def generate_complete_prompt_for_given_post(
    post: dict,
    task_name: str,
    include_context: bool = True,
    justify_result: bool = False,
    only_json_format: bool = False
) -> str:
    """Generates a complete prompt for a given post."""
    is_text_only = not include_context
    return generate_complete_prompt(
        post=post,
        task_prompt=task_name_to_task_prompt_map[task_name],
        is_text_only=is_text_only,
        justify_result=justify_result,
        only_json_format=only_json_format
    )


def generate_complete_prompt_for_post_link(
    link: str,
    task_name: str,
    only_json_format: bool = False,
    include_context: bool = True
) -> str:
    """Generates a complete prompt for a given post link."""
    post: dict = convert_post_link_to_post(link)
    return generate_complete_prompt_for_given_post(
        post=post,
        task_name=task_name,
        only_json_format=only_json_format,
        include_context=include_context
    )


def generate_batched_post_prompt(posts: list[dict], task_name: str) -> str:
    """Create a prompt that classifies a batch of posts."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    task_prompt += """
You will receive a batch of posts to classify. The batch of posts is in a JSON
with the following fields: "posts": a list of posts, and "expected_number_of_posts":
the number of posts in the batch.

Each post will be its own JSON \
object including the text to classify (in the "text" field) and the context \
in which the post was made (in the "context" field). Return a list of JSONs \
for each post, in the format specified before. The length of the list of \
JSONs must match the value in the "expected_number_of_posts" field.

Return a JSON with the following format:
{
    "results": <list of JSONs, one for each post>,
    "count": <number of posts classified. Must match `expected_number_of_posts`>
}
"""  # noqa
    post_contexts = []
    for post in posts:
        post_contexts.append(generate_post_and_context_json(post))
    batched_context = {
        "posts": post_contexts,
        "expected_number_of_posts": len(posts)
    }
    batched_context_str = pformat(batched_context, width=200)
    full_prompt = f"""
{task_prompt}

{batched_context_str}
    """
    print(f"Length of batched_context_str: {len(batched_context_str)}")
    print(f"Length of full prompt: {len(full_prompt)}")
    return full_prompt
