"""Helper tools for generating prompts.

Resource for all things prompting: https://github.com/dair-ai/Prompt-Engineering-Guide?tab=readme-ov-file
"""  # noqa
import json

from ml_tooling.llm.task_prompts import task_name_to_task_prompt_map
from services.add_context.current_events_enrichment.bsky_news_orgs import bsky_did_to_news_org_name  # noqa
from services.add_context.current_events_enrichment.newsapi_context import url_is_to_news_domain  # noqa
from transform.bluesky_helper import get_post_record_given_post_uri
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


def process_images_embed(embed_dict: dict) -> str:
    """Processes image information and adds it to the prompt.

    Currently, we only can get info about images from its alt text,
    and we combine all the alt text across images together, so we
    combine the alt text together and return the results (which is a
    step that is done during preprocessing).

    Returns an enumerated list of the alt text.
    """
    image_alt_texts = []
    image_alt_texts: list[str] = (
        embed_dict["image_alt_text"].split(LIST_SEPARATOR_CHAR)
    )
    idx = 1
    output_str = ""
    for alt_text in image_alt_texts:
        output_str += f"Image {idx} alt text: {alt_text}\n"
        idx += 1
    return output_str


def process_record_context(record_uri: str) -> dict:
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
    embed_image_alt_text = ""
    if embed:
        embed_dict = json.loads(embed)
        if embed_dict["has_image"]:
            embed_image_alt_text = process_images_embed(embed_dict)
    return {"text": text, "embed_image_alt_text": embed_image_alt_text}


def process_record_embed(embed_dict: dict) -> str:
    """Processes record embeds.

    Record embeds are posts that are embedded in other posts. This is a way to
    reference another post in a post.

    For any records embedded in another post, we want to give enough
    information to tell the model about the original post and content that the
    current post is referencing.
    """
    output_str = ""
    embedded_record_dict: dict = json.loads(embed_dict["embedded_record"])
    embedded_record_uri: str = embedded_record_dict["uri"]
    # for now let's just add a subset of the fields from the embedded
    # record to the context, since if we add too many fields, the model
    # might confuse which information refers to the embedded record vs
    # the post that needs to be classified. We can also add other fields
    # as context if we want to, but I think that the fields that people
    # see when they see a post being replied to are the text of that
    # post and any images included.
    record_context: dict = (
        process_record_context(record_uri=embedded_record_uri)
    )
    text = record_context["text"]
    embed_image_alt_text = record_context["embed_image_alt_text"]

    # to avoid edge case of recursive embeds, let's just get any embedded
    # images or external content, not records
    if text:
        output_str += f"\n[embedded record text]: {text}"
    if embed_image_alt_text:
        output_str += f"\n[embedded record image alt text]: {embed_image_alt_text}"  # noqa
    return output_str


def process_external_embed(embed_dict: dict) -> str:
    """Processes an "external" embed, which is some externally linked content
    plus its preview card.

    External embeds are links to external content, like a YouTube video or a
    news article, which also has a preview card showing its content.
    """
    external_content_json = embed_dict["external"]
    external_content: dict = json.loads(external_content_json)
    title = external_content["title"]
    description = external_content["description"]
    return f"[title]: {title}\n[description]: {description}"


def process_record_embed_with_media_embed(embed_dict: dict) -> dict:
    """Processes a record with a media embed.

    This refers to the scenario where a post both has an image(s) attached
    and also refers to another post. In this case, we want to give context
    on both the image(s) and the embedded record.

    This returns a dict with the image and the embedded record context
    """
    images_context: str = process_images_embed(embed_dict)
    embedded_record_context: str = process_record_embed(embed_dict)
    return {
        "images_context": images_context,
        "embedded_record_context": embedded_record_context
    }


def embedded_content_context(post: dict) -> str:  # noqa: C901
    """Context of the images and attachments of a post, if any.

    Returns a blank string if none.
    """
    output_str = ""
    embed: str = post["embed"]
    if not embed:
        return output_str
    embed_dict: dict = json.loads(embed)
    if embed_dict["has_embedded_record"]:
        # records can either be plain records or be records that also reference
        # images as well.
        if embed_dict["has_image"]:
            # these have both images and embedded context, so we need to grab
            # both of these.
            context_dict: dict = process_record_embed_with_media_embed(embed_dict)  # noqa
            images_context = context_dict["images_context"]
            embedded_record_context = context_dict["embedded_record_context"]
            if images_context:
                output_str += f"\n{embedded_content_type_to_preamble_map['images']}\n```\n{images_context}\n```"  # noqa
                output_str += '\n'
            if embedded_record_context:
                output_str += f"\n{embedded_content_type_to_preamble_map['record']}:\n```\n{embedded_record_context}\n```"  # noqa
                output_str += '\n'
        else:
            context = process_record_embed(embed_dict)
            output_str += f"\n{embedded_content_type_to_preamble_map['record']}\n```\n{context}\n```"  # noqa
    elif embed_dict["has_image"]:
        context: str = process_images_embed(embed_dict)
        if context:
            output_str += f"\n{embedded_content_type_to_preamble_map['images']}\n```\n{context}\n```"  # noqa
    elif embed_dict["has_external"]:
        context: str = process_external_embed(embed_dict)
        if context:
            output_str += f"\n{embedded_content_type_to_preamble_map['external']}\n```\n{context}\n```"  # noqa
    if output_str:
        output_str += '\n'
    return output_str


def get_parent_reply_context(parent_reply_uri: str) -> str:
    """Gets the context of a post that a given post is replying to (the
    post that is being replied to is the 'parent_reply').

    Returns a blank string if none.
    """
    output_str = ""
    context = ""
    record_context = process_record_context(parent_reply_uri)
    text = record_context["text"]
    embed_image_alt_text = record_context["embed_image_alt_text"]
    text_context = f"\n[text]: {text}" if text else ""
    embed_image_alt_text = f"\n[image alt text]: {embed_image_alt_text}" if embed_image_alt_text else ""  # noqa
    if text_context or embed_image_alt_text:
        context = f"```{text_context} {embed_image_alt_text}\n```"
    if context:
        output_str = f"\n{embedded_content_type_to_preamble_map['parent_reply']}\n{context}"  # noqa
    return output_str


def get_parent_root_context(parent_root_uri: str) -> str:
    """Gets the context of the root post in a thread that a given post is
    a part of (the first post in a thread is the "parent root").

    Returns a blank string if none.
    """
    output_str = ""
    context = ""
    record_context = process_record_context(parent_root_uri)
    text = record_context["text"]
    embed_image_alt_text = record_context["embed_image_alt_text"]
    text_context = f"\n[text]: {text}\n" if text else ""
    embed_image_alt_text = f"\n[image alt text]: {embed_image_alt_text}" if embed_image_alt_text else ""  # noqa
    if text_context or embed_image_alt_text:
        context = f"```{text_context} {embed_image_alt_text}\n```"
    if context:
        output_str = f"\n{embedded_content_type_to_preamble_map['parent_root']}\n{context}"  # noqa
    return output_str


def define_thread_context(post: dict) -> str:
    """Context of a thread that the post is a part of, if any.

    Returns a blank string if none.
    """
    # check if there is a reply parent or a reply root, and then check
    # if that post's URI exists in our filtered DB.
    output_str = ""

    reply_parent: str = post["reply_parent"]
    reply_root: str = post["reply_root"]
    parent_reply_context: str = ""
    parent_root_context: str = ""

    if reply_parent:
        parent_reply_context: str = get_parent_reply_context(reply_parent)
    # we can do this check this way because the existence of a reply root
    # implies the existence of a reply parent, so we only need to check for
    # the existence of a reply root if it's not the same as the reply parent.
    if reply_root and reply_root != reply_parent:
        parent_root_context: str = get_parent_root_context(reply_root)

    if parent_reply_context:
        output_str += parent_reply_context + '\n'
    if parent_root_context:
        output_str += parent_root_context + '\n'
    return output_str


def context_embed_url(post: dict) -> str:
    """Adds context for URLs that are added as embeds in the post.

    These are for links that are embeds in the post, not just linked to in
    the text of the post.

    Example: https://bsky.app/profile/parismarx.bsky.social/post/3kpvlrkcr6m2q
    """
    output_str = ""
    embed: str = post["embed"]
    if not embed:
        return output_str
    embed_dict = json.loads(embed)
    if embed_dict["has_external"]:
        external_content_json = embed_dict["external"]
        external_content: dict = json.loads(external_content_json)
        url = external_content["uri"]
        if url_is_to_news_domain(url):
            output_str = "This post links to a trustworthy news article.\n"
    return output_str


def context_url_in_text(post: dict) -> str:
    """Adds context for URLs that are linked to in the text of the post.

    URLs are included in the text of the post as "facets", as this is how
    the Bluesky platform does links in lieu of markdown.

    Example: https://bsky.app/profile/donmoyn.bsky.social/post/3kqfra54rt52z
    """
    output_str = ""
    facets_str: str = post["facets"]
    links: list[str] = []
    if facets_str:
        facets: list[str] = facets_str.split(LIST_SEPARATOR_CHAR)
        for facet in facets:
            if facet.startswith("link:"):
                link = facet.replace("link:", "")
                links.append(link)
    if any([url_is_to_news_domain(link) for link in links]):
        output_str = "This post links to a trustworthy news article.\n"
    return output_str


def post_linked_urls(post: dict) -> str:
    """Context if the post refers to any URLs in the text.

    Returns a blank string if none.
    """
    output_str = ""

    # check if there are any URLs in the text of the post
    url_in_text_context: str = context_url_in_text(post)
    embed_url_context: str = context_embed_url(post)

    if url_in_text_context or embed_url_context:
        output_str += f"{embedded_content_type_to_preamble_map['linked_urls']}\n"  # noqa
        # so far our context just checks to see if a link is to a news outlet
        # regardless of whether it's in the text or an embed, so we can just
        # see if the post has any reputable links at all. We keep those
        # contexts separate in case we want to add more context to each.
        context = (
            url_in_text_context if url_in_text_context else embed_url_context
        )
        output_str += context
    return output_str


def post_tags_labels(post: dict) -> str:
    """Context based on tags/labels provided about the post, if any.

    Returns a blank string if none.
    """
    output_str = ""
    tags = []
    if post["tags"]:
        tags: list[str] = post["tags"].split(LIST_SEPARATOR_CHAR)
    labels = []
    if post["labels"]:
        labels: list[str] = post["labels"].split(LIST_SEPARATOR_CHAR)
    if labels:
        output_str += f"{embedded_content_type_to_preamble_map['labels']}\n"
        for label in labels:
            output_str += f"- {label}\n"
    if tags:
        output_str += f"{embedded_content_type_to_preamble_map['tags']}\n"
        for tag in tags:
            output_str += f"- {tag}\n"
    if output_str:
        output_str += '\n'
    return output_str


def additional_current_events_context(post: dict) -> str:
    """Additional context based on the post if it references
    something related to current events (which likely isn't in the training
    set of the model).

    Returns a blank string if none.
    """
    pass


def post_author_context(post: dict) -> str:
    """Returns contextual information about the post's author.

    For now, we just return if the post author is a news org, but we can add to
    this later on.
    """
    output_str = ""
    author = post["author"]
    if author in bsky_did_to_news_org_name:
        news_org = bsky_did_to_news_org_name[author]
        output_str = f"{embedded_content_type_to_preamble_map['post_author']}\n- The post is from a reputable news outlet: {news_org}\n"  # noqa
    return output_str


post_context_and_funcs = [
    ("Content referenced or linked to in the post", embedded_content_context),
    ("URLs", post_linked_urls),
    ("Thread that the post is a part of", define_thread_context),
    ("Tags and labels in the post", post_tags_labels),
    ("Context about current events", additional_current_events_context),
    ("Context about the post author", post_author_context)
]


def generate_context_details_list(post: dict) -> list[tuple]:
    context_details_list = []
    for context_name, context_func in post_context_and_funcs:
        context = context_func(post)
        if context:
            context_details_list.append((context_name, context))
    return context_details_list


def generate_complete_prompt(
    post: dict,
    task_prompt: str,
    context_details_list: list[tuple],
    justify_result: bool = False
) -> str:
    """Given a task prompt and the details of the context, generate
    the resulting complete prompt.
    """
    post_text = post["text"]
    full_context = ""
    for context_type, context_details in context_details_list:
        full_context += f"<{context_type}>\n {context_details}\n"
    if full_context:
        full_context = f"""
The classification of a post might depend on contextual information. Attend to the context where appropriate.
For example, the text in a post might comment on an image or on a retweeted post.
Here is some context on the post that needs classification:
```
{full_context}
```
Again, the text of the post that needs to be classified is:
```
<text>
{post_text}
```
"""  # noqa
    if justify_result:
        full_context += "\nAfter giving your label, start a new line and then justify your answer in 1 sentence."  # noqa
    return base_prompt.format(
        task_prompt=task_prompt, post_text=post_text, context=full_context
    )


def generate_complete_prompt_for_given_post(
    post: dict,
    task_name: str,
    include_context: bool = True,
    justify_result: bool = False
) -> str:
    """Generates a complete prompt for a given post."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    context_details_list = (
        generate_context_details_list(post) if include_context else []
    )
    return generate_complete_prompt(
        post=post,
        task_prompt=task_prompt,
        context_details_list=context_details_list,
        justify_result=justify_result
    )