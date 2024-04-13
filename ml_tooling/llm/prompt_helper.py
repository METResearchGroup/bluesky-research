"""Helper tools for generating prompts."""
from ml_tooling.llm.task_prompts import task_name_to_task_prompt_map


base_prompt = """
    {task_prompt}.

    Here is the post text that needs to be classified:
    <text>
    {post_text}

    Here is some context on the post that needs classification:

    {context}

    Again, the text of the post that needs to be classified is:
    <text>
    {post_text}
"""


def image_and_attachments_context(post: dict) -> str:
    """Context of the images and attachments of a post, if any.

    Returns a blank string if none.
    """
    output_str = ""
    return output_str


def get_parent_reply_context(parent_reply_uri: str) -> str:
    """Gets the context of a post that a given post is replying to (the
    post that is being replied to is the 'parent_reply').

    Returns a blank string if none.
    """
    output_str = ""
    return output_str


def get_parent_root_context(parent_root_uri: str) -> str:
    """Gets the context of the root post in a thread that a given post is
    a part of (the first post in a thread is the "parent root").

    Returns a blank string if none.
    """
    output_str = ""
    return output_str


def define_thread_context(post: dict) -> str:
    """Context of a thread that the post is a part of, if any.

    Returns a blank string if none.
    """
    # check if there is a reply parent or a reply root, and then check
    # if that post's URI exists in our filtered DB.
    output_str = ""

    reply_parent = post["reply_parent"]
    reply_root = post["reply_root"]
    parent_reply_context = None
    parent_root_context = None

    if reply_parent:
        parent_reply_context = get_parent_reply_context(reply_parent)
    # we can do this check this way because the existence of a reply root
    # implies the existence of a reply parent, so we only need to check for
    # the existence of a reply root if it's not the same as the reply parent.
    elif reply_root and reply_root != reply_parent:
        parent_root_context = get_parent_root_context(reply_root)

    if parent_reply_context:
        output_str += f"[Here is the post that it responds to]:\n {parent_reply_context}"
    if parent_root_context:
        output_str += f"[Here is the first post in the thread]:\n {parent_reply_context}"

    return output_str


def post_linked_urls(post: dict) -> str:
    """Context if the post refers to any URLs in the text.
    
    Returns a blank string if none.
    """
    output_str = ""
    
    return output_str


def post_tags_and_labels(post: dict) -> str:
    """Context based on tags/labels provided about the post, if any.

    Returns a blank string if none.
    """
    pass


def additional_current_events_context(post: dict) -> str:
    """Additional context based on the post if it references
    something related to current events (which likely isn't in the training
    set of the model).

    Returns a blank string if none.
    """
    pass


post_context_and_funcs = [
    ("images_and_attachments", image_and_attachments_context),
    ("URLs", post_linked_urls),
    ("thread", define_thread_context),
    ("tags and labels", post_tags_and_labels),
    ("current events context", additional_current_events_context)
]

def generate_context_details_list(post: dict) -> list[tuple]:
    context_details_list = []
    for context_name, context_func in post_context_and_funcs:
        context = context_func(post)
        if context:
            context_details_list.append((context_name, context))
    return context_details_list


def generate_complete_prompt(
    post: dict, task_prompt: str, context_details_list: list[tuple]
) -> str:
    """Given a task prompt and the details of the context, generate
    the resulting complete prompt.
    """
    full_context = ""
    for context_type, context_details in context_details_list:
        full_context += f"<{context_type}>:\n {context_details}\n"
    return base_prompt.format(
        task_prompt=task_prompt,
        post_text=post["text"],
        context=full_context
    )


def generate_complete_prompt_for_given_post(post: dict, task_name: str) -> str:
    """Generates a complete prompt for a given post."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    context_details_list = generate_context_details_list(post)
    return generate_complete_prompt(
        post=post,
        task_prompt=task_prompt,
        context_details_list=context_details_list
    )
