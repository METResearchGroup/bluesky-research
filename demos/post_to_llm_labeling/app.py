"""Streamlit demo app for labeling Bluesky posts using an LLM.

Lets the user upload a link to a Bluesky post and a task for classification,
and then the app prints the prompt as well as the LLM's label.
"""
import json
from pprint import pformat
import textwrap

import streamlit as st

from ml_tooling.llm.inference import BACKEND_OPTIONS, run_query
from ml_tooling.llm.prompt_helper import (
    generate_complete_prompt,
    generate_complete_prompt_for_given_post,
    generate_context_details_list
)
from ml_tooling.perspective_api.helper import classify_single_post
from transform.bluesky_helper import convert_post_link_to_post

st.set_page_config(layout="wide")
st.title("Labeling of Bluesky posts.")


option_to_task_name_map = {
    "Civic": "civic",
    "Political Ideology": "political_ideology",
    "Both Civic and Political": "both",
    "Toxicity": "toxicity",
    "Constructiveness": "constructiveness",
}


def wrap_text(text, width=100):
    """Wraps text into a nicer representation by controlling line length.

    Splits by newline chars and then wraps each line separately.
    """
    lines = text.split('\n')
    wrapped_lines = [textwrap.fill(line, width=width) if line.strip() != '' else line for line in lines]  # noqa
    wrapped_text = '\n'.join(wrapped_lines)

    return wrapped_text


# Text box to accept user input for the link
user_input = st.text_input('Enter your Bluesky link/text')

# Dropdown menu
option = st.selectbox(
    'Choose your option',
    (
        'Civic', 'Political Ideology', "Both Civic and Political",
        "Toxicity", "Constructiveness", "Perspective API"
    )
)

if option == "Perspective API":
    include_context = st.checkbox(
        'Include context in the prompt to the Perspective API?')
else:
    justify_result_bool = st.checkbox('Justify the result?')
include_context_bool = st.checkbox('Include context in the prompt?')


# run the labeling and print the result
if st.button('Submit'):
    is_text_bool = not (
        user_input.startswith("http") or "bsky.app" in user_input
    )
    st.subheader("The JSON of the post information:")
    if is_text_bool:
        post = {}
        post["text"] = user_input
        post["uid"] = ""
    else:
        post: dict = convert_post_link_to_post(
            user_input, include_author_info=True
        )
    st.text(
        wrap_text(json.dumps(post), width=200)
    )

    if option == "Perspective API":
        st.subheader("Text passed to the Perspective API:")
        post_text = post["text"]
        # our API expects a uid, so we need to provide a default value for
        # our demo app. In production, the uid should always be provided.
        default_uid = ""
        post["uid"] = post.get("uid", default_uid)
        # if we want to include context as opposed to just using the text in
        # the post, we need to create the context string.
        if include_context and not is_text_bool:
            context_details_list = generate_context_details_list(post)
            full_context = ""
            for context_type, context_details in context_details_list:
                full_context += f"<{context_type}>\n {context_details}\n"
            post_with_context_prompt = f"""
            Here is the post text that needs to be classified:
            ```
            <text>
            {post_text}
            ```

            Here is some context on the post that needs classification:
            ```
            {full_context}
            ```
            Again, the text of the post that needs to be classified is:
            ```
            <text>
            {post_text}
            """
            post["text"] = post_with_context_prompt
        wrapped_prompt = textwrap.fill(post["text"], width=200)
        st.markdown(f"```\n{wrapped_prompt}\n```", unsafe_allow_html=True)
        st.subheader("Result from the Perspective API:")
        classification_dict: dict = classify_single_post(post)
        # convert JSON to pprint format
        formatted_res = pformat(classification_dict)
        st.text(formatted_res)

    else:
        st.subheader("Prompt to the LLM:")
        if is_text_bool:
            context_details_list = []
            prompt: str = (
                generate_complete_prompt(
                    post=post, task_prompt=option_to_task_name_map[option],
                    context_details_list=context_details_list,
                    justify_result=justify_result_bool
                )
            )
        else:
            prompt: str = (
                generate_complete_prompt_for_given_post(
                    post=post,
                    task_name=option_to_task_name_map[option],
                    include_context=include_context_bool,
                    justify_result=justify_result_bool
                )
            )
        wrapped_prompt = wrap_text(prompt, width=200)
        st.text(wrapped_prompt)
        st.subheader("Result from the LLM")
        llm_result = run_query(prompt=prompt)
        st.text(wrap_text(llm_result, width=120))


if __name__ == "__main__":
    # link = "https://bsky.app/profile/brendelbored.bsky.social/post/3kpp6rd7z3k2r" # noqa
    # post = convert_post_link_to_post(link)
    # prompt = generate_complete_prompt_for_given_post(post, "civic")
    # print(prompt)
    pass