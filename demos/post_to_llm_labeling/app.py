"""Streamlit demo app for labeling Bluesky posts using an LLM.

Lets the user upload a link to a Bluesky post and a task for classification,
and then the app prints the prompt as well as the LLM's label.
"""
import json
from pprint import pformat
import textwrap
import time

import streamlit as st
import tiktoken

from ml_tooling.llm.inference import run_query
from ml_tooling.llm.prompt_helper import (
    generate_batched_post_prompt,
    generate_complete_prompt,
    generate_complete_prompt_for_given_post
)
from ml_tooling.llm.task_prompts import task_name_to_task_prompt_map
from ml_tooling.perspective_api.helper import classify_single_post
from transform.bluesky_helper import convert_post_link_to_post

encoding = tiktoken.encoding_for_model('gpt-3.5-turbo')


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.encoding_for_model('gpt-3.5-turbo')
    num_tokens = len(encoding.encode(string))
    return num_tokens


st.set_page_config(layout="wide")
st.title("Labeling of Bluesky posts.")


option_to_task_name_map = {
    "Civic": "civic",
    "Political Ideology": "political_ideology",
    "Both Civic and Political": "both",
    "Toxicity": "toxicity",
    "Constructiveness": "constructiveness",
    "Rewrite if toxic": "rewrite_if_toxic"
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

bulk_bluesky_posts = st.text_area(
    "Enter multiple Bluesky links/texts separated by commas"
)

if bulk_bluesky_posts:
    bulk_classify_posts = st.checkbox(
        "Classify all the posts in bulk?"
    )
else:
    bulk_classify_posts = False

# Dropdown menu
option = st.selectbox(
    'Choose your option',
    (
        'Civic', 'Political Ideology', "Both Civic and Political",
        "Toxicity", "Constructiveness", "Perspective API", "Rewrite if toxic"
    )
)

context_format = st.selectbox('Choose the context format', ("json", "yaml"))

if option == "Perspective API":
    include_context = st.checkbox(
        'Include context in the prompt to the Perspective API?'
    )
    model_name = None
else:
    model_name = st.selectbox(
        "Choose your LLM model",
        (
            "Gemini", "Llama3-8b (via Groq)", "Llama3-70b (via Groq)"
        )
    )

include_context_bool = st.checkbox('Include context in the prompt?')


# run the labeling and print the result
if st.button('Submit'):

    # manage bulk posts
    if bulk_classify_posts:
        bsky_post_links: list[str] = bulk_bluesky_posts.split(",")
        posts: list[dict] = [
            convert_post_link_to_post(
                post_link, include_author_info=True
            ) for post_link in bsky_post_links
        ]
        task_name: str = option_to_task_name_map[option]
        prompt: str = generate_batched_post_prompt(
            posts=posts, task_name=task_name
        )
        num_tokens = num_tokens_from_string(prompt)
        st.text(f"Number of tokens in the prompt: {num_tokens}")
        start_time = time.time()
        wrapped_prompt = wrap_text(prompt, width=200)
        st.text(wrapped_prompt)
        st.subheader("Result from the LLM")
        llm_result = run_query(prompt=prompt, model_name=model_name)
        st.text(wrap_text(llm_result, width=120))
        end_time = time.time()
        execution_time_seconds = round(end_time - start_time)
        execution_time_minutes = execution_time_seconds // 60
        execution_time_leftover_seconds = (
            execution_time_seconds - (60 * execution_time_minutes)
        )

        time_str = f"Execution time: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds"  # noqa
        st.text(time_str)
        print(llm_result)

    else:
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
                task_name = option_to_task_name_map[option]
                prompt: str = (
                    generate_complete_prompt(
                        post=post,
                        task_prompt=task_name_to_task_prompt_map[task_name],
                        is_text_only=True,
                        format=context_format
                    )
                )
            else:
                prompt: str = (
                    generate_complete_prompt_for_given_post(
                        post=post,
                        task_name=option_to_task_name_map[option],
                        include_context=include_context_bool,
                        format=context_format
                    )
                )
            wrapped_prompt = wrap_text(prompt, width=200)
            st.text(wrapped_prompt)
            st.subheader("Result from the LLM")
            llm_result = run_query(prompt=prompt, model_name=model_name)
            st.text(wrap_text(llm_result, width=120))
            print(llm_result)
            input_token_count = num_tokens_from_string(prompt)
            output_token_count = num_tokens_from_string(llm_result)
            st.text(f"Prompt token count: {input_token_count}")
            st.text(f"Output token count: {output_token_count}")


if __name__ == "__main__":
    # link = "https://bsky.app/profile/brendelbored.bsky.social/post/3kpp6rd7z3k2r" # noqa
    # post = convert_post_link_to_post(link)
    # prompt = generate_complete_prompt_for_given_post(post, "civic")
    # print(prompt)
    pass
