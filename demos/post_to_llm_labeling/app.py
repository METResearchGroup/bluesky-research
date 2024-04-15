"""Streamlit demo app for labeling Bluesky posts using an LLM.

Lets the user upload a link to a Bluesky post and a task for classification,
and then the app prints the prompt as well as the LLM's label.
"""
import json
import textwrap

import streamlit as st

from ml_tooling.llm.inference import run_query
from ml_tooling.llm.prompt_helper import generate_complete_prompt_for_given_post # noqa
from transform.bluesky_helper import convert_post_link_to_post

st.title("LLM labeling of Bluesky posts.")


option_to_task_name_map = {
    "Civic": "civic", "Political Ideology": "political_ideology"
}


# Text box to accept user input for the link
user_link = st.text_input('Enter your link')

# Dropdown menu
option = st.selectbox('Choose your option', ('Civic', 'Political Ideology'))
justify_result_bool = st.checkbox('Justify the result?')


# run the labeling and print the result
if st.button('Submit'):
    st.subheader("The JSON of the post information:")
    post: dict = convert_post_link_to_post(user_link)
    st.text(json.dumps(post))

    st.subheader("Prompt to the LLM:")
    prompt: str = (
        generate_complete_prompt_for_given_post(
            post=post,
            task_name=option_to_task_name_map[option],
            justify_result=justify_result_bool
        )
    )
    wrapped_prompt = textwrap.fill(prompt, width=100)
    st.text(wrapped_prompt)

    st.subheader("Result from the LLM:")
    llm_result = run_query(prompt=prompt)
    st.text(llm_result)


if __name__ == "__main__":
    # link = "https://bsky.app/profile/brendelbored.bsky.social/post/3kpp6rd7z3k2r"
    # post = convert_post_link_to_post(link)
    # prompt = generate_complete_prompt_for_given_post(post, "civic")
    # print(prompt)
    pass
