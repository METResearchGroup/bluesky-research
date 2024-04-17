"""Streamlit demo app for labeling Bluesky posts using an LLM.

Lets the user upload a link to a Bluesky post and a task for classification,
and then the app prints the prompt as well as the LLM's label.
"""
import json
from pprint import pformat
import textwrap

import streamlit as st

from ml_tooling.llm.inference import run_query
from ml_tooling.llm.prompt_helper import (
    generate_complete_prompt_for_given_post,
    generate_context_details_list
)
from ml_tooling.perspective_api.helper import classify_single_post
from transform.bluesky_helper import convert_post_link_to_post

st.title("LLM labeling of Bluesky posts.")


option_to_task_name_map = {
    "Civic": "civic",
    "Political Ideology": "political_ideology",
    "Both": "both"
}


# Text box to accept user input for the link
user_link = st.text_input('Enter your link')

# Dropdown menu
option = st.selectbox('Choose your option', ('Civic', 'Political Ideology', "Both", "Perspective API"))

if option == "Perspective API":
    include_context = st.checkbox('Include context in the prompt to the Perspective API?')
else:
    justify_result_bool = st.checkbox('Justify the result?')


# run the labeling and print the result
if st.button('Submit'):
    st.subheader("The JSON of the post information:")
    post: dict = convert_post_link_to_post(user_link)
    st.text(json.dumps(post))

    if option == "Perspective API":
        st.subheader("Text passed to the Perspective API:")
        post_text = post["text"]
        # our API expects a uid, so we need to provide a default value for
        # our demo app. In production, the uid should always be provided.
        default_uid = ""
        post["uid"] = post.get("uid", default_uid)
        # if we want to include context as opposed to just using the text in
        # the post, we need to create the context string.
        if include_context:
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
        wrapped_prompt = textwrap.fill(post["text"], width=75)
        st.markdown(f"```\n{wrapped_prompt}\n```", unsafe_allow_html=True)
        st.subheader("Result from the Perspective API:")
        classification_dict: dict = classify_single_post(post)
        # convert JSON to pprint format
        formatted_res = pformat(classification_dict)
        st.text(formatted_res)

    else:
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
