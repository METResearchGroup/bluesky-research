"""Streamlit app for adding users to the study."""
import streamlit as st

from lib.db.sql.participant_data_database import (
    get_users_in_condition, insert_bsky_user_to_study
)
from transform.bluesky_helper import get_author_did_from_handle

map_label_to_condition = {
    "Reverse Chronological": "reverse_chronological",
    "Engagement": "engagement",
    "Representative Diversification": "representative_diversification"

}


def display_users_by_condition():
    # for each condition, print the count of users in that condition
    # and print 5 users in that condition. The condition should be in bold.
    for label, condition in map_label_to_condition.items():
        users_in_condition = get_users_in_condition(condition)
        st.markdown(f"**{label}**")
        st.write(f"Count: {len(users_in_condition)}")
        for user in users_in_condition[:5]:
            st.write(user.bluesky_handle)
        st.write("...")
        st.write("\n")


st.title("Add Users to Study")

# first, add bluesky handle
bluesky_handle = st.text_input("Bluesky Handle (or profile link):")

if bluesky_handle.startswith("http"):
    if bluesky_handle.startswith("https://bsky.app/profile/"):
        bluesky_handle = bluesky_handle.split("/")[-1]
    else:
        st.error("Invalid Bluesky handle.")
        st.stop()

# then, add condition as dropdown menu
condition_label = st.selectbox("Condition:", list(map_label_to_condition.keys()))

# finally, add user to study
add_user = st.button("Add User to Study")

display_users_by_condition()

try:
    bsky_author_did = get_author_did_from_handle(bluesky_handle)
except Exception as e:
    st.error(f"No user found with handle {bluesky_handle}.")
    st.stop()

if add_user:
    st.write(f"Adding user with handle {bluesky_handle} to the study...")
    # add user to study
    try:
        condition = map_label_to_condition[condition_label]
        insert_bsky_user_to_study(
            bluesky_handle=bluesky_handle,
            condition=condition,
            bluesky_user_did=bsky_author_did
        )
        st.write("User added to study.")
    except Exception as e:
        st.error(f"Error adding user to study: {e}")
        st.stop()

    display_users_by_condition()
