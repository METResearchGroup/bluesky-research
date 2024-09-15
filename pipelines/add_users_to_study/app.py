"""Streamlit app for adding users to the study."""

import streamlit as st

from lib.db.sql.participant_data_database import insert_bsky_user_to_study
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from transform.bluesky_helper import get_author_did_from_handle

map_label_to_condition = {
    "Reverse Chronological": "reverse_chronological",
    "Engagement": "engagement",
    "Representative Diversification": "representative_diversification",
}

st.set_page_config(layout="wide")

study_users: list[UserToBlueskyProfileModel] = get_all_users()


def validate_user():
    pass


def display_users_by_condition():
    reverse_chronological_users = []
    engagement_users = []
    representative_diversification_users = []
    for user in study_users:
        if user.condition == "reverse_chronological":
            reverse_chronological_users.append(user)
        elif user.condition == "engagement":
            engagement_users.append(user)
        elif user.condition == "representative_diversification":
            representative_diversification_users.append(user)

    # for each condition, print the count of users in that condition
    # and print 5 users in that condition. The condition should be in bold.
    columns = st.columns(3)  # Create three columns

    with columns[0]:  # Display in the first column
        st.markdown(
            f"**Reverse Chronological: {len(reverse_chronological_users)} users**"
        )  # noqa
        for user in reverse_chronological_users:
            st.write(user.bluesky_handle)
        st.write("-" * 10)

    with columns[1]:  # Display in the first column
        st.markdown(f"**Engagement: {len(engagement_users)} users**")
        for user in engagement_users:
            st.write(user.bluesky_handle)
        st.write("-" * 10)

    with columns[2]:  # Display in the first column
        st.markdown(
            f"**Representative Diversification: {len(representative_diversification_users)} users**"
        )
        for user in representative_diversification_users:
            st.write(user.bluesky_handle)
        st.write("-" * 10)


def add_new_user_to_study():
    # then, add condition as dropdown menu
    condition_label = st.selectbox("Condition:", list(map_label_to_condition.keys()))  # noqa

    # first, add bluesky handle
    bluesky_handle = st.text_input("Bluesky Handle (or profile link):")
    add_user = st.button("Add user to study.")
    add_user = False

    if add_user:
        st.write(f"Adding user with handle {bluesky_handle} to the study...")
        # add user to study
        try:
            if bluesky_handle.startswith("http"):
                if bluesky_handle.startswith("https://bsky.app/profile/"):
                    bluesky_handle = bluesky_handle.split("/")[-1]
                else:
                    st.error("Invalid Bluesky handle.")
                    st.stop()

            try:
                bsky_author_did = get_author_did_from_handle(bluesky_handle)
            except Exception as e:
                st.error(f"No user found with handle {bluesky_handle}: {e}.")
                st.stop()

            condition = map_label_to_condition[condition_label]

            insert_bsky_user_to_study(
                bluesky_handle=bluesky_handle,
                condition=condition,
                bluesky_user_did=bsky_author_did,
            )
            st.write("User added to study.")
        except Exception as e:
            st.error(f"Error adding user to study: {e}")
            st.stop()

        st.subheader("Existing users in the study.")
        display_users_by_condition()


st.title("Manage Bluesky study users.")

st.subheader("Add a user to the study.")
add_new_user_to_study()

st.subheader("Existing users in the study.")
display_users_by_condition()
