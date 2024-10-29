"""Add users who were missed in a previous onboarding run.

39 users, all from Prolific.
"""

import os

import pandas as pd

from pipelines.add_users_to_study.add_users_from_csv_file import add_users_from_csv_file
from services.participant_data.models import UserToBlueskyProfileModel
from services.participant_data.study_users import get_all_users


current_file_directory = os.path.dirname(os.path.abspath(__file__))
missing_users_filename = "missing_users.csv"
missing_users_filepath = os.path.join(current_file_directory, missing_users_filename)


def fetch_profile_data_of_previously_missing_users():
    """Get the profile data of previously missing users."""
    study_users: list[UserToBlueskyProfileModel] = get_all_users()
    existing_bsky_handles = [user.bluesky_handle for user in study_users]
    users = pd.read_csv("userlist.csv")

    users = users[["handle", "condition"]]
    users["handle"] = users["handle"].str.lower()

    master_bsky_handles = users["handle"].tolist()

    # these are handles marked as excluded from the study.
    excludelist_handles = [
        "eighty.gay",
        "alexamadeo.bsky.social",
        "stevshut.bsky.social",
        "blue.social",
        "thelittlemage.bsky.social",
        "brightjourneys.bsky.social",
        "john.doe.bsky.social",
        "xdespite.bsky.social",
        "marira4vj3.bsky.social",
        "guesswurk.bsky.social",
        "kanjan96.bsky.social",
        "countryboy.bsky.social",
    ]

    missing_bsky_handles = [
        handle
        for handle in master_bsky_handles
        if handle not in existing_bsky_handles
        and not pd.isna(handle)
        and handle not in excludelist_handles
    ]

    subset_users = users[users["handle"].isin(missing_bsky_handles)]

    # override their condition to be reverse-chronological since they've been
    # seeing the default feeds.
    subset_users["condition"] = 1

    print(f"Missing {len(subset_users)} bsky handles.")

    subset_users.to_csv(missing_users_filepath, index=False)

    print(f"Exported {len(subset_users)} previously missing users.")


if __name__ == "__main__":
    fetch_profile_data_of_previously_missing_users()
    add_users_from_csv_file(missing_users_filepath)
