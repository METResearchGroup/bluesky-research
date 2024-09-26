"""Manages users from the provided .csv files."""

import pandas as pd

from services.participant_data.helper import manage_bsky_study_user
from transform.bluesky_helper import get_author_did_from_handle

number_to_condition_map = {
    1: "reverse_chronological",
    2: "engagement",
    3: "representative_diversification",
}


def add_users_from_csv_file(csv_file_path: str):
    """Adds users from the provided .csv file."""
    df = pd.read_csv(csv_file_path)
    handles = df["handle"].tolist()
    handles = [handle for handle in handles if pd.notna(handle)]
    # same length as handles, and guaranteed to be in the same order
    # (the first X posts in handles are all NaN and they'll also have
    # conditions assigned to them)
    conditions = df["condition"].tolist()
    conditions = conditions[: len(handles)]
    handle_to_condition_tuples = [
        (handle, number_to_condition_map[condition])
        for handle, condition in zip(handles, conditions)
    ]
    total_users_to_add = len(handle_to_condition_tuples)
    users_added = 0
    for idx, (bluesky_handle, condition) in enumerate(handle_to_condition_tuples):
        try:
            bsky_author_did = get_author_did_from_handle(bluesky_handle)
        except Exception as e:
            print(f"No user found with handle {bluesky_handle}: {e}.")
            continue
        payload = {
            "operation": "POST",
            "bluesky_user_did": bsky_author_did,
            "bluesky_handle": bluesky_handle,
            "is_study_user": True,
            "condition": condition,
        }
        manage_bsky_study_user(payload=payload)
        users_added += 1
        if idx % 10 == 0:
            print(f"Added {users_added}/{total_users_to_add} users.")
    print(f"Completed adding study users from {csv_file_path}.")


def delete_users_from_csv_file(csv_file_path: str):
    """Deletes users from the provided .csv file."""
    df = pd.read_csv(csv_file_path)
    handles = df["username"].tolist()
    bsky_handle_to_did_map = {}
    for bluesky_handle in handles:
        try:
            bsky_author_did = get_author_did_from_handle(bluesky_handle)
            bsky_handle_to_did_map[bluesky_handle] = bsky_author_did
        except Exception as e:
            print(f"No user found with handle {bluesky_handle}: {e}.")
            continue
    total_users_to_delete = len(bsky_handle_to_did_map)
    print(f"Deleting {total_users_to_delete} users...")
    users_deleted = 0
    for bluesky_handle, bsky_author_did in bsky_handle_to_did_map.items():
        payload = {
            "operation": "DELETE",
            "bluesky_user_did": bsky_author_did,
        }
        manage_bsky_study_user(payload=payload)
        users_deleted += 1
        if users_deleted % 10 == 0:
            print(f"Deleted {users_deleted}/{total_users_to_delete} users.")
    print(f"Completed deleting {total_users_to_delete} users.")


if __name__ == "__main__":
    csv_files_of_users_to_add = ["users_1.csv"]
    csv_files_of_users_to_delete = ["spam_users_1.csv"]

    # TODO: for deletes, logic should check if they're in the study. I've
    # already deleted some so some might not be in there.

    print(f"Adding users from {csv_files_of_users_to_add}")
    for csv_file_path in csv_files_of_users_to_add:
        add_users_from_csv_file(csv_file_path)

    print(f"Deleting users from {csv_files_of_users_to_delete}")
    for csv_file_path in csv_files_of_users_to_delete:
        delete_users_from_csv_file(csv_file_path)
