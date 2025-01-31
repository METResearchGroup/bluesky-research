"""Script to get all study users, by condition.

Also corrects for users who we previously incorrectly assigned to the
RC condition.
"""

from datetime import datetime

import pandas as pd

from services.participant_data.helper import get_all_users

min_timestamp = "2024-09-20"  # anything before this was a test user.
# these are users that were marked as missing but they actually aren't, and
# were assigned to the RC condition by default.
previously_missing_bsky_handles = pd.read_csv("missing_bsky_handles.csv")
date_str = datetime.now().strftime("%Y-%m-%d")


def main():
    users = get_all_users()
    users = [user for user in users if user.created_timestamp >= min_timestamp]
    users_df = pd.DataFrame([user.dict() for user in users])
    # Convert handles to lowercase for consistent comparison
    previously_missing_bsky_handles["handle"] = previously_missing_bsky_handles[
        "handle"
    ].str.lower()
    users_df["bluesky_handle"] = users_df["bluesky_handle"].str.lower()
    users_df = users_df[["bluesky_handle", "condition"]]

    # for any handles in "previously_missing_bsky_handles", if they are in
    # users_df, set their condition to "RC"
    total_users_corrected = 0
    for handle in previously_missing_bsky_handles["handle"]:
        if handle in users_df["bluesky_handle"].values:
            users_df.loc[users_df["bluesky_handle"] == handle, "condition"] = (
                "reverse_chronological"
            )
            total_users_corrected += 1
    print(f"Corrected {total_users_corrected} users")

    export_filename = f"study_users_{date_str}.csv"
    users_df.to_csv(export_filename, index=False)
    print(f"Exported {len(users_df)} users to {export_filename}")


if __name__ == "__main__":
    main()
