"""
Main script for user engagement analysis.
"""

import pandas as pd

from services.calculate_analytics.shared.data_loading.users import (
    load_user_date_to_week_df,
    load_user_demographic_info,
)


def load_user_data() -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    """Loads user data, transformed in the format needed for analysis.

    Returns the following:
    - user_df: DataFrame where each row is a user.
    - user_date_to_week_df: transformed DataFrame where each row is a user + date
    and the week assigned to that date. This requires adding the "bluesky_user_did"
    since the original .csv file loaded doesn't contain the user DIDs and all
    of our transformations are done at the user DID level.
    - valid_study_users_dids: a set of the DIDs for valid study users. We return as
    a set since this helps us quickly filter content during analysis (as opposed
    to essentially regenerating this set during individual transformations whenever
    we need to do filtering).
        - We can have activities that aren't tied to valid study users because (1) we
        may have been tracking a pilot user (in which case we don't want to include)
        their data, or (2) we might be tracking activities from a user who dropped out
        of the study (either voluntary or we ourselves excluded their data or their account
        got banned).
    """

    # load the user data from DynamoDB.
    user_df: pd.DataFrame = load_user_demographic_info()
    user_df = user_df[user_df["is_study_user"]]
    user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]

    # load mapping of user to date and week. This denotes, for each user and date,
    # what week of the study that was for that user. We need this so we know, for a
    # given date, what week to assign their activities and measures to.
    user_date_to_week_df: pd.DataFrame = load_user_date_to_week_df()
    user_handle_to_did_map = {
        bluesky_handle: bluesky_user_did
        for bluesky_handle, bluesky_user_did in zip(
            user_df["bluesky_handle"], user_df["bluesky_user_did"]
        )
    }
    user_date_to_week_df["bluesky_user_did"] = user_date_to_week_df[
        "bluesky_handle"
    ].map(user_handle_to_did_map)

    # we base the valid users on the Qualtrics logs. We load the users from
    # DynamoDB but some of those users aren't valid (e.g., they were
    # experimental users, they didn't do the study, etc.). Since
    # user_date_to_week_df is generated using the Qualtrics logs, we treat that
    # as the master log of valid users who participated and completed the study.
    valid_study_user_handles = set(user_date_to_week_df["bluesky_handle"].unique())
    valid_study_users_dids = set(
        [user_handle_to_did_map[handle] for handle in valid_study_user_handles]
    )
    user_df = user_df[user_df["bluesky_user_did"].isin(valid_study_users_dids)]

    return user_df, user_date_to_week_df, valid_study_users_dids


def main():
    # load users
    user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()

    # get all content engaged with by study users
    engaged_content = pd.DataFrame()
    print(f"Shape of engaged content: {engaged_content.shape}")

    # start doing aggregations. First is daily aggregations.
    daily_aggregated = pd.DataFrame()
    print(f"Shape of daily aggregated content: {daily_aggregated.shape}")

    # then next is per-week aggregations.
    weekly_aggregated = pd.DataFrame()
    print(f"Shape of weekly aggregated: {weekly_aggregated.shape}")

    pass


if __name__ == "__main__":
    main()
