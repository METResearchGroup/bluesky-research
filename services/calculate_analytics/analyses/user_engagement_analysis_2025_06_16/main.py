"""
Main script for user engagement analysis.
"""

import os

import pandas as pd

from lib.helper import generate_current_datetime_str
from services.calculate_analytics.shared.data_loading.engagement import (
    get_content_engaged_with_per_user,
    get_engaged_content,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_labels_for_engaged_content,
)
from services.calculate_analytics.shared.data_loading.users import (
    load_user_date_to_week_df,
    load_user_demographic_info,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()
engaged_content_daily_aggregated_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"daily_content_label_proportions_per_user_{current_datetime_str}.csv",
)
engaged_content_weekly_aggregated_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"weekly_content_label_proportions_per_user_{current_datetime_str}.csv",
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

    # We have to add the bluesky_user_did to the user + date + week DataFrame
    # since our engagement data (and frankly, any Bluesky activity data that can
    # be linked to a specific user) only contains their DID, not their user
    # handle and the generated `user_date_to_week_df` doesn't originally have that
    # field available.
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


def do_setup():
    """Setup steps for analysis. Includes:
    - 1. Loading users
    - 2. Loading content that users engaged with. Then we transform it to a
    format suited for our analysis.
    - 3. Loading the labels for the content that users engaged with. Then we
    transform it to a format suited for our analysis.
    """
    # load users
    user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()

    # TODO: prints are just to satisfy linter. Remove later.
    print(f"Shape of user_df: {user_df.shape}")
    print(f"Shape of user_date_to_week_df: {user_date_to_week_df.shape}")
    print(f"Number of valid study user DIDs: {valid_study_users_dids}")

    # get all content engaged with by study users, keyed on the URI of the post.
    # We do it in this way so that we can track all the ways that a given post is engaged
    # and by who. This is required since multiple users can engage with the same post, in
    # multiple different ways (e.g., one user likes a post, one shares it, etc.) and
    # the same user can even engage with one post in multiple ways (e.g., a user can both
    # retweet and like a post)
    engaged_content: dict[str, list[dict]] = get_engaged_content(
        valid_study_users_dids=valid_study_users_dids
    )
    print(f"Total number of posts engaged with in some way: {len(engaged_content)}")  # noqa

    # get all the content engaged with by user, keyed on user DID.
    # keyed on user, value = another dict, with key = date, value = engagements (like/post/repost/reply)
    # The idea here is we can know, for each user, on which days they engaged with content
    # on Bluesky, and for each of those days, what they did those days.
    user_to_content_engaged_with: dict[str, dict] = get_content_engaged_with_per_user(
        engaged_content=engaged_content
    )
    print(
        f"Total number of users who engaged with Bluesky content at some point: {len(user_to_content_engaged_with)}"
    )  # noqa

    # now that we have the engagement data loaded and we've organized it on a
    # per-user, per-day level, we now load the labels for that engagement data.
    engaged_content_uris = list(engaged_content.keys())
    labels_for_engaged_content: dict[str, dict] = get_labels_for_engaged_content(
        uris=engaged_content_uris
    )
    print(
        f"Loaded {len(labels_for_engaged_content)} labels for {len(engaged_content_uris)} posts engaged with."
    )  # noqa


def do_aggregations_and_export_results(
    user_df: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
    user_to_content_engaged_with: dict[str, dict],
    labels_for_engaged_content: dict[str, dict],
):
    """Perform aggregated analyses and export results.

    Aggregations are done at two levels:
    - (1) daily
    - (2) weekly.
    """

    # (1) Daily aggregations
    print("[Daily analysis] Getting per-user, per-day content label proportions...")

    # NOTE: stubs until we actually review these functions closely.
    def get_per_user_per_day_content_label_proportions():
        return

    def transform_per_user_per_day_content_label_proportions():
        return

    user_per_day_content_label_proportions = (
        get_per_user_per_day_content_label_proportions(
            user_to_content_engaged_with=user_to_content_engaged_with,
            labels_for_engaged_content=labels_for_engaged_content,
        )
    )

    transformed_per_user_per_day_content_label_proportions: pd.DataFrame = transform_per_user_per_day_content_label_proportions(
        user_per_day_content_label_proportions=user_per_day_content_label_proportions,
        users=user_df,
    )

    print(
        f"[Daily analysis] Exporting per-user, per-day content label proportions to {engaged_content_daily_aggregated_results_export_fp}..."
    )
    os.makedirs(
        os.path.dirname(engaged_content_daily_aggregated_results_export_fp),
        exist_ok=True,
    )
    transformed_per_user_per_day_content_label_proportions.to_csv(
        engaged_content_daily_aggregated_results_export_fp, index=False
    )
    print(
        f"[Daily analysis] Exported per-user, per-day content label proportions to {engaged_content_daily_aggregated_results_export_fp}..."
    )

    # (2) Weekly aggregations.
    print("[Weekly analysis] Getting per-user, per-week content label proportions...")

    # NOTE: stubs until we actually review these functions closely.
    def get_per_user_to_weekly_content_label_proportions():
        return

    def transform_per_user_to_weekly_content_label_proportions():
        return

    user_to_weekly_content_label_proportions: dict = get_per_user_to_weekly_content_label_proportions(
        user_per_day_content_label_proportions=user_per_day_content_label_proportions,
        user_date_to_week_df=user_date_to_week_df,
    )
    transformed_per_user_to_weekly_content_label_proportions: pd.DataFrame = transform_per_user_to_weekly_content_label_proportions(
        user_to_weekly_content_label_proportions=user_to_weekly_content_label_proportions,
        users=user_df,
        user_date_to_week_df=user_date_to_week_df,
    )

    print(
        f"[Weekly analysis] Exporting per-user, per-week content label proportions to {engaged_content_weekly_aggregated_results_export_fp}..."
    )
    os.makedirs(
        os.path.dirname(engaged_content_weekly_aggregated_results_export_fp),
        exist_ok=True,
    )
    transformed_per_user_to_weekly_content_label_proportions.to_csv(
        engaged_content_weekly_aggregated_results_export_fp, index=False
    )
    print(
        f"[Weekly analysis] Exported per-user, per-day content label proportions to {engaged_content_weekly_aggregated_results_export_fp}..."
    )


def main():
    """Execute the steps required for doing analysis of the content that users engaged
    with during the study, both at the daily and weekly aggregation levels."""

    setup_objs = do_setup()

    user_df = setup_objs["user_df"]
    user_date_to_week_df = setup_objs["user_date_to_week_df"]
    user_to_content_engaged_with = setup_objs["user_to_content_engaged_with"]
    labels_for_engaged_content = setup_objs["labels_for_engaged_content"]

    do_aggregations_and_export_results(
        user_df=user_df,
        user_date_to_week_df=user_date_to_week_df,
        user_to_content_engaged_with=user_to_content_engaged_with,
        labels_for_engaged_content=labels_for_engaged_content,
    )


if __name__ == "__main__":
    main()
