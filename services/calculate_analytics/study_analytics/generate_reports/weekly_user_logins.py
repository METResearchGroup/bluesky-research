"""Calculate the # of times each user has logged into the app, aggregated
by week."""

import os

import pandas as pd

from lib.constants import root_local_data_directory, timestamp_format
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.calculate_analytics.calculate_weekly_thresholds_per_user import (
    load_user_demographic_info,
)

logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))


def load_user_date_to_week_df() -> pd.DataFrame:
    """Load the user date to week mapping from the database."""
    fp = os.path.join(current_dir, "bluesky_per_user_week_assignments.csv")
    df = pd.read_csv(fp)
    df = df[["bluesky_handle", "date", "week_dynamic"]]
    df = df.rename(columns={"week_dynamic": "week"})
    return df


def main():
    consolidated_user_session_logs_path = os.path.join(
        root_local_data_directory,
        "analytics",
        "consolidated",
        "user_session_logs",
        "consolidated_user_session_logs.parquet",
    )

    consolidated_user_session_logs = pd.read_parquet(
        consolidated_user_session_logs_path
    )

    consolidated_user_session_logs["date"] = pd.to_datetime(
        consolidated_user_session_logs["timestamp"],
        format=timestamp_format,
    ).dt.date

    # Get value_counts by "user_did" per "date".
    user_date_to_logins_df = (
        consolidated_user_session_logs.groupby(["user_did", "date"])
        .size()
        .reset_index(name="logins")
    )  # type: ignore # noqa

    sorted_daily_logins = user_date_to_logins_df.sort_values(
        by=["user_did", "date"], ascending=[False, False]
    )

    # filter out bot users.
    sorted_daily_logins = sorted_daily_logins[
        ~sorted_daily_logins["user_did"].isin(["default", "test-did"])
    ]

    # join against user demographics, on user_did=bluesky_user_did
    user_demographics: pd.DataFrame = load_user_demographic_info()
    logger.info(f"Loaded user demographics with {len(user_demographics)} rows")

    print(
        f"Total number of user to daily logins before join: {len(sorted_daily_logins)}"
    )
    print(f"Total number of user demographics: {len(user_demographics)}")
    print(
        f"Expected number of user to daily logins after join: {len(sorted_daily_logins)}"
    )

    # get the users who logged in and match them with their demographic info.
    # since this is a left join, this excludes users who have not logged in.
    user_to_daily_logins_df = sorted_daily_logins.merge(
        user_demographics,
        left_on="user_did",
        right_on="bluesky_user_did",
        how="left",
    )

    print(
        f"Actual number of user to daily logins after join: {len(user_to_daily_logins_df)}"
    )
    print(f"Number of NaNs per column: {user_to_daily_logins_df.isna().sum()}")

    # Remove rows where bluesky_handle is NaN. These are NaN because those
    # users have been deleted from the study (hence, their data isn't in
    # DynamoDB).
    user_to_daily_logins_df = user_to_daily_logins_df.dropna(subset=["bluesky_handle"])
    print(
        f"Number of rows after removing NaN bluesky_handles: {len(user_to_daily_logins_df)}"
    )
    print(
        f"Number of NaNs per column after removing NaN bluesky_handles: {user_to_daily_logins_df.isna().sum()}"
    )

    user_to_daily_logins_df = user_to_daily_logins_df[
        ["bluesky_handle", "date", "logins", "condition"]
    ]

    # Sort the dataframe by bluesky_handle and date in ascending order
    user_to_daily_logins_df = user_to_daily_logins_df.sort_values(
        by=["bluesky_handle", "date"], ascending=[True, True]
    )

    # join against user date to weeks map.
    user_date_to_week_df = load_user_date_to_week_df()

    print(f"Total number of user to daily logins: {len(user_to_daily_logins_df)}")
    print(f"Total number of user date to week mappings: {len(user_date_to_week_df)}")
    print(
        f"Expected number of user to daily logins after join: {len(user_date_to_week_df)}"
    )

    user_to_daily_logins_df["date"] = user_to_daily_logins_df["date"].astype(str)
    user_date_to_week_df["date"] = user_date_to_week_df["date"].astype(str)

    # remove 'user_date_to_week_df' rows with bluesky_handle not in
    # 'user_to_daily_logins_df'
    user_date_to_week_df = user_date_to_week_df[
        user_date_to_week_df["bluesky_handle"].isin(
            user_to_daily_logins_df["bluesky_handle"]
        )
    ]

    # Join against user date to weeks map.
    joined_user_to_daily_logins_df = user_to_daily_logins_df.merge(
        user_date_to_week_df,
        left_on=["bluesky_handle", "date"],
        right_on=["bluesky_handle", "date"],
        how="right",
    )

    print(
        f"Actual number of user to daily logins after join: {len(joined_user_to_daily_logins_df)}"
    )

    # Impute the "condition" for any missing "condition" values. This'll be
    # missing for the dates where the user has not logged in.
    map_handle_to_condition = dict(
        zip(user_demographics["bluesky_handle"], user_demographics["condition"])
    )

    def impute_condition(row):
        if pd.isna(row["condition"]):
            return map_handle_to_condition[row["bluesky_handle"]]
        else:
            return row["condition"]

    joined_user_to_daily_logins_df["condition"] = joined_user_to_daily_logins_df.apply(
        impute_condition, axis=1
    )

    print(
        f"After imputing condition NaNs, number of NaNs per column: {joined_user_to_daily_logins_df.isna().sum()}"
    )

    # impute missing values for a date with 0
    joined_user_to_daily_logins_df["logins"] = joined_user_to_daily_logins_df[
        "logins"
    ].fillna(0)

    print(
        f"Number of NaNs per column after imputing missing logins: {joined_user_to_daily_logins_df.isna().sum()}"
    )

    joined_user_to_daily_logins_df = joined_user_to_daily_logins_df[
        ["bluesky_handle", "condition", "logins", "week"]
    ]

    # remove weeks with NaN.
    joined_user_to_daily_logins_df = joined_user_to_daily_logins_df[
        joined_user_to_daily_logins_df["week"].notna()
    ]

    # Get value_counts by "user_did" per "week".
    weekly_user_logins_df = (
        joined_user_to_daily_logins_df.groupby(["bluesky_handle", "week"])
        .agg(
            logins=("logins", "sum"),
            condition=("condition", "first"),
        )
        .reset_index()
    )  # type: ignore # noqa

    print(f"{weekly_user_logins_df.head()}")

    # order by bluesky_handle desc, week ascending
    weekly_user_logins_df = weekly_user_logins_df.sort_values(
        by=["bluesky_handle", "week"], ascending=[False, True]
    )

    # export to csv
    weekly_user_logins_df.to_csv(
        os.path.join(current_dir, "weekly_user_logins.csv"), index=False
    )


if __name__ == "__main__":
    main()
