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

    user_to_daily_logins_df = sorted_daily_logins.merge(
        user_demographics,
        left_on="user_did",
        right_on="bluesky_user_did",
        how="right",
    )

    # TODO: figure out why 'condition' is NaN... should be no NaNs.
    breakpoint()

    print(
        f"Actual number of user to daily logins after join: {len(user_to_daily_logins_df)}"
    )
    print(f"Number of NaNs per column: {user_to_daily_logins_df.isna().sum()}")

    user_to_daily_logins_df = user_to_daily_logins_df[
        ["bluesky_handle", "date", "logins", "condition"]
    ]

    # join against user date to weeks map.
    user_date_to_week_df = load_user_date_to_week_df()

    print(f"Total number of user to daily logins: {len(user_to_daily_logins_df)}")
    print(f"Total number of user date to week mappings: {len(user_date_to_week_df)}")
    print(
        f"Expected number of user to daily logins after join: {len(user_date_to_week_df)}"
    )

    # Join against user date to weeks map.
    user_to_daily_logins_df = user_to_daily_logins_df.merge(
        user_date_to_week_df,
        left_on=["bluesky_handle", "date"],
        right_on=["bluesky_handle", "date"],
        how="right",
    )
    print(
        f"Actual number of user to daily logins after join: {len(user_to_daily_logins_df)}"
    )
    print(f"Number of NaNs per column: {user_to_daily_logins_df.isna().sum()}")

    # impute missing values for a date with 0
    user_to_daily_logins_df["logins"] = user_to_daily_logins_df["logins"].fillna(0)
    print(
        f"Number of NaNs per column after imputation: {user_to_daily_logins_df.isna().sum()}"
    )

    user_to_daily_logins_df = user_to_daily_logins_df[
        ["bluesky_handle", "condition", "logins", "week"]
    ]

    # Get value_counts by "user_did" per "week".
    user_week_to_logins_df = (
        sorted_daily_logins.groupby(["user_did", "week"])
        .size()
        .reset_index(name="logins")
    )  # type: ignore # noqa
    print(
        f"(Expected: 0): Number of NaNs per column after imputation: {user_week_to_logins_df.isna().sum()}"
    )
    print(f"{user_week_to_logins_df.head()=}")

    # order by user_did desc, week ascending
    user_week_to_logins_df = user_week_to_logins_df.sort_values(
        by=["user_did", "week"], ascending=[False, True]
    )

    breakpoint()

    # export to csv
    user_week_to_logins_df.to_csv(
        os.path.join(current_dir, "weekly_user_logins.csv"), index=False
    )


if __name__ == "__main__":
    main()
