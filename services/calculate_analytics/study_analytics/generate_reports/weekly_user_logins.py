"""Calculate the # of times each user has logged into the app, aggregated
by week."""

import os

import pandas as pd

from lib.constants import root_local_data_directory, timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.calculate_analytics.calculate_weekly_thresholds_per_user import (
    load_user_demographic_info,
)

logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))


def load_feeds() -> pd.DataFrame:
    df = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        # start_partition_date="2024-09-28",
        start_partition_date="2024-11-28",
        end_partition_date="2024-12-01",
    )
    return df


def load_daily_logins_for_user(user_did: str) -> dict[str, int]:
    """Load the daily logins for a user from the database.

    Keys = YYYY-MM-DD, values = # of logins on that day.
    """
    return {}


def load_user_date_to_week_df() -> pd.DataFrame:
    """Load the user date to week map from the database.

    Columns:
    - user_did
    - date
    - week
    """
    return pd.DataFrame()


def load_weekly_logins_for_user(
    user_did: str, user_date_to_week_map: dict[str, str]
) -> dict[str, int]:
    """Load the weekly logins for a user from the database.

    Keys = YYYY-MM-DD, values = # of logins on that week.
    """
    daily_logins_for_user = load_daily_logins_for_user(user_did=user_did)
    weekly_logins_for_user = {}
    for week in ["1", "2", "3", "4", "5", "6", "7", "8"]:
        weekly_logins_for_user[week] = 0
    for date, logins in daily_logins_for_user.items():
        week = user_date_to_week_map[date]
        weekly_logins_for_user[week] += logins
    return weekly_logins_for_user


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

    # filter out 'user_did' = default
    sorted_daily_logins = sorted_daily_logins[
        sorted_daily_logins["user_did"] != "default"
    ]

    # TODO: join against user date to weeks map.
    user_date_to_week_df = load_user_date_to_week_df()  # TODO: write this.

    # Join against user date to weeks map.
    sorted_daily_logins = sorted_daily_logins.merge(
        user_date_to_week_df, on="date", how="left"
    )

    # Get value_counts by "user_did" per "week".
    user_week_to_logins_df = (
        sorted_daily_logins.groupby(["user_did", "week"])
        .size()
        .reset_index(name="logins")
    )  # type: ignore # noqa

    # join against user demographics, on user_did=bluesky_user_did
    user_demographics: pd.DataFrame = load_user_demographic_info()
    logger.info(f"Loaded user demographics with {len(user_demographics)} rows")

    user_week_to_logins_df = user_week_to_logins_df.merge(
        user_demographics, left_on="user_did", right_on="bluesky_user_did", how="left"
    )

    # export to csv
    user_week_to_logins_df.to_csv(
        os.path.join(current_dir, "weekly_user_logins.csv"), index=False
    )


if __name__ == "__main__":
    main()
