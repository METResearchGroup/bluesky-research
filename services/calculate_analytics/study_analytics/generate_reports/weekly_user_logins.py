"""Calculate the # of times each user has logged into the app, aggregated
by week."""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.calculate_analytics.calculate_weekly_thresholds_per_user import (
    load_user_demographic_info,
)

logger = get_logger(__name__)


def load_feeds() -> pd.DataFrame:
    df = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        start_partition_date="2024-09-28",
        end_partition_date="2025-12-01",
    )
    return df


def load_daily_logins_for_user(user_did: str) -> dict[str, int]:
    """Load the daily logins for a user from the database.

    Keys = YYYY-MM-DD, values = # of logins on that day.
    """
    pass


def load_user_date_to_week_df() -> pd.DataFrame:
    """Load the user date to week map from the database.

    Columns:
    - user_did
    - date
    - week
    """
    pass


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
    user_demographics: pd.DataFrame = load_user_demographic_info()
    logger.info(f"Loaded user demographics with {len(user_demographics)} rows")

    # files I need from Quest.
    # bluesky_per_user_week_assignments.csv
    # feeds are in "generated_feeds" DB.

    # user_dids = []
    # user_date_to_week_df: pd.DataFrame = load_user_date_to_week_df()


if __name__ == "__main__":
    main()
