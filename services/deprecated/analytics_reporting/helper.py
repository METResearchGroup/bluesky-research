"""Helper functions for analytics reporting."""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from lib.aws.athena import Athena
from lib.log.logger import get_logger

from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

athena = Athena()
study_users: list[UserToBlueskyProfileModel] = get_all_users()
study_users_df = pd.DataFrame(
    [
        {
            "bluesky_user_did": user.bluesky_user_did,
            "bluesky_handle": user.bluesky_handle,
        }
        for user in study_users
    ]
)

lookback_days = 12
partition_start_date = (datetime.now() - timedelta(days=lookback_days)).strftime(
    "%Y-%m-%d"
)
current_date = datetime.now().strftime("%Y-%m-%d")

logger = get_logger(__name__)


def load_user_session_logs_to_df(
    partition_start_date: Optional[str] = None,
) -> pd.DataFrame:
    where_clause = (
        f"WHERE partition_date >= '{partition_start_date}'"
        if partition_start_date
        else ""
    )
    query = f"SELECT * FROM user_session_logs {where_clause}"
    df = athena.query_results_as_df(query=query)
    return df


def main():
    logger.info(f"Starting fetching usage data from Athena for {lookback_days} days...")
    df = load_user_session_logs_to_df(partition_start_date=partition_start_date)
    # drop testing rows.
    df = df[~df["user_did"].isin(["default", "test-did"])]
    # join can be less than # of rows of df, but only if user_did=default is included
    # (from previous testing). Otherwise should be the same shape.
    joined_df = pd.merge(
        df, study_users_df, left_on="user_did", right_on="bluesky_user_did", how="inner"
    )
    joined_df = joined_df.sort_values(by="partition_date", ascending=False)

    # export raw rows of usage to .csv
    filename = f"user_session_logs_{partition_start_date}_{current_date}.csv"
    joined_df.to_csv(filename, index=False)

    # export counts per user per day to .csv
    counts_df = (
        joined_df.groupby(["bluesky_handle", "partition_date"])
        .size()
        .reset_index(name="count")
    )
    counts_df = counts_df.rename(columns={"partition_date": "login_date"})
    counts_filename = (
        f"user_session_logs_counts_{partition_start_date}_{current_date}.csv"
    )
    counts_df.to_csv(counts_filename, index=False)
    logger.info(f"Saved to {counts_filename}")


if __name__ == "__main__":
    main()
