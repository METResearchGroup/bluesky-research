# TODO: refactor as necessary. This is just a first pass.

import pandas as pd

from lib.helper import get_partition_dates
from lib.log.logger import get_logger


start_date = "2024-10-01"
end_date = "2024-12-01"
exclude_partition_dates = ["2024-10-08"]

logger = get_logger(__file__)


def get_posts_used_in_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Get the posts used in feeds for a given partition date."""
    pass


def get_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    pass


def get_perspective_api_labels_for_posts(
    posts: list[dict], partition_date: str
) -> pd.DataFrame:
    pass


def get_ime_labels_for_posts(posts: list[dict], partition_date: str) -> pd.DataFrame:
    pass


def get_sociopolitical_labels_for_posts(
    posts: list[dict], partition_date: str
) -> pd.DataFrame:
    pass


def get_hydrated_posts_for_partition_date(partition_date: str) -> pd.DataFrame:
    """TODO: Join everything together here."""
    pass


def get_per_user_feed_averages_for_partition_date(partition_date: str) -> pd.DataFrame:
    """For each user, calculates the average feed content for a given partition
    date.

    For example, what was the average % of toxicity of the posts that appeared
    in the user's feed on the given date? How about the average % of political
    posts? The average % of IME labels?
    """
    pass


def get_per_user_feed_averages_for_study() -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    for partition_date in partition_dates:
        logger.info(f"Getting per-user averages for partition date: {partition_date}")
        dfs.append(get_hydrated_posts_for_partition_date(partition_date))
    logger.info("Concatenating all dataframes...")
    return pd.concat(dfs)


def load_user_demographic_info() -> pd.DataFrame:
    # TODO: load user handle, demographics from DynamoDB (think I do this elsewhere?)
    # TODO: drop duplicates.
    pass


def main():
    per_user_averages: pd.DataFrame = get_per_user_feed_averages_for_study()
    user_demographics: pd.DataFrame = load_user_demographic_info()
    joined_df = per_user_averages.merge(user_demographics, on="user_did", how="left")
    assert len(joined_df) == len(
        per_user_averages
    ), f"Expected {len(per_user_averages)} rows after join but got {len(joined_df)}"


if __name__ == "__main__":
    main()
