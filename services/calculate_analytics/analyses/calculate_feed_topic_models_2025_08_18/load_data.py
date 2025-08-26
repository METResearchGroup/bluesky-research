"""
This module contains the DataLoader class, which is used to load data from the local or production environment.
"""

import pandas as pd

from lib.helper import get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    map_users_to_posts_used_in_feeds,
)
from services.calculate_analytics.shared.data_loading.posts import (
    load_preprocessed_posts,
    load_preprocessed_posts_by_uris,
)
from services.calculate_analytics.shared.data_loading.users import (
    get_user_condition_mapping,
)

logger = get_logger(__name__)


class DataLoader:
    """DataLoader for topic modeling analysis."""

    def __init__(self, mode: str):
        self.mode = mode

    def _load_local_data(self) -> pd.DataFrame:
        """Local data loader. Just loads preprocessed posts."""
        table_columns = ["text", "partition_date"]
        table_columns_str = ", ".join(table_columns)
        sort_filter = "ORDER BY partition_date ASC"
        query = (
            f"SELECT {table_columns_str} "
            f"FROM preprocessed_posts "
            f"WHERE text IS NOT NULL "
            f"AND text != '' "
            f"{sort_filter}"
        ).strip()
        df: pd.DataFrame = load_preprocessed_posts(
            lookback_start_date=STUDY_START_DATE,
            lookback_end_date=STUDY_END_DATE,
            duckdb_query=query,
            query_metadata={
                "tables": [{"name": "preprocessed_posts", "columns": table_columns}]
            },
            export_format="duckdb",
        )
        return df

    def _load_prod_data(self) -> tuple[dict[str, dict[str, set[str]]], pd.DataFrame]:
        """
        Accumulate posts used in feeds for each condition and date.

        Args:
            partition_dates: list of partition dates
            user_condition_mapping: mapping from user DID to condition

        Returns:
            tuple[dict[str, dict[str, set[str]]], pd.DataFrame]: mapping from date to mapping from condition to set of post URIs, and a dataframe of all texts.

        We need to also make sure that these posts also have associated metadata
        so that we can slice them by condition, etc.

        We need to slice by:
        - Posts used on particular dates
        - Condition
        - Dates x condition

        We need to load all post URIs, but then also separately have a data structure
        that, for a given day, every day, track the post URIs.

        That data structure should be something like:

        {
            "<date>": {
                "<condition>": set() # set of post URIs
            }
        }

        Return a tuple of:
        - date_condition_uris_map: dict[str, dict[str, set[str]]]: mapping from date to mapping from condition to set of post URIs.
            - We will use this to splice the topic model to analyze by date and condition.
        - all_texts: set[str]: set of all texts across all partition dates. This is what we'll use to train the topic model.
        """
        # load users.
        user_condition_mapping: dict[str, str] = get_user_condition_mapping()

        # load posts used in feeds. Accumulate post URIs used in feeds for each condition.
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )

        table_columns = ["uri", "text", "partition_date"]
        table_columns_str = ", ".join(table_columns)

        sort_filter = "ORDER BY partition_date ASC"
        query = (
            f"SELECT {table_columns_str} "
            f"FROM preprocessed_posts "
            f"WHERE text IS NOT NULL "
            f"AND text != '' "
            f"{sort_filter}"
        ).strip()

        # keep a running set of all texts.
        all_texts = set()

        date_condition_uris_map: dict[str, dict[str, set[str]]] = {}
        for partition_date in partition_dates:
            users_to_posts_used_in_feeds: dict[str, set[str]] = (
                map_users_to_posts_used_in_feeds(partition_date)
            )
            invalid_users = set()

            # initialize the date_condition_uris_map for the given partition date.
            if partition_date not in date_condition_uris_map:
                date_condition_uris_map[partition_date] = {}
            condition_to_post_uris: dict[str, set[str]] = {}
            condition_to_post_uris_count: dict[str, int] = {}
            # for the given partition date, accumulate post URIs by condition.
            for user, post_uris in users_to_posts_used_in_feeds.items():
                try:
                    condition = user_condition_mapping[user]
                    if condition not in condition_to_post_uris:
                        condition_to_post_uris[condition] = set()
                    if condition not in condition_to_post_uris_count:
                        condition_to_post_uris_count[condition] = 0
                    condition_to_post_uris[condition].update(post_uris)
                    condition_to_post_uris_count[condition] += 1
                except Exception:
                    invalid_users.add(user)

            # given these URIs, load the posts and add to the running set of all texts.
            all_post_uris = set()
            for condition, post_uris in condition_to_post_uris.items():
                all_post_uris.update(post_uris)
            all_post_uris_df = pd.DataFrame(list(all_post_uris), columns=["uri"])
            all_post_texts_df: pd.DataFrame = load_preprocessed_posts_by_uris(
                uris=all_post_uris_df,
                partition_date=partition_date,
                duckdb_query=query,
                query_metadata={
                    "tables": [{"name": "preprocessed_posts", "columns": table_columns}]
                },
                export_format="duckdb",
            )
            total_posts_before_adding_new_posts = len(all_texts)
            for text in all_post_texts_df["text"]:
                all_texts.add(text)
            total_posts_after_adding_new_posts = len(all_texts)
            logger.info(
                f"[{partition_date}] Total posts before adding new posts: {total_posts_before_adding_new_posts}\tTotal posts after adding new posts: {total_posts_after_adding_new_posts}"
            )

            # then, add the condition_to_post_uris to the date_condition_uris_map.
            date_condition_uris_map[partition_date] = condition_to_post_uris

            # expected that some wont' be found (maybe ~30) due to test users who
            # we added during our pilot.
            logger.info(
                f"[{partition_date}] Total URIs for partition date {partition_date}, by condition: {condition_to_post_uris_count}\tTotal invalid users for partition date {partition_date}: {len(invalid_users)}"
            )

        text_df = pd.DataFrame(list(all_texts), columns=["text"])
        return date_condition_uris_map, text_df

    def load_data(self):
        if self.mode == "local":
            return dict(), self._load_local_data()
        elif self.mode == "prod":
            return self._load_prod_data()
        else:
            raise ValueError(f"Invalid mode: {self.mode}")
