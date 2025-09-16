"""
This module contains the DataLoader class, which is used to load data from the local or production environment.
"""

import hashlib
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
)
from services.calculate_analytics.shared.data_loading.labels import (
    load_sociopolitical_labels_by_uris,
)
from services.calculate_analytics.shared.data_loading.users import (
    get_user_condition_mapping,
)

logger = get_logger(__name__)


class DataLoader:
    """DataLoader for topic modeling analysis."""

    def __init__(self, mode: str, run_mode: str = "full"):
        self.mode = mode
        self.run_mode = run_mode  # "train" or "inference" or "full"
        self.sample_per_day = 500  # Hardcoded sampling for training mode

    def _canonicalize_text(self, text: str) -> str:
        """Canonicalize text for stable hashing and deduplication."""
        if text is None:
            return ""
        # Normalize whitespace and lowercase; avoid heavy transforms to preserve semantics
        return " ".join(str(text).split()).lower()

    def _compute_doc_id(self, text: str) -> str:
        """Compute stable doc_id from text."""
        canon = self._canonicalize_text(text)
        return hashlib.sha256(canon.encode("utf-8")).hexdigest()

    def _postprocess_documents(
        self, raw_data: pd.DataFrame, has_uris: bool = False
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Postprocess raw document data into standardized format.

        Args:
            raw_data: DataFrame with at least 'text' column, optionally 'uri' and 'partition_date'
            has_uris: Whether the data includes URI information for mapping

        Returns:
            tuple: (documents_df, uri_doc_map)
                - documents_df: DataFrame with columns [doc_id, text] (deduplicated documents)
                - uri_doc_map: DataFrame with columns [uri, partition_date, doc_id] (empty if has_uris=False)
        """
        # Create documents DataFrame with doc_id
        documents_df = raw_data[["text"]].copy()
        documents_df["doc_id"] = documents_df["text"].apply(self._compute_doc_id)
        documents_df = documents_df.drop_duplicates(subset=["doc_id"])[
            ["doc_id", "text"]
        ]

        # Create URI-document mapping if URIs are available
        if (
            has_uris
            and "uri" in raw_data.columns
            and "partition_date" in raw_data.columns
        ):
            uri_doc_rows = []
            for row in raw_data.itertuples(index=False):
                uri = row.uri
                text = row.text
                partition_date = row.partition_date
                doc_id = self._compute_doc_id(text)
                uri_doc_rows.append(
                    {"uri": uri, "partition_date": partition_date, "doc_id": doc_id}
                )
            uri_doc_map = pd.DataFrame(
                uri_doc_rows, columns=["uri", "partition_date", "doc_id"]
            )
        else:
            # No URIs in local mode; return empty mapping with expected columns
            uri_doc_map = pd.DataFrame(columns=["uri", "partition_date", "doc_id"])

        return documents_df, uri_doc_map

    def _load_sociopolitical_labels_for_conditions(
        self,
        condition_to_post_uris: dict[str, set[str]],
        partition_date: str,
        query: str,
        table_columns: list[str],
    ) -> pd.DataFrame:
        """Load sociopolitical labels for posts across all conditions for a given partition date.

        Args:
            condition_to_post_uris: Mapping from condition to set of post URIs
            partition_date: The partition date to load labels for
            query: DuckDB query for filtering data
            table_columns: List of columns to select from the table

        Returns:
            DataFrame containing sociopolitical labels with sampling applied if in training mode
        """
        # Collect all post URIs across all conditions
        all_post_uris = set()
        for condition, post_uris in condition_to_post_uris.items():
            all_post_uris.update(post_uris)

        # Load sociopolitical labels for all URIs
        all_post_texts_df: pd.DataFrame = load_sociopolitical_labels_by_uris(
            uris=all_post_uris,
            partition_date=partition_date,
            duckdb_query=query,
            query_metadata={
                "tables": [
                    {"name": "ml_inference_sociopolitical", "columns": table_columns}
                ]
            },
            export_format="duckdb",
        )

        # Apply sampling for training mode
        if self.run_mode == "train" and len(all_post_texts_df) > self.sample_per_day:
            all_post_texts_df = all_post_texts_df.sample(
                n=self.sample_per_day, random_state=42
            ).reset_index(drop=True)
            logger.info(
                f"[{partition_date}] Sampled {len(all_post_texts_df)} posts for training (from {len(all_post_uris)} total URIs)"
            )

        return all_post_texts_df

    def _accumulate_post_uris_by_condition(
        self,
        users_to_posts_used_in_feeds: dict[str, set[str]],
        user_condition_mapping: dict[str, str],
    ) -> tuple[dict[str, set[str]], dict[str, int], set[str]]:
        """Accumulate post URIs by condition."""
        condition_to_post_uris: dict[str, set[str]] = {}
        condition_to_post_uris_count: dict[str, int] = {}
        invalid_users: set[str] = set()
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
        return condition_to_post_uris, condition_to_post_uris_count, invalid_users

    def _build_documents_for_partition_date(
        self,
        all_post_texts_df: pd.DataFrame,
        partition_date: str,
    ) -> tuple[dict[str, str], list[dict[str, str]]]:
        """Build documents for a partition date."""
        documents_map: dict[str, str] = {}
        uri_doc_rows: list[dict[str, str]] = []
        for row in all_post_texts_df.itertuples(index=False):
            uri = row.uri
            text = row.text
            doc_id = self._compute_doc_id(text)
            # Preserve first seen raw text for the doc_id
            if doc_id not in documents_map:
                documents_map[doc_id] = text
            uri_doc_rows.append(
                {"uri": uri, "partition_date": partition_date, "doc_id": doc_id}
            )
        return documents_map, uri_doc_rows

    def _load_posts_and_build_documents_for_partition_date(
        self, partition_date: str, user_condition_mapping: dict[str, str]
    ):
        """Load posts and build documents for a partition date."""

        users_to_posts_used_in_feeds: dict[str, set[str]] = (
            map_users_to_posts_used_in_feeds(partition_date=partition_date)
        )
        invalid_users = set()
        date_condition_uris_map: dict[str, dict[str, set[str]]] = {}

        # initialize the date_condition_uris_map for the given partition date.
        if partition_date not in date_condition_uris_map:
            date_condition_uris_map[partition_date] = {}
        condition_to_post_uris: dict[str, set[str]] = {}
        condition_to_post_uris_count: dict[str, int] = {}

        # for the given partition date, accumulate post URIs by condition.
        condition_to_post_uris, condition_to_post_uris_count, invalid_users = (
            self._accumulate_post_uris_by_condition(
                users_to_posts_used_in_feeds=users_to_posts_used_in_feeds,
                user_condition_mapping=user_condition_mapping,
            )
        )

        # given these URIs, load the posts and add to the running set of all texts.
        table_columns = ["uri", "text", "partition_date"]
        table_columns_str = ", ".join(table_columns)

        # Load sociopolitical labels, but only for posts that are labeled
        # as sociopolitical.
        sort_filter = "ORDER BY partition_date ASC"
        query = (
            f"SELECT {table_columns_str} "
            f"FROM ml_inference_sociopolitical "
            f"WHERE text IS NOT NULL "
            f"AND text != '' "
            f"AND is_sociopolitical = True "
            f"{sort_filter}"
        ).strip()

        all_post_texts_df: pd.DataFrame = (
            self._load_sociopolitical_labels_for_conditions(
                condition_to_post_uris=condition_to_post_uris,
                partition_date=partition_date,
                query=query,
                table_columns=table_columns,
            )
        )

        # Build documents and uri->doc_id mapping for this partition_date
        updated_documents_map, updated_uri_doc_rows = (
            self._build_documents_for_partition_date(
                all_post_texts_df=all_post_texts_df,
                partition_date=partition_date,
            )
        )

        # then, add the condition_to_post_uris to the date_condition_uris_map.
        date_condition_uris_map[partition_date] = condition_to_post_uris

        # expected that some won't be found (maybe ~30) due to test users who
        # we added during our pilot.
        logger.info(
            f"[{partition_date}] Total URIs for partition date {partition_date}, by condition: {condition_to_post_uris_count}\tTotal invalid users for partition date {partition_date}: {len(invalid_users)}"
        )

        return updated_documents_map, updated_uri_doc_rows, date_condition_uris_map

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

    def _load_prod_data(
        self,
    ) -> tuple[dict[str, dict[str, set[str]]], pd.DataFrame, pd.DataFrame]:
        """
        Accumulate posts used in feeds for each condition and date.

        Args:
            partition_dates: list of partition dates
            user_condition_mapping: mapping from user DID to condition

        Returns:
            tuple[
                dict[str, dict[str, set[str]]],
                pd.DataFrame,
                pd.DataFrame,
            ]: (
                date_condition_uris_map,  # mapping from date to mapping from condition to set of post URIs
                documents_df,             # deduplicated documents for training: columns [doc_id, text]
                uri_doc_map,              # lookup to reconstruct slices: columns [uri, partition_date, doc_id]
            )

        We need to also make sure that these posts also have associated metadata
        so that we can slice them by condition, etc.

        We need to slice by:
        - Posts used on particular dates
        - Condition
        - Dates x condition

        We need to load all post URIs, but then also separately have a data structure
        that, for a given day, every day, track the post URIs.

        We want to get just the posts that are sociopolitical because our local
        experiments showed us that non-sociopolitical posts just add noise to
        our analysis, so we explicitly filter for posts about social or political
        topics.

        We generate doc_id for each text and the doc_id is the stable hash of the text.
        This is better than using the URI because two posts can be identical in content
        but have different URIs.

        That data structure should be something like:

        {
            "<date>": {
                "<condition>": set() # set of post URIs
            }
        }

        Return a tuple of:
        - date_condition_uris_map: dict[str, dict[str, set[str]]]: mapping from date to mapping from condition to set of post URIs.
            - We will use this to splice the topic model to analyze by date and condition.
        - documents_df: unique texts across all dates (deduped by stable doc_id) for model training.
        - uri_doc_map: mapping from uri(+partition_date) to doc_id to reconstruct slice-level aggregations.
        """
        # load users.
        user_condition_mapping: dict[str, str] = get_user_condition_mapping()

        # load posts used in feeds. Accumulate post URIs used in feeds for each condition.
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )

        # Collect unique documents and the uri->doc mapping
        # documents_map: doc_id -> text (first observed canonical representative)
        documents_map: dict[str, str] = {}
        uri_doc_rows: list[dict[str, str]] = []
        date_condition_uris_map: dict[str, dict[str, set[str]]] = {}

        for partition_date in partition_dates:
            # load posts and build documents for the given partition date.
            (
                updated_documents_map,
                updated_uri_doc_rows,
                updated_date_condition_uris_map,
            ) = self._load_posts_and_build_documents_for_partition_date(
                partition_date=partition_date,
                user_condition_mapping=user_condition_mapping,
            )

            total_docs_before = len(documents_map)

            documents_map.update(updated_documents_map)
            uri_doc_rows.extend(updated_uri_doc_rows)
            date_condition_uris_map.update(updated_date_condition_uris_map)

            total_docs_after = len(documents_map)
            logger.info(
                f"[{partition_date}] Unique documents before: {total_docs_before}\tafter: {total_docs_after}"
            )

        # Finalize DataFrames
        documents_df = pd.DataFrame(
            [(doc_id, text) for doc_id, text in documents_map.items()],
            columns=["doc_id", "text"],
        )
        uri_doc_map = pd.DataFrame(
            uri_doc_rows, columns=["uri", "partition_date", "doc_id"]
        )
        return date_condition_uris_map, documents_df, uri_doc_map

    def load_data(self):
        if self.mode == "local":
            # Load raw data from local source
            base_df = self._load_local_data()

            # Use abstracted postprocessing for local data (no URIs)
            documents_df, uri_doc_map = self._postprocess_documents(
                base_df, has_uris=False
            )

            # No date_condition_uris_map for local mode
            return dict(), documents_df, uri_doc_map
        elif self.mode == "prod":
            return self._load_prod_data()
        else:
            raise ValueError(f"Invalid mode: {self.mode}")
