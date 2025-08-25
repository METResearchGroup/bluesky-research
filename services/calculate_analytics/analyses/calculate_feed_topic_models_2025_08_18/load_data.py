"""
This module contains the DataLoader class, which is used to load data from the local or production environment.
"""

import pandas as pd

from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.shared.data_loading.posts import (
    load_preprocessed_posts,
)


class DataLoader:
    """DataLoader for topic modeling analysis."""

    def __init__(self, mode: str):
        self.mode = mode

    def _load_local_data(self):
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

    def _load_prod_data(self):
        """Prod data loader. Loads all posts used in feeds.

        TODO: need to also make sure that these posts also have associated metadata
        so that we can slice them by condition, etc.
        """
        pass

    def load_data(self):
        if self.mode == "local":
            return self._load_local_data()
        elif self.mode == "prod":
            return self._load_prod_data()
        else:
            raise ValueError(f"Invalid mode: {self.mode}")
