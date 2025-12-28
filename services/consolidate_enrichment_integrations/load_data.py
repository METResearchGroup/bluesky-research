"""Load consolidated enriched post data."""

from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger


logger = get_logger(__name__)


def load_enriched_posts(
    latest_timestamp: Optional[str] = None,
) -> pd.DataFrame:
    """Load and validate consolidated enriched posts from local storage.

    Loads consolidated enriched post records, validates them against the
    Pydantic model schema, and returns them as a DataFrame.

    Args:
        latest_timestamp: Optional timestamp string to filter posts. If None,
            loads all available data.

    Returns:
        DataFrame containing validated consolidated enriched posts.

    Raises:
        ValidationError: If any post fails schema validation.
    """
    return load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        latest_timestamp=latest_timestamp,
    )
