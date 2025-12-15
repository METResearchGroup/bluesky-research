"""Load consolidated enriched post data."""

from typing import Optional

import pandas as pd
from pydantic import ValidationError

from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)

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
    loaded_posts_df: pd.DataFrame = load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        latest_timestamp=latest_timestamp,
    )
    df_dicts = loaded_posts_df.to_dict(orient="records")
    df_dicts = parse_converted_pandas_dicts(df_dicts)

    # Validate schemas of posts, then convert back to dataframe
    try:
        validated_post_models = [
            ConsolidatedEnrichedPostModel(**post) for post in df_dicts
        ]
    except ValidationError as e:
        logger.error(f"Failed to validate schemas of posts: {e}")
        raise

    posts_df: pd.DataFrame = pd.DataFrame(
        [post.model_dump() for post in validated_post_models]
    )
    return posts_df
