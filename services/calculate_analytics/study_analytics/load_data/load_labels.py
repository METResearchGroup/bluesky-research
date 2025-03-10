import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

logger = get_logger(__file__)


def get_perspective_api_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Get the Perspective API labels for a list of posts."""
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    logger.info(
        f"Loaded {len(df)} Perspective API labels for partition date {partition_date}"
    )
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} Perspective API labels for partition date {partition_date}"
    )
    return df


def get_sociopolitical_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Get the sociopolitical labels for a list of posts."""
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_sociopolitical",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    logger.info(
        f"Loaded {len(df)} sociopolitical labels for partition date {partition_date}"
    )
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} sociopolitical labels for partition date {partition_date}"
    )
    return df


def get_ime_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Get the IME labels for a list of posts."""
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    logger.info(f"Loaded {len(df)} IME labels for partition date {partition_date}")
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(f"Filtered to {len(df)} IME labels for partition date {partition_date}")
    return df
