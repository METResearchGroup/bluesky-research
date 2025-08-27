"""Shared label data loading functionality for analytics system.

This module provides unified interfaces for loading various types of ML labels
used across the analytics system, eliminating code duplication and
ensuring consistent data handling patterns.
"""

import gc
from typing import Literal, Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_CONTENT_EARLIEST_LOOKBACK_DATE,
    STUDY_END_DATE,
)

logger = get_logger(__file__)


def get_perspective_api_labels(
    lookback_start_date: str,
    lookback_end_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
):
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        export_format=export_format,
    )
    logger.info(
        f"Loaded {len(df)} Perspective API labels for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df


def get_perspective_api_labels_for_posts(
    posts: pd.DataFrame,
    lookback_start_date: str,
    lookback_end_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
) -> pd.DataFrame:
    """Get the Perspective API labels for a list of posts.

    Args:
        posts: DataFrame containing posts with 'uri' column
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing Perspective API labels filtered to the given posts
    """
    df: pd.DataFrame = get_perspective_api_labels(
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        export_format=export_format,
    )

    # Filter to only include labels for the given posts
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} Perspective API labels for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df


def get_labels_for_partition_date(
    integration: str, partition_date: str
) -> pd.DataFrame:
    """Loads deduplicated labels for a given partition date."""
    df = load_data_from_local_storage(
        service=f"ml_inference_{integration}",
        directory="cache",
        partition_date=partition_date,
    )
    df = df.drop_duplicates(subset=["uri"])
    return df


def get_relevant_probs_for_label(integration: str, label_dict: dict):
    if integration == "perspective_api":
        return {
            "prob_toxic": label_dict["prob_toxic"],
            "prob_constructive": label_dict["prob_constructive"],
        }
    elif integration == "sociopolitical":
        return {
            "is_sociopolitical": label_dict["is_sociopolitical"],
            "is_not_sociopolitical": not label_dict["is_sociopolitical"],
            # TODO: need to double-check the formatting of these labels.
            "is_political_left": (label_dict["political_ideology_label"] == "left"),
            "is_political_right": (label_dict["political_ideology_label"] == "right"),
            "is_political_moderate": (
                label_dict["political_ideology_label"] == "moderate"
            ),
            "is_political_unclear": (
                label_dict["political_ideology_label"] == "unclear"
            ),
        }
    elif integration == "ime":
        return {
            "prob_intergroup": label_dict["prob_intergroup"],
            "prob_moral": label_dict["prob_moral"],
            "prob_emotion": label_dict["prob_emotion"],
            "prob_other": label_dict["prob_other"],
        }
    elif integration == "valence_classifier":
        return {
            "valence_clf_score": label_dict[
                "compound"
            ],  # raw score given by valence classifier
            "is_valence_positive": label_dict["valence_label"] == "positive",
            "is_valence_negative": label_dict["valence_label"] == "negative",
            "is_valence_neutral": label_dict["valence_label"] == "neutral",
        }
    else:
        raise ValueError(f"Invalid integration for labeling: {integration}")


def get_labels_for_engaged_content(engaged_content_uris: list[str]) -> dict:
    """For the content engaged with, get their associated labels.

    Algorithm:
        - Create a hash map:
            {
                "<uri>": {
                    "prob_toxic",
                    "prob_constructive",
                    ...
                }
            }

        - For each integration
            - For each partition date range
                - Load in the labels.
                - Filter for the subset of labels that are in the
                'engaged_content' keys.
                - Update the hash map with the labels from that integration.

    Result should look something like this:
        {
            "<uri_1>": {
                "prob_toxic": 0.001,
                "prob_constructive": 0.0002,
                ...
            },
            "<uri_2>": {
                "prob_toxic": 0.0001,
                "prob_constructive": 0.0002,
                ...
            },
            ...
        }
    """
    uri_to_labels_map = {}

    uris_to_pending_integrations: dict[str, set[str]] = {}

    for uri in engaged_content_uris:
        uri_to_labels_map[uri] = {}
        # pop each of these from the set as they're hydrated.
        uris_to_pending_integrations[uri] = {
            "perspective_api",
            "sociopolitical",
            "ime",
            "valence_classifier",
        }

    partition_dates: list[str] = get_partition_dates(
        start_date=STUDY_CONTENT_EARLIEST_LOOKBACK_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=[],
    )

    for integration in [
        "perspective_api",
        "sociopolitical",
        "ime",
        "valence_classifier",
    ]:
        # load day-by-day labels for each integration, and filter for only the
        # relevant URIs.
        filtered_uris = set()
        for partition_date in partition_dates:
            labels_df: pd.DataFrame = get_labels_for_partition_date(
                integration=integration, partition_date=partition_date
            )
            labels_df = labels_df[labels_df["uri"].isin(engaged_content_uris)]
            labels = labels_df.to_dict(orient="records")
            for label_dict in labels:
                # get the labels formatted in the way that we care about.
                relevant_label_probs = get_relevant_probs_for_label(
                    integration=integration, label_dict=label_dict
                )
                post_uri = label_dict["uri"]
                filtered_uris.add(post_uri)
                uri_to_labels_map[post_uri] = {
                    **uri_to_labels_map[post_uri],
                    **relevant_label_probs,
                }
            del labels_df
            gc.collect()

        # remove from list of pending integrations.
        for uri in filtered_uris:
            uris_to_pending_integrations[uri].remove(integration)
            if len(uris_to_pending_integrations[uri]) == 0:
                uris_to_pending_integrations.pop(uri)

    if len(uris_to_pending_integrations) > 0:
        print(
            f"We have {len(uris_to_pending_integrations)}/{len(engaged_content_uris)} still missing some integration of some sort."
        )  # noqa
        integration_to_missing_uris = {}
        for uri, integrations in uris_to_pending_integrations.items():
            for integration in integrations:
                if integration not in integration_to_missing_uris:
                    integration_to_missing_uris[integration] = []
                integration_to_missing_uris[integration].append(uri)
    return uri_to_labels_map
