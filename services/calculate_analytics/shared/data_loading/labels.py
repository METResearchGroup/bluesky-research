"""Shared label data loading functionality for analytics system.

This module provides unified interfaces for loading various types of ML labels
used across the analytics system, eliminating code duplication and
ensuring consistent data handling patterns.
"""

import gc
from typing import Literal, Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import integrations_list

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


def transform_labels_dict(integration: str, labels_dict: dict):
    """Transform the labels into a format so that we get the relevant
    measures. Integration-specific."""
    if integration == "perspective_api":
        return {
            "prob_toxic": labels_dict["prob_toxic"],
            "prob_constructive": labels_dict["prob_constructive"],
        }
    elif integration == "sociopolitical":
        return {
            "is_sociopolitical": labels_dict["is_sociopolitical"],
            "is_not_sociopolitical": not labels_dict["is_sociopolitical"],
            # TODO: need to double-check the formatting of these labels.
            "is_political_left": (labels_dict["political_ideology_label"] == "left"),
            "is_political_right": (labels_dict["political_ideology_label"] == "right"),
            "is_political_moderate": (
                labels_dict["political_ideology_label"] == "moderate"
            ),
            "is_political_unclear": (
                labels_dict["political_ideology_label"] == "unclear"
            ),
        }
    elif integration == "ime":
        return {
            "prob_intergroup": labels_dict["prob_intergroup"],
            "prob_moral": labels_dict["prob_moral"],
            "prob_emotion": labels_dict["prob_emotion"],
            "prob_other": labels_dict["prob_other"],
        }
    elif integration == "valence_classifier":
        return {
            "valence_clf_score": labels_dict[
                "compound"
            ],  # raw score given by valence classifier
            "is_valence_positive": labels_dict["valence_label"] == "positive",
            "is_valence_negative": labels_dict["valence_label"] == "negative",
            "is_valence_neutral": labels_dict["valence_label"] == "neutral",
        }
    else:
        raise ValueError(f"Invalid integration for labeling: {integration}")


# TODO: check if I should return as a pd.DataFrame or as a dict?
# dict works pretty well for now and I worry about pandas being
# too expensive. Conversion to pandas is easy, just take the dict
# and convert to df.
# TODO: should this be at the day-level? This should work fine and we
# can test on a smaller subset of data. I'd rather not have to do this
# on chunks of dates as that complicates orchestration.
def get_all_labels_for_posts(post_uris: set[str], partition_dates: list[str]) -> dict:
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

    # set up hash maps.
    for uri in post_uris:
        uri_to_labels_map[uri] = {}
        # pop each of these from the set as they're hydrated.
        uris_to_pending_integrations[uri] = {
            "perspective_api",
            "sociopolitical",
            "ime",
            "valence_classifier",
        }

    # iterate through each integration and add the labels for each URI.
    for integration in integrations_list:
        # load day-by-day labels for each integration. Once the labels are
        # loaded, filter for relevant URIs.
        filtered_uris = set()
        for partition_date in partition_dates:
            labels_df: pd.DataFrame = get_labels_for_partition_date(
                integration=integration, partition_date=partition_date
            )
            labels_df: pd.DataFrame = labels_df[labels_df["uri"].isin(post_uris)]
            labels_dicts: list[dict] = labels_df.to_dict(orient="records")
            for labels_dict in labels_dicts:
                # get the relevant transformed labels.
                transformed_labels_dict: dict = transform_labels_dict(
                    integration=integration, labels_dict=labels_dict
                )
                post_uri: str = labels_dict["uri"]
                filtered_uris.add(post_uri)
                uri_to_labels_map[post_uri] = {
                    **uri_to_labels_map[post_uri],
                    **transformed_labels_dict,
                }
            del labels_df
            gc.collect()

        # after going through all the labels for the integration, we go through
        # the URIs that we got labels for, and we update our tracker so we know
        # which integrations we got labels for, for a given URI.
        for uri in filtered_uris:
            uris_to_pending_integrations[uri].remove(integration)
            if len(uris_to_pending_integrations[uri]) == 0:
                uris_to_pending_integrations.pop(uri)

    # double-check and log in case we're missing labels for some URIs.
    if len(uris_to_pending_integrations) > 0:
        print(
            f"We have {len(uris_to_pending_integrations)}/{len(post_uris)} still missing some integration of some sort."
        )
        integration_to_missing_uris = {}
        for uri, integrations in uris_to_pending_integrations.items():
            for integration in integrations:
                if integration not in integration_to_missing_uris:
                    integration_to_missing_uris[integration] = []
                integration_to_missing_uris[integration].append(uri)

    return uri_to_labels_map
