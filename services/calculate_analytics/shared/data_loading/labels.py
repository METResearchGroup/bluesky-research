"""Shared label data loading functionality for analytics system.

This module provides unified interfaces for loading various types of ML labels
used across the analytics system, eliminating code duplication and
ensuring consistent data handling patterns.
"""

from typing import Literal, Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import integrations_list

logger = get_logger(__file__)

dataloader_batch_size: int = 10_000


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
            "prob_severe_toxic": labels_dict["prob_severe_toxic"],
            "prob_identity_attack": labels_dict["prob_identity_attack"],
            "prob_insult": labels_dict["prob_insult"],
            "prob_profanity": labels_dict["prob_profanity"],
            "prob_threat": labels_dict["prob_threat"],
            "prob_affinity": labels_dict["prob_affinity"],
            "prob_compassion": labels_dict["prob_compassion"],
            "prob_curiosity": labels_dict["prob_curiosity"],
            "prob_nuance": labels_dict["prob_nuance"],
            "prob_personal_story": labels_dict["prob_personal_story"],
            "prob_reasoning": labels_dict["prob_reasoning"],
            "prob_respect": labels_dict["prob_respect"],
            "prob_alienation": labels_dict["prob_alienation"],
            "prob_fearmongering": labels_dict["prob_fearmongering"],
            "prob_generalization": labels_dict["prob_generalization"],
            "prob_moral_outrage": labels_dict["prob_moral_outrage"],
            "prob_scapegoating": labels_dict["prob_scapegoating"],
            "prob_sexually_explicit": labels_dict["prob_sexually_explicit"],
            "prob_flirtation": labels_dict["prob_flirtation"],
            "prob_spam": labels_dict["prob_spam"],
        }
    elif integration == "sociopolitical":
        return {
            "is_sociopolitical": labels_dict["is_sociopolitical"],
            "is_not_sociopolitical": (labels_dict["is_sociopolitical"] is False),
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
    # Initialize the result structure for ALL URIs
    uri_to_labels_map: dict[str, dict[str, float]] = {uri: {} for uri in post_uris}

    # Track which integrations we've processed for each URI
    uri_integration_status: dict[str, set[str]] = {
        uri: set(integrations_list) for uri in post_uris
    }

    # iterate through each integration and add the labels for each URI.
    for integration in integrations_list:
        logger.info(f"[Get all labels for posts] Processing {integration}...")
        for partition_date in partition_dates:
            # load the full labels dataset for the integration + date
            labels_df: pd.DataFrame = get_labels_for_partition_date(
                integration=integration, partition_date=partition_date
            )

            # filter to only the URIs that we care about.
            relevant_labels_df: pd.DataFrame = labels_df[
                labels_df["uri"].isin(post_uris)
            ]

            del labels_df

            if len(relevant_labels_df) == 0:
                logger.info(
                    f"[Get all labels for posts] No labels for {integration} on {partition_date}..."
                )
                continue

            total_rows = len(relevant_labels_df)
            chunk_size = dataloader_batch_size

            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)

                # get chunk of df as a view.
                chunk_df: pd.DataFrame = relevant_labels_df.iloc[start_idx:end_idx]

                # process the chunk.
                # NOTE: itertuples is much faster than iterrows.
                for row in chunk_df.itertuples():
                    try:
                        # Use attribute access for itertuples (row.uri, not row["uri"])
                        post_uri: str = row.uri

                        # Extract only the fields relevant to this specific integration
                        labels_dict = _extract_integration_fields(row, integration)

                        transformed_labels_dict: dict = transform_labels_dict(
                            integration=integration, labels_dict=labels_dict
                        )

                        # Update the labels for this URI
                        uri_to_labels_map[post_uri].update(transformed_labels_dict)

                        # Mark this integration as processed for this URI
                        uri_integration_status[post_uri].discard(integration)
                    except Exception as e:
                        # post_uri is now always defined before this point
                        logger.error(
                            f"[Get all labels for posts] Error processing {integration} for {post_uri}: {e}"
                        )
                        continue
                # delete chunk data.
                del chunk_df

            del relevant_labels_df

    # Completeness check and logging
    _log_completeness_status(uri_integration_status, post_uris)

    return uri_to_labels_map


def _extract_integration_fields(row, integration: str) -> dict:
    """Extract only the fields relevant to a specific integration from a DataFrame row.

    This is more efficient than extracting all possible fields since we process
    one integration at a time and only need specific fields for each.
    """
    if integration == "perspective_api":
        return {
            "uri": row.uri,
            "prob_toxic": row.prob_toxic,
            "prob_constructive": row.prob_constructive,
            "prob_severe_toxic": row.prob_severe_toxic,
            "prob_identity_attack": row.prob_identity_attack,
            "prob_insult": row.prob_insult,
            "prob_profanity": row.prob_profanity,
            "prob_threat": row.prob_threat,
            "prob_affinity": row.prob_affinity,
            "prob_compassion": row.prob_compassion,
            "prob_curiosity": row.prob_curiosity,
            "prob_nuance": row.prob_nuance,
            "prob_personal_story": row.prob_personal_story,
            "prob_reasoning": row.prob_reasoning,
            "prob_respect": row.prob_respect,
            "prob_alienation": row.prob_alienation,
            "prob_fearmongering": row.prob_fearmongering,
            "prob_generalization": row.prob_generalization,
            "prob_moral_outrage": row.prob_moral_outrage,
            "prob_scapegoating": row.prob_scapegoating,
            "prob_sexually_explicit": row.prob_sexually_explicit,
            "prob_flirtation": row.prob_flirtation,
            "prob_spam": row.prob_spam,
        }
    elif integration == "sociopolitical":
        return {
            "uri": row.uri,
            "is_sociopolitical": row.is_sociopolitical,
            "political_ideology_label": row.political_ideology_label,
        }
    elif integration == "ime":
        return {
            "uri": row.uri,
            "prob_intergroup": row.prob_intergroup,
            "prob_moral": row.prob_moral,
            "prob_emotion": row.prob_emotion,
            "prob_other": row.prob_other,
        }
    elif integration == "valence_classifier":
        return {
            "uri": row.uri,
            "compound": row.compound,
            "valence_label": row.valence_label,
        }
    else:
        raise ValueError(f"Invalid integration for field extraction: {integration}")


def _log_completeness_status(
    uri_integration_status: dict[str, set[str]], post_uris: set[str]
) -> None:
    """Log the completeness status of label processing."""
    total_uris = len(post_uris)
    complete_uris = sum(
        1 for status in uri_integration_status.values() if len(status) == 0
    )
    incomplete_uris = total_uris - complete_uris

    logger.info(
        f"[Get all labels for posts] Completeness: {complete_uris}/{total_uris} URIs have all integrations"
    )

    if incomplete_uris > 0:
        logger.warning(
            f"[Get all labels for posts] {incomplete_uris} URIs missing some integrations:"
        )

        # Group by missing integrations for better debugging
        missing_integrations: dict[str, list[str]] = {}
        for uri, missing in uri_integration_status.items():
            if missing:  # If there are missing integrations
                for integration in missing:
                    if integration not in missing_integrations:
                        missing_integrations[integration] = []
                    missing_integrations[integration].append(uri)

        # Log missing integrations by type
        for integration, uris in missing_integrations.items():
            logger.warning(
                f"[Get all labels for posts] {integration}: {len(uris)} URIs missing"
            )
            if len(uris) <= 5:  # Log individual URIs if not too many
                logger.warning(
                    f"[Get all labels for posts] Missing {integration} for: {uris}"
                )
            else:
                logger.warning(
                    f"[Get all labels for posts] Missing {integration} for: {uris[:5]}... and {len(uris)-5} more"
                )
