"""
Topic analysis helpers for MET-46 stratified feed-level analysis.

This module provides utilities to:
1. Compute doc_id -> topic_id assignments from BERTopic results
2. Aggregate topic distributions across condition/time slices
3. Support the stratified analysis requirements for feed-level topic modeling

Author: AI Agent implementing MET-46 helpers
Date: 2025-01-20
"""

import hashlib
from typing import Dict
import pandas as pd
from collections import defaultdict

from lib.log.logger import get_logger
from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper

logger = get_logger(__name__)


def sanitize_slice_name(slice_name: str) -> str:
    """
    Sanitize slice name for safe use in file paths.

    Args:
        slice_name: Original slice name

    Returns:
        Sanitized slice name safe for file paths
    """
    # Replace problematic characters with underscores
    sanitized = str(slice_name).replace("/", "_").replace("\\", "_").replace(":", "_")
    # Remove any other potentially problematic characters
    sanitized = "".join(c if c.isalnum() or c in "_-" else "_" for c in sanitized)
    return sanitized


def canonicalize_text(text: str) -> str:
    """
    Canonicalize text for stable hashing and deduplication.

    Must match the canonicalization used in load_data.py to ensure
    consistent doc_id generation.

    Args:
        text: Raw text to canonicalize

    Returns:
        Canonicalized text string
    """
    if text is None:
        return ""
    # Normalize whitespace and lowercase; avoid heavy transforms to preserve semantics
    return " ".join(str(text).split()).lower()


def compute_doc_id(text: str) -> str:
    """
    Compute stable doc_id from text.

    Must match the doc_id computation used in load_data.py to ensure
    consistent mapping.

    Args:
        text: Raw text to hash

    Returns:
        SHA-256 hash of canonicalized text
    """
    canon = canonicalize_text(text)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def compute_doc_topic_assignments(
    bertopic: BERTopicWrapper, documents_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute doc_id -> topic_id assignments from trained BERTopic model.

    Args:
        bertopic: Trained BERTopicWrapper instance
        documents_df: DataFrame with columns [doc_id, text] (deduplicated documents)

    Returns:
        DataFrame with columns [doc_id, topic_id] mapping each document to its assigned topic
    """
    logger.info("🎯 Computing doc_id -> topic_id assignments...")

    # Validate inputs
    required_cols = {"doc_id", "text"}
    if not required_cols.issubset(documents_df.columns):
        raise ValueError(
            f"documents_df must have columns {required_cols}, got {list(documents_df.columns)}"
        )

    # Get topic assignments from BERTopic
    topics = bertopic.get_document_topics()

    # Get the processed documents that were actually used for training
    processed_documents_df = bertopic.get_processed_documents_df()
    if processed_documents_df is None:
        raise ValueError("BERTopic model has not been trained yet")

    # Use processed documents for topic assignment (these match the topic assignments)
    if len(topics) != len(processed_documents_df):
        raise ValueError(
            f"Topic assignments length ({len(topics)}) doesn't match processed documents length ({len(processed_documents_df)})"
        )

    # Create doc_id -> topic_id mapping using processed documents
    doc_topic_assignments = []
    for i, (doc_id, _) in enumerate(
        processed_documents_df[["doc_id", "text"]].itertuples(index=False)
    ):
        topic_id = topics[i]
        doc_topic_assignments.append({"doc_id": doc_id, "topic_id": topic_id})

    result_df = pd.DataFrame(doc_topic_assignments)

    logger.info(f"✅ Generated {len(result_df)} doc_id -> topic_id assignments")
    logger.info(
        f"📊 Topic distribution: {result_df['topic_id'].value_counts().sort_index().to_dict()}"
    )

    return result_df


def aggregate_topic_distributions_by_slice(
    date_condition_uris_map: Dict[str, Dict[str, set[str]]],
    uri_doc_map: pd.DataFrame,
    doc_topic_assignments: pd.DataFrame,
    include_temporal_slices: bool = True,
    election_date: str = "2024-11-05",
) -> Dict[str, pd.DataFrame]:
    """
    Aggregate topic distributions across condition/time slices for MET-46 analysis.

    Computes stratified topic distributions across:
    - Overall corpus baseline
    - Three experimental conditions
    - Temporal periods (weekly + pre/post election)
    - Condition × Time interactions

    Args:
        date_condition_uris_map: Mapping from date -> condition -> set of post URIs
        uri_doc_map: DataFrame with columns [uri, partition_date, doc_id]
        doc_topic_assignments: DataFrame with columns [doc_id, topic_id]
        include_temporal_slices: Whether to compute temporal and interaction slices
        election_date: Election date for pre/post analysis (YYYY-MM-DD format)

    Returns:
        Dictionary mapping slice_name -> DataFrame with topic distribution statistics
        Each DataFrame has columns: [topic_id, count, proportion, slice_type, slice_value]
    """
    logger.info("📊 Aggregating topic distributions by slice...")

    # Validate inputs
    uri_required_cols = {"uri", "partition_date", "doc_id"}
    if not uri_required_cols.issubset(uri_doc_map.columns):
        raise ValueError(f"uri_doc_map must have columns {uri_required_cols}")

    doc_required_cols = {"doc_id", "topic_id"}
    if not doc_required_cols.issubset(doc_topic_assignments.columns):
        raise ValueError(f"doc_topic_assignments must have columns {doc_required_cols}")

    # Join uri_doc_map with topic assignments to get uri -> topic_id mapping
    uri_topic_df = uri_doc_map.merge(doc_topic_assignments, on="doc_id", how="left")

    # Handle any missing topic assignments (shouldn't happen with proper dedup)
    missing_topics = uri_topic_df["topic_id"].isnull().sum()
    if missing_topics > 0:
        logger.warning(f"⚠️ Found {missing_topics} URIs without topic assignments")
        uri_topic_df = uri_topic_df.dropna(subset=["topic_id"])

    results = {}

    # 1. Overall corpus baseline
    logger.info("📈 Computing overall corpus baseline...")
    overall_topics = uri_topic_df["topic_id"].value_counts().sort_index()
    total_docs = len(uri_topic_df)
    overall_df = pd.DataFrame(
        {
            "topic_id": overall_topics.index,
            "count": overall_topics.values,
            "proportion": overall_topics.values / total_docs,
            "slice_type": "overall",
            "slice_value": "corpus",
        }
    )
    results["overall_corpus"] = overall_df

    # 2. By experimental condition
    logger.info("🧪 Computing topic distributions by condition...")
    condition_uri_sets = defaultdict(set)
    for date, conditions in date_condition_uris_map.items():
        for condition, uris in conditions.items():
            condition_uri_sets[condition].update(uris)

    for condition, uris in condition_uri_sets.items():
        condition_topics = (
            uri_topic_df[uri_topic_df["uri"].isin(uris)]["topic_id"]
            .value_counts()
            .sort_index()
        )
        condition_total = condition_topics.sum()
        if condition_total > 0:
            condition_df = pd.DataFrame(
                {
                    "topic_id": condition_topics.index,
                    "count": condition_topics.values,
                    "proportion": condition_topics.values / condition_total,
                    "slice_type": "condition",
                    "slice_value": condition,
                }
            )
            results[f"condition_{condition}"] = condition_df

    if not include_temporal_slices:
        logger.info(f"✅ Computed {len(results)} slices (conditions only)")
        return results

    # 3. By temporal periods (weekly)
    logger.info("📅 Computing topic distributions by weekly periods...")
    uri_topic_df["partition_date"] = pd.to_datetime(uri_topic_df["partition_date"])
    uri_topic_df["week"] = uri_topic_df["partition_date"].dt.to_period("W")

    for week_period in uri_topic_df["week"].unique():
        week_topics = (
            uri_topic_df[uri_topic_df["week"] == week_period]["topic_id"]
            .value_counts()
            .sort_index()
        )
        week_total = week_topics.sum()
        if week_total > 0:
            week_df = pd.DataFrame(
                {
                    "topic_id": week_topics.index,
                    "count": week_topics.values,
                    "proportion": week_topics.values / week_total,
                    "slice_type": "weekly",
                    "slice_value": str(week_period),
                }
            )
            # Convert period to string and sanitize for file paths
            week_str = sanitize_slice_name(week_period)
            results[f"week_{week_str}"] = week_df

    # 4. Pre/post election analysis
    logger.info(
        f"🗳️ Computing pre/post election analysis (election_date: {election_date})..."
    )
    election_dt = pd.to_datetime(election_date)
    uri_topic_df["election_period"] = uri_topic_df["partition_date"].apply(
        lambda x: "pre_election" if x < election_dt else "post_election"
    )

    for period in ["pre_election", "post_election"]:
        period_topics = (
            uri_topic_df[uri_topic_df["election_period"] == period]["topic_id"]
            .value_counts()
            .sort_index()
        )
        period_total = period_topics.sum()
        if period_total > 0:
            period_df = pd.DataFrame(
                {
                    "topic_id": period_topics.index,
                    "count": period_topics.values,
                    "proportion": period_topics.values / period_total,
                    "slice_type": "election_period",
                    "slice_value": period,
                }
            )
            results[f"election_{period}"] = period_df

    # 5. Condition × Time interactions
    logger.info("🔄 Computing condition × time interaction effects...")

    # Condition × Weekly interactions
    for condition, uris in condition_uri_sets.items():
        condition_uri_topic_df = uri_topic_df[uri_topic_df["uri"].isin(uris)]
        for week_period in condition_uri_topic_df["week"].unique():
            interaction_topics = (
                condition_uri_topic_df[condition_uri_topic_df["week"] == week_period][
                    "topic_id"
                ]
                .value_counts()
                .sort_index()
            )
            interaction_total = interaction_topics.sum()
            if interaction_total > 0:
                interaction_df = pd.DataFrame(
                    {
                        "topic_id": interaction_topics.index,
                        "count": interaction_topics.values,
                        "proportion": interaction_topics.values / interaction_total,
                        "slice_type": "condition_week",
                        "slice_value": f"{condition}_{week_period}",
                    }
                )
                # Sanitize week period for file paths
                week_str = sanitize_slice_name(week_period)
                results[f"condition_{condition}_week_{week_str}"] = interaction_df

    # Condition × Election period interactions
    for condition, uris in condition_uri_sets.items():
        condition_uri_topic_df = uri_topic_df[uri_topic_df["uri"].isin(uris)]
        for period in ["pre_election", "post_election"]:
            interaction_topics = (
                condition_uri_topic_df[
                    condition_uri_topic_df["election_period"] == period
                ]["topic_id"]
                .value_counts()
                .sort_index()
            )
            interaction_total = interaction_topics.sum()
            if interaction_total > 0:
                interaction_df = pd.DataFrame(
                    {
                        "topic_id": interaction_topics.index,
                        "count": interaction_topics.values,
                        "proportion": interaction_topics.values / interaction_total,
                        "slice_type": "condition_election",
                        "slice_value": f"{condition}_{period}",
                    }
                )
                results[f"condition_{condition}_election_{period}"] = interaction_df

    logger.info(
        f"✅ Computed {len(results)} total slices across all stratification levels"
    )
    logger.info(
        f"📊 Slice types: {set(df['slice_type'].iloc[0] for df in results.values())}"
    )

    return results


def export_stratified_analysis_results(
    topic_distributions: Dict[str, pd.DataFrame], output_dir: str, timestamp: str
) -> None:
    """
    Export stratified topic distribution results to CSV files.

    Args:
        topic_distributions: Results from aggregate_topic_distributions_by_slice
        output_dir: Directory to save results
        timestamp: Timestamp string for unique filenames
    """
    import os
    from pathlib import Path

    logger.info("📁 Exporting stratified analysis results...")

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Export individual slice results
    for slice_name, slice_df in topic_distributions.items():
        # Sanitize slice name for use in file path
        safe_slice_name = sanitize_slice_name(slice_name)
        slice_file = os.path.join(
            output_path, f"topic_distribution_{safe_slice_name}_{timestamp}.csv"
        )
        slice_df.to_csv(slice_file, index=False)

    # Export combined results for easy analysis
    combined_df = pd.concat(
        [df.assign(slice_name=name) for name, df in topic_distributions.items()],
        ignore_index=True,
    )
    combined_file = os.path.join(
        output_path, f"topic_distributions_combined_{timestamp}.csv"
    )
    combined_df.to_csv(combined_file, index=False)

    # Export summary statistics
    summary_stats = []
    for slice_name, slice_df in topic_distributions.items():
        summary_stats.append(
            {
                "slice_name": slice_name,
                "slice_type": slice_df["slice_type"].iloc[0],
                "slice_value": slice_df["slice_value"].iloc[0],
                "total_documents": slice_df["count"].sum(),
                "num_topics": len(slice_df),
                "top_topic_id": slice_df.loc[
                    slice_df["proportion"].idxmax(), "topic_id"
                ],
                "top_topic_proportion": slice_df["proportion"].max(),
            }
        )

    summary_df = pd.DataFrame(summary_stats)
    summary_file = os.path.join(
        output_path, f"stratified_analysis_summary_{timestamp}.csv"
    )
    summary_df.to_csv(summary_file, index=False)

    logger.info(
        f"📊 Exported {len(topic_distributions)} slice results to {output_path}"
    )
    logger.info(f"   📋 Individual slices: topic_distribution_*_{timestamp}.csv")
    logger.info(f"   📋 Combined results: {os.path.basename(combined_file)}")
    logger.info(f"   📋 Summary stats: {os.path.basename(summary_file)}")


def get_topic_evolution_analysis(
    topic_distributions: Dict[str, pd.DataFrame], topic_info: pd.DataFrame
) -> pd.DataFrame:
    """
    Analyze how topics evolve over time periods.

    Args:
        topic_distributions: Results from aggregate_topic_distributions_by_slice
        topic_info: Topic information from BERTopic (with topic names/words)

    Returns:
        DataFrame with topic evolution metrics across time periods
    """
    logger.info("📈 Analyzing topic evolution over time...")

    # Extract temporal slices (weekly and election periods)
    temporal_slices = {
        name: df
        for name, df in topic_distributions.items()
        if df["slice_type"].iloc[0] in ["weekly", "election_period"]
    }

    if not temporal_slices:
        logger.warning("⚠️ No temporal slices found for evolution analysis")
        return pd.DataFrame()

    # Build topic evolution matrix
    evolution_data = []

    for slice_name, slice_df in temporal_slices.items():
        slice_type = slice_df["slice_type"].iloc[0]
        slice_value = slice_df["slice_value"].iloc[0]

        for _, row in slice_df.iterrows():
            evolution_data.append(
                {
                    "topic_id": row["topic_id"],
                    "slice_name": slice_name,
                    "slice_type": slice_type,
                    "slice_value": slice_value,
                    "count": row["count"],
                    "proportion": row["proportion"],
                }
            )

    evolution_df = pd.DataFrame(evolution_data)

    if evolution_df.empty:
        return evolution_df

    # Add topic information if available
    if not topic_info.empty and "Topic" in topic_info.columns:
        topic_names = (
            topic_info.set_index("Topic")["Name"].to_dict()
            if "Name" in topic_info.columns
            else {}
        )
        evolution_df["topic_name"] = evolution_df["topic_id"].map(topic_names)

    logger.info(
        f"✅ Generated topic evolution analysis with {len(evolution_df)} data points"
    )

    return evolution_df
