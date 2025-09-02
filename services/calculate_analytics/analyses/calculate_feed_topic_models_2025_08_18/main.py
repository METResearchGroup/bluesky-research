"""Calculate feed-level topic modeling analysis.

This analysis performs BERTopic topic modeling on Bluesky feed content to understand
topic distributions across experimental conditions and time periods.
"""

import os
import json
import hashlib

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    map_users_to_posts_used_in_feeds,
)
from services.backfill.posts_used_in_feeds.load_data import (
    load_preprocessed_posts_used_in_feeds_for_partition_date,
)
from services.calculate_analytics.shared.data_loading.users import (
    get_user_condition_mapping,
    load_user_data,
)
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.topic_analysis_helpers import (
    compute_doc_topic_assignments,
    aggregate_topic_distributions_by_slice,
    export_stratified_analysis_results,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()

# Output file paths
topics_export_fp = os.path.join(
    current_dir,
    "results",
    f"topic_modeling_results_{current_datetime_str}.csv",
)
quality_metrics_export_fp = os.path.join(
    current_dir,
    "results",
    f"topic_quality_metrics_{current_datetime_str}.json",
)
stratified_analysis_export_fp = os.path.join(
    current_dir,
    "results",
    f"stratified_topic_analysis_{current_datetime_str}.csv",
)
summary_export_fp = os.path.join(
    current_dir,
    "results",
    f"topic_analysis_summary_{current_datetime_str}.json",
)

logger = get_logger(__name__)


def canonicalize_text(text: str) -> str:
    """Canonicalize text for stable hashing and deduplication."""
    if text is None:
        return ""
    return " ".join(str(text).split()).lower()


def compute_doc_id(text: str) -> str:
    """Compute stable doc_id from text."""
    canon = canonicalize_text(text)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def load_topic_modeling_data():
    """Load data for topic modeling analysis.

    Returns:
        tuple: (documents_df, date_condition_uris_map, uri_doc_map)
            - documents_df: DataFrame with columns [doc_id, text] (deduplicated documents)
            - date_condition_uris_map: dict mapping date->condition->set of post URIs
            - uri_doc_map: DataFrame with columns [uri, partition_date, doc_id]
    """
    logger.info("Loading topic modeling data...")

    # Load users and partition dates
    try:
        user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to load user data and/or partition dates: {e}")
        raise

    # Load user condition mapping
    try:
        user_condition_mapping: dict[str, str] = get_user_condition_mapping()
    except Exception as e:
        logger.error(f"Failed to load user condition mapping: {e}")
        raise

    # Collect unique documents and the uri->doc mapping
    documents_map: dict[str, str] = {}
    uri_doc_rows: list[dict[str, str]] = []
    date_condition_uris_map: dict[str, dict[str, set[str]]] = {}

    for partition_date in partition_dates:
        logger.info(f"Processing partition date: {partition_date}")

        # Get users to posts used in feeds for this date
        try:
            users_to_posts_used_in_feeds: dict[str, set[str]] = (
                map_users_to_posts_used_in_feeds(partition_date)
            )
        except Exception as e:
            logger.error(f"Failed to get posts used in feeds for {partition_date}: {e}")
            raise

        # Accumulate post URIs by condition
        condition_to_post_uris: dict[str, set[str]] = {}
        invalid_users = set()

        for user, post_uris in users_to_posts_used_in_feeds.items():
            try:
                condition = user_condition_mapping[user]
                if condition not in condition_to_post_uris:
                    condition_to_post_uris[condition] = set()
                condition_to_post_uris[condition].update(post_uris)
            except Exception:
                invalid_users.add(user)

        # Load post texts for this partition date
        all_post_uris = set()
        for condition, post_uris in condition_to_post_uris.items():
            all_post_uris.update(post_uris)

        if all_post_uris:
            try:
                # Load preprocessed posts used in feeds for this partition date
                all_post_texts_df: pd.DataFrame = (
                    load_preprocessed_posts_used_in_feeds_for_partition_date(
                        partition_date=partition_date
                    )
                )

                # Filter to only the URIs we're interested in
                all_post_texts_df = all_post_texts_df[
                    all_post_texts_df["uri"].isin(all_post_uris)
                ]

            except Exception as e:
                logger.error(f"Failed to load post texts for {partition_date}: {e}")
                raise

            # Build documents and uri->doc_id mapping for this partition_date
            total_docs_before = len(documents_map)
            for row in all_post_texts_df.itertuples(index=False):
                uri = row.uri
                text = row.text
                doc_id = compute_doc_id(text)
                # Preserve first seen raw text for the doc_id
                if doc_id not in documents_map:
                    documents_map[doc_id] = text
                uri_doc_rows.append(
                    {"uri": uri, "partition_date": partition_date, "doc_id": doc_id}
                )
            total_docs_after = len(documents_map)
            logger.info(
                f"[{partition_date}] Unique documents before: {total_docs_before}\tafter: {total_docs_after}"
            )

        # Store condition to post URIs mapping for this date
        date_condition_uris_map[partition_date] = condition_to_post_uris

        logger.info(
            f"[{partition_date}] Total URIs by condition: {len(condition_to_post_uris)}\tTotal invalid users: {len(invalid_users)}"
        )

    # Finalize DataFrames
    documents_df = pd.DataFrame(
        [(doc_id, text) for doc_id, text in documents_map.items()],
        columns=["doc_id", "text"],
    )
    uri_doc_map = pd.DataFrame(
        uri_doc_rows, columns=["uri", "partition_date", "doc_id"]
    )

    logger.info(f"Loaded {len(documents_df)} unique documents for topic modeling")
    logger.info(f"Loaded {len(uri_doc_map)} URI-document mappings")
    logger.info(
        f"Loaded {len(date_condition_uris_map)} partition dates with condition mappings"
    )

    return documents_df, date_condition_uris_map, uri_doc_map


def train_bertopic_model(documents_df: pd.DataFrame) -> BERTopicWrapper:
    """Train BERTopic model on the documents.

    Args:
        documents_df: DataFrame with columns [doc_id, text]

    Returns:
        Trained BERTopicWrapper instance
    """
    logger.info("Training BERTopic model...")

    # Use conservative configuration for large datasets
    if len(documents_df) > 100000:
        logger.warning(
            f"Large dataset detected ({len(documents_df)} documents), using conservative configuration"
        )
        config = {
            "embedding_model": {
                "name": "all-MiniLM-L6-v2",
                "device": "auto",
                "batch_size": 16,
            },
            "bertopic": {
                "top_n_words": 15,
                "min_topic_size": 200,
                "nr_topics": 50,
                "calculate_probabilities": False,
                "verbose": True,
                "hdbscan_model": {
                    "min_cluster_size": 200,
                    "min_samples": 100,
                    "cluster_selection_method": "leaf",
                    "prediction_data": True,
                    "gen_min_span_tree": False,
                    "core_dist_n_jobs": 1,
                },
                "umap_model": {
                    "n_neighbors": 10,
                    "n_components": 3,
                    "min_dist": 0.1,
                    "metric": "cosine",
                    "random_state": 42,
                    "low_memory": True,
                },
            },
            "random_seed": 42,
        }
    else:
        config = {
            "embedding_model": {
                "name": "all-MiniLM-L6-v2",
                "device": "auto",
                "batch_size": 32,
            },
            "bertopic": {
                "top_n_words": 20,
                "min_topic_size": 100,
                "nr_topics": 50,
                "calculate_probabilities": True,
                "verbose": True,
                "hdbscan_model": {
                    "min_cluster_size": 100,
                    "min_samples": 50,
                    "cluster_selection_method": "leaf",
                    "prediction_data": True,
                    "gen_min_span_tree": True,
                },
                "umap_model": {
                    "n_neighbors": 10,
                    "n_components": 3,
                    "min_dist": 0.1,
                    "metric": "cosine",
                    "random_state": 42,
                },
            },
            "random_seed": 42,
        }

    try:
        logger.info(f"Initializing BERTopicWrapper with config: {config}")
        bertopic = BERTopicWrapper(config_dict=config)

        logger.info(
            f"Dataset info: {len(documents_df)} documents, columns: {list(documents_df.columns)}"
        )
        logger.info(
            f"Text sample (first 100 chars): {documents_df['text'].iloc[0][:100]}..."
        )
        logger.info(
            f"Text length stats: min={documents_df['text'].str.len().min()}, max={documents_df['text'].str.len().max()}, mean={documents_df['text'].str.len().mean():.1f}"
        )

        logger.info("Starting BERTopic training...")
        bertopic.fit(documents_df, "text")
        logger.info("BERTopic training completed successfully!")

    except Exception as e:
        logger.error(f"BERTopic training failed: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Full error details: {str(e)}")
        raise RuntimeError(f"BERTopic training failed: {e}")

    return bertopic


def do_setup():
    """Setup steps for topic modeling analysis."""
    try:
        documents_df, date_condition_uris_map, uri_doc_map = load_topic_modeling_data()
    except Exception as e:
        logger.error(f"Failed to load topic modeling data: {e}")
        raise

    return {
        "documents_df": documents_df,
        "date_condition_uris_map": date_condition_uris_map,
        "uri_doc_map": uri_doc_map,
    }


def do_analysis_and_export_results(
    documents_df: pd.DataFrame,
    date_condition_uris_map: dict[str, dict[str, set[str]]],
    uri_doc_map: pd.DataFrame,
):
    """Perform topic modeling analysis and export results."""

    # 1. Train BERTopic model
    logger.info("[Topic Modeling] Training BERTopic model...")
    try:
        bertopic = train_bertopic_model(documents_df)
    except Exception as e:
        logger.error(f"Failed to train BERTopic model: {e}")
        raise

    # 2. Compute topic assignments
    logger.info("[Topic Modeling] Computing topic assignments...")
    try:
        doc_topic_assignments = compute_doc_topic_assignments(bertopic, documents_df)
    except Exception as e:
        logger.error(f"Failed to compute topic assignments: {e}")
        raise

    # 3. Perform stratified analysis
    logger.info("[Topic Modeling] Performing stratified analysis...")
    try:
        topic_distributions = aggregate_topic_distributions_by_slice(
            date_condition_uris_map, uri_doc_map, doc_topic_assignments
        )
    except Exception as e:
        logger.error(f"Failed to perform stratified analysis: {e}")
        raise

    # 4. Export topic information
    logger.info("[Topic Modeling] Exporting topic information...")
    try:
        os.makedirs(os.path.dirname(topics_export_fp), exist_ok=True)

        topics = bertopic.get_topics()
        topic_info = bertopic.get_topic_info()
        quality_metrics = bertopic.get_quality_metrics()

        # Export topic information
        topic_info.to_csv(topics_export_fp, index=False)
        logger.info(f"Exported topic information to {topics_export_fp}")

        # Export quality metrics
        with open(quality_metrics_export_fp, "w") as f:
            json.dump(quality_metrics, f, indent=2, default=str)
        logger.info(f"Exported quality metrics to {quality_metrics_export_fp}")

    except Exception as e:
        logger.error(f"Failed to export topic information: {e}")
        raise

    # 5. Export stratified analysis results
    logger.info("[Topic Modeling] Exporting stratified analysis results...")
    try:
        export_stratified_analysis_results(
            topic_distributions,
            os.path.dirname(stratified_analysis_export_fp),
            current_datetime_str,
        )
        logger.info(
            f"Exported stratified analysis results to {stratified_analysis_export_fp}"
        )
    except Exception as e:
        logger.error(f"Failed to export stratified analysis results: {e}")
        raise

    # 6. Export summary
    logger.info("[Topic Modeling] Exporting analysis summary...")
    try:
        summary = {
            "start_date": STUDY_START_DATE,
            "end_date": STUDY_END_DATE,
            "total_documents": len(documents_df),
            "total_topics": len([t for t in topics.keys() if t != -1]),
            "training_time_seconds": quality_metrics.get("training_time", 0),
            "c_v_coherence": quality_metrics.get("c_v_mean", 0),
            "c_npmi_coherence": quality_metrics.get("c_npmi_mean", 0),
            "export_timestamp": current_datetime_str,
        }

        with open(summary_export_fp, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Exported analysis summary to {summary_export_fp}")

    except Exception as e:
        logger.error(f"Failed to export analysis summary: {e}")
        raise

    # 7. Display results
    logger.info("[Topic Modeling] Displaying results...")
    try:
        print("\n" + "=" * 80)
        print("🎯 TOPIC MODELING RESULTS")
        print("=" * 80)
        print(f"📊 Total Documents: {len(documents_df)}")
        print(f"🏷️  Total Topics: {len([t for t in topics.keys() if t != -1])}")
        print(
            f"⏱️  Training Time: {quality_metrics.get('training_time', 0):.2f} seconds"
        )

        if "c_v_mean" in quality_metrics:
            print(f"📈 c_v Coherence: {quality_metrics['c_v_mean']:.3f}")
        if "c_npmi_mean" in quality_metrics:
            print(f"📈 c_npmi Coherence: {quality_metrics['c_npmi_mean']:.3f}")

        print("\n🔍 Discovered Topics:")
        for topic_id, words in topics.items():
            if topic_id != -1:  # Skip outlier topic
                top_words = [word for word, _ in words[:8]]
                print(f"  Topic {topic_id}: {', '.join(top_words)}")

        print("=" * 80)
        logger.info("Results displayed successfully")

    except Exception as e:
        logger.error(f"Failed to display results: {e}")
        raise


def main():
    """Execute the topic modeling analysis."""
    try:
        setup_objs = do_setup()

        documents_df: pd.DataFrame = setup_objs["documents_df"]
        date_condition_uris_map: dict[str, dict[str, set[str]]] = setup_objs[
            "date_condition_uris_map"
        ]
        uri_doc_map: pd.DataFrame = setup_objs["uri_doc_map"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        do_analysis_and_export_results(
            documents_df=documents_df,
            date_condition_uris_map=date_condition_uris_map,
            uri_doc_map=uri_doc_map,
        )
    except Exception as e:
        logger.error(f"Failed to do analysis and export results: {e}")
        raise

    logger.info("Topic modeling analysis completed successfully!")


if __name__ == "__main__":
    main()
