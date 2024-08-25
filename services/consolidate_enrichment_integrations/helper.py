"""Helper functions for the consolidate_enrichment_integrations service."""

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa
from services.generate_vector_embeddings.models import PostSimilarityScoreModel
from services.ml_inference.models import (
    PerspectiveApiLabelsModel,
    SociopoliticalLabelsModel,
)  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


logger = get_logger(__name__)
root_s3_key = "consolidated_enriched_post_records"

athena = Athena()
dynamodb = DynamoDB()
s3 = S3()

dynamodb_table_name = "enrichment_consolidation_sessions"


def get_latest_enrichment_consolidation_session() -> dict:
    """Get the latest enrichment consolidation session."""
    try:
        sessions: list[dict] = dynamodb.get_all_items_from_table(
            table_name=dynamodb_table_name
        )  # noqa
        if not sessions:
            logger.info("No enrichment consolidation sessions found.")
            return None
        sorted_sessions = sorted(
            sessions,
            key=lambda x: x.get("enrichment_consolidation_timestamp", {}).get("S", ""),  # noqa
            reverse=True,
        )  # noqa
        return sorted_sessions[0]
    except Exception as e:
        logger.error(f"Failed to get latest enrichment consolidation session: {e}")  # noqa
        raise


def insert_enrichment_consolidation_session(enrichment_consolidation_session: dict):  # noqa
    """Insert the enrichment consolidation session."""
    try:
        dynamodb.insert_item_into_table(
            item=enrichment_consolidation_session, table_name=dynamodb_table_name
        )
        logger.info(
            f"Successfully inserted enrichment consolidation session: {enrichment_consolidation_session}"
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to insert enrichment consolidation session: {e}")  # noqa
        raise


def load_latest_preprocessed_posts(
    timestamp: str,
) -> list[FilteredPreprocessedPostModel]:
    return athena.get_latest_preprocessed_posts(timestamp=timestamp)


def load_latest_perspective_api_labels(
    timestamp: str,
) -> list[PerspectiveApiLabelsModel]:
    source_tables = [
        "perspective_api_firehose_labels",
        "perspective_api_most_liked_labels",
    ]
    where_filter = f"synctimestamp > '{timestamp}'" if timestamp else "1=1"  # noqa
    query = " UNION ALL ".join(
        [f"SELECT * FROM {table} WHERE {where_filter}" for table in source_tables]  # noqa
    )

    df: pd.DataFrame = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    return [PerspectiveApiLabelsModel(**label) for label in df_dicts]


def load_latest_sociopolitical_labels(
    timestamp: str,
) -> list[SociopoliticalLabelsModel]:
    source_tables = [
        "llm_sociopolitical_firehose_labels",
        "llm_sociopolitical_most_liked_labels",
    ]
    where_filter = f"synctimestamp > '{timestamp}'" if timestamp else "1=1"  # noqa
    query = " UNION ALL ".join(
        [f"SELECT * FROM {table} WHERE {where_filter}" for table in source_tables]  # noqa
    )
    df: pd.DataFrame = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    return [SociopoliticalLabelsModel(**label) for label in df_dicts]


def load_latest_similarity_scores(timestamp: str) -> list[PostSimilarityScoreModel]:
    source_tables = ["post_cosine_similarity_scores"]
    where_filter = f"synctimestamp > '{timestamp}'" if timestamp else "1=1"  # noqa
    query = " UNION ALL ".join(
        [f"SELECT * FROM {table} WHERE {where_filter}" for table in source_tables]  # noqa
    )
    df: pd.DataFrame = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    return [PostSimilarityScoreModel(**score) for score in df_dicts]


def consolidate_enrichment_integrations(
    preprocessed_posts: list[FilteredPreprocessedPostModel],
    perspective_api_labels: list[PerspectiveApiLabelsModel],
    sociopolitical_labels: list[SociopoliticalLabelsModel],
    similarity_scores: list[PostSimilarityScoreModel],
) -> list[ConsolidatedEnrichedPostModel]:
    """Consolidates the perspective API labels, sociopolitical labels, and
    similarity scores into a single list of enriched posts."""
    logger.info("Consolidating enrichment integrations...")
    logger.info(f"Preprocessed posts: {len(preprocessed_posts)}")
    logger.info(f"Perspective API labels: {len(perspective_api_labels)}")
    logger.info(f"Sociopolitical labels: {len(sociopolitical_labels)}")
    logger.info(f"Similarity scores: {len(similarity_scores)}")

    # Create dictionaries for faster lookup
    preprocessed_dict = {post.uri: post for post in preprocessed_posts}
    perspective_dict = {label.uri: label for label in perspective_api_labels}
    sociopolitical_dict = {label.uri: label for label in sociopolitical_labels}
    similarity_dict = {score.uri: score for score in similarity_scores}

    # Get all unique URIs
    all_uris = (
        set(preprocessed_dict.keys())
        & set(perspective_dict.keys())
        & set(sociopolitical_dict.keys())
        & set(similarity_dict.keys())
    )

    logger.info(
        f"Number of shared URIs to combine enrichment sources for: {len(all_uris)}"
    )  # noqa

    consolidated_posts: list[ConsolidatedEnrichedPostModel] = []

    for uri in all_uris:
        preprocessed = preprocessed_dict[uri]
        perspective = perspective_dict[uri]
        sociopolitical = sociopolitical_dict[uri]
        similarity = similarity_dict[uri]

        consolidated_post = ConsolidatedEnrichedPostModel(
            # Fields from FilteredPreprocessedPostModel
            uri=preprocessed.uri,
            cid=preprocessed.cid,
            indexed_at=preprocessed.indexed_at,
            author_did=preprocessed.author_did,
            author_handle=preprocessed.author_handle,
            author_avatar=preprocessed.author_avatar,
            author_display_name=preprocessed.author_display_name,
            created_at=preprocessed.created_at,
            text=preprocessed.text,
            embed=preprocessed.embed,
            entities=preprocessed.entities,
            facets=preprocessed.facets,
            labels=preprocessed.labels,
            langs=preprocessed.langs,
            reply_parent=preprocessed.reply_parent,
            reply_root=preprocessed.reply_root,
            tags=preprocessed.tags,
            synctimestamp=preprocessed.synctimestamp,
            url=preprocessed.url,
            source=preprocessed.source,
            like_count=preprocessed.like_count,
            reply_count=preprocessed.reply_count,
            repost_count=preprocessed.repost_count,
            passed_filters=preprocessed.passed_filters,
            filtered_at=preprocessed.filtered_at,
            filtered_by_func=preprocessed.filtered_by_func,
            preprocessing_timestamp=preprocessed.preprocessing_timestamp,
            # Fields from SociopoliticalLabelsModel
            llm_model_name=sociopolitical.llm_model_name,
            sociopolitical_was_successfully_labeled=sociopolitical.was_successfully_labeled,
            sociopolitical_reason=sociopolitical.reason,
            sociopolitical_label_timestamp=sociopolitical.label_timestamp,
            is_sociopolitical=sociopolitical.is_sociopolitical,
            political_ideology_label=sociopolitical.political_ideology_label,
            # Fields from PerspectiveApiLabelsModel
            perspective_was_successfully_labeled=perspective.was_successfully_labeled,
            perspective_reason=perspective.reason,
            perspective_label_timestamp=perspective.label_timestamp,
            prob_toxic=perspective.prob_toxic,
            prob_severe_toxic=perspective.prob_severe_toxic,
            prob_identity_attack=perspective.prob_identity_attack,
            prob_insult=perspective.prob_insult,
            prob_profanity=perspective.prob_profanity,
            prob_threat=perspective.prob_threat,
            prob_affinity=perspective.prob_affinity,
            prob_compassion=perspective.prob_compassion,
            prob_constructive=perspective.prob_constructive,
            prob_curiosity=perspective.prob_curiosity,
            prob_nuance=perspective.prob_nuance,
            prob_personal_story=perspective.prob_personal_story,
            prob_reasoning=perspective.prob_reasoning,
            prob_respect=perspective.prob_respect,
            prob_alienation=perspective.prob_alienation,
            prob_fearmongering=perspective.prob_fearmongering,
            prob_generalization=perspective.prob_generalization,
            prob_moral_outrage=perspective.prob_moral_outrage,
            prob_scapegoating=perspective.prob_scapegoating,
            prob_sexually_explicit=perspective.prob_sexually_explicit,
            prob_flirtation=perspective.prob_flirtation,
            prob_spam=perspective.prob_spam,
            # Fields from similarity_scores_results
            similarity_score=similarity.similarity_score,
            most_liked_average_embedding_key=similarity.most_liked_average_embedding_key,
        )

        consolidated_posts.append(consolidated_post)

    logger.info(f"Number of consolidated posts: {len(consolidated_posts)}")
    return consolidated_posts


def export_posts(posts: list[ConsolidatedEnrichedPostModel]):
    """Exports the posts to S3."""
    post_dicts = [post.dict() for post in posts]
    s3.write_dicts_jsonl_to_s3(data=post_dicts, key=root_s3_key)
    logger.info(f"Exported {len(post_dicts)} posts to S3.")


def do_consolidate_enrichment_integrations():
    """Do the enrichment consolidation."""
    # load previous session data
    latest_enrichment_consolidation_session: dict = (
        get_latest_enrichment_consolidation_session()
    )  # noqa
    if latest_enrichment_consolidation_session is not None:
        enrichment_consolidation_timestamp: str = (
            latest_enrichment_consolidation_session[
                "enrichment_consolidation_timestamp"
            ]["S"]  # noqa
        )
    else:
        enrichment_consolidation_timestamp = None

    # load data
    preprocessed_posts: list[FilteredPreprocessedPostModel] = (
        load_latest_preprocessed_posts(timestamp=enrichment_consolidation_timestamp)  # noqa
    )  # noqa
    perspective_api_labels: list[PerspectiveApiLabelsModel] = (
        load_latest_perspective_api_labels(timestamp=enrichment_consolidation_timestamp)  # noqa
    )  # noqa
    sociopolitical_labels: list[SociopoliticalLabelsModel] = (
        load_latest_sociopolitical_labels(timestamp=enrichment_consolidation_timestamp)  # noqa
    )  # noqa
    similarity_scores: list[PostSimilarityScoreModel] = load_latest_similarity_scores(
        timestamp=enrichment_consolidation_timestamp
    )  # noqa  # noqa

    # run enrichment consolidation
    consolidated_posts: list[ConsolidatedEnrichedPostModel] = (
        consolidate_enrichment_integrations(  # noqa
            preprocessed_posts=preprocessed_posts,
            perspective_api_labels=perspective_api_labels,
            sociopolitical_labels=sociopolitical_labels,
            similarity_scores=similarity_scores,
        )
    )

    # export results and update session metadata
    export_posts(consolidated_posts)
    enrichment_consolidation_session = {
        "enrichment_consolidation_timestamp": current_datetime_str,
        "total_posts_consolidated": len(consolidated_posts),
    }
    insert_enrichment_consolidation_session(enrichment_consolidation_session)


if __name__ == "__main__":
    do_consolidate_enrichment_integrations()
