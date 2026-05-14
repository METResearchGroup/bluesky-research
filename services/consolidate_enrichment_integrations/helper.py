"""Helper functions for the consolidate_enrichment_integrations service."""

# ruff: noqa: E402

from datetime import datetime, timedelta, timezone

import pandas as pd

_dynamodb = None
from lib.constants import timestamp_format
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA

try:
    from lib.helper import track_performance
except ModuleNotFoundError:  # pragma: no cover - local lightweight fallback

    def track_performance(func=None, *args, **kwargs):
        if func is None:
            return lambda inner: inner
        return func


from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from services.consolidate_enrichment_integrations.loaders import (
    load_latest_perspective_api_labels,
    load_latest_preprocessed_posts,
    load_latest_similarity_scores,
    load_latest_sociopolitical_labels,
    load_previously_consolidated_enriched_post_uris,
)
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

dynamodb_table_name = "enrichment_consolidation_sessions"


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        from lib.aws.dynamodb import DynamoDB

        _dynamodb = DynamoDB()
    return _dynamodb


def _build_consolidated_post(
    *,
    preprocessed: FilteredPreprocessedPostModel,
    perspective: PerspectiveApiLabelsModel | None,
    sociopolitical: SociopoliticalLabelsModel | None,
    similarity: PostSimilarityScoreModel | None,
) -> ConsolidatedEnrichedPostModel:
    return ConsolidatedEnrichedPostModel(
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
        source=preprocessed.source,  # type: ignore
        like_count=preprocessed.like_count,
        reply_count=preprocessed.reply_count,
        repost_count=preprocessed.repost_count,
        passed_filters=preprocessed.passed_filters,
        filtered_at=preprocessed.filtered_at,
        filtered_by_func=preprocessed.filtered_by_func,
        preprocessing_timestamp=preprocessed.preprocessing_timestamp,
        llm_model_name=(sociopolitical.llm_model_name if sociopolitical else None),
        sociopolitical_was_successfully_labeled=(
            sociopolitical.was_successfully_labeled if sociopolitical else False
        ),
        sociopolitical_reason=(sociopolitical.reason if sociopolitical else None),
        sociopolitical_label_timestamp=(
            sociopolitical.label_timestamp if sociopolitical else None
        ),
        is_sociopolitical=(
            sociopolitical.is_sociopolitical if sociopolitical else False
        ),
        political_ideology_label=(
            sociopolitical.political_ideology_label if sociopolitical else None
        ),
        perspective_was_successfully_labeled=(
            perspective.was_successfully_labeled if perspective else False
        ),
        perspective_reason=(perspective.reason if perspective else None),
        perspective_label_timestamp=(
            perspective.label_timestamp if perspective else None
        ),
        prob_toxic=(perspective.prob_toxic if perspective else 0),
        prob_severe_toxic=(perspective.prob_severe_toxic if perspective else 0),
        prob_identity_attack=(perspective.prob_identity_attack if perspective else 0),
        prob_insult=(perspective.prob_insult if perspective else 0),
        prob_profanity=(perspective.prob_profanity if perspective else 0),
        prob_threat=(perspective.prob_threat if perspective else 0),
        prob_affinity=(perspective.prob_affinity if perspective else 0),
        prob_compassion=(perspective.prob_compassion if perspective else 0),
        prob_constructive=(perspective.prob_constructive if perspective else 0),
        prob_curiosity=(perspective.prob_curiosity if perspective else 0),
        prob_nuance=(perspective.prob_nuance if perspective else 0),
        prob_personal_story=(perspective.prob_personal_story if perspective else 0),
        prob_reasoning=(perspective.prob_reasoning if perspective else 0),
        prob_respect=(perspective.prob_respect if perspective else 0),
        prob_alienation=(perspective.prob_alienation if perspective else 0),
        prob_fearmongering=(perspective.prob_fearmongering if perspective else 0),
        prob_generalization=(perspective.prob_generalization if perspective else 0),
        prob_moral_outrage=(perspective.prob_moral_outrage if perspective else 0),
        prob_scapegoating=(perspective.prob_scapegoating if perspective else 0),
        prob_sexually_explicit=(
            perspective.prob_sexually_explicit if perspective else 0
        ),
        prob_flirtation=(perspective.prob_flirtation if perspective else 0),
        prob_spam=(perspective.prob_spam if perspective else 0),
        similarity_score=similarity.similarity_score if similarity else None,
        most_liked_average_embedding_key=(
            similarity.most_liked_average_embedding_key if similarity else None
        ),
        consolidation_timestamp=generate_current_datetime_str(),
    )


def get_latest_enrichment_consolidation_session() -> dict | None:
    """Get the latest enrichment consolidation session."""
    try:
        sessions: list[dict] = _get_dynamodb().get_all_items_from_table(
            table_name=dynamodb_table_name
        )  # noqa
        if not sessions:
            logger.info("No enrichment consolidation sessions found.")
            return None
        sorted_sessions = sorted(
            sessions,
            key=lambda x: x.get("enrichment_consolidation_timestamp", ""),  # noqa
            reverse=True,
        )  # noqa
        return sorted_sessions[0]
    except Exception as e:
        logger.error(f"Failed to get latest enrichment consolidation session: {e}")  # noqa
        raise


def insert_enrichment_consolidation_session(enrichment_consolidation_session: dict):  # noqa
    """Insert the enrichment consolidation session."""
    try:
        _get_dynamodb().insert_item_into_table(
            item=enrichment_consolidation_session, table_name=dynamodb_table_name
        )
        logger.info(
            f"Successfully inserted enrichment consolidation session: {enrichment_consolidation_session}"
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to insert enrichment consolidation session: {e}")  # noqa
        raise


@track_performance
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
    all_uris = set(preprocessed_dict.keys())

    # remove those that have been previously processed.
    all_uris = all_uris - load_previously_consolidated_enriched_post_uris()

    logger.info(
        f"Number of shared URIs to combine enrichment sources for: {len(all_uris)}"
    )  # noqa

    consolidated_posts: list[ConsolidatedEnrichedPostModel] = []

    for uri in all_uris:
        preprocessed = preprocessed_dict[uri]
        perspective = perspective_dict.get(uri, None)
        sociopolitical = sociopolitical_dict.get(uri, None)
        similarity = similarity_dict.get(uri, None)

        consolidated_posts.append(
            _build_consolidated_post(
                preprocessed=preprocessed,
                perspective=perspective,
                sociopolitical=sociopolitical,
                similarity=similarity,
            )
        )

    total = len([post for post in consolidated_posts if post.source == "most_liked"])
    print(f"Total most-liked posts: {total}")

    logger.info(f"Number of consolidated posts: {len(consolidated_posts)}")
    return consolidated_posts


def export_posts(posts: list[ConsolidatedEnrichedPostModel]):
    """Exports the posts to S3."""
    if len(posts) == 0:
        logger.info("No posts to export.")
        return
    post_dicts = [post.dict() for post in posts]
    dtype_map = MAP_SERVICE_TO_METADATA["consolidated_enriched_post_records"][
        "dtypes_map"
    ]  # noqa
    df = pd.DataFrame(post_dicts)
    df["partition_date"] = pd.to_datetime(
        df["consolidation_timestamp"], format=timestamp_format
    ).dt.date
    df = df.astype(dtype_map)
    export_data_to_local_storage(service="consolidated_enriched_post_records", df=df)
    logger.info(f"Exported {len(post_dicts)} consolidated enriched posts.")


@track_performance
def do_consolidate_enrichment_integrations(
    backfill_period: str | None = None, backfill_duration: int | None = None
):
    """Do the enrichment consolidation.

    Also includes optional backfill period and backfill duration.
    """
    backfill_time: datetime | None = None
    if backfill_duration is not None and backfill_period in ["days", "hours"]:
        current_time = datetime.now(timezone.utc)
        if backfill_period == "days":
            backfill_time = current_time - timedelta(days=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} days of data.")
        elif backfill_period == "hours":
            backfill_time = current_time - timedelta(hours=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} hours of data.")

    # load previous session data
    latest_enrichment_consolidation_session: dict | None = (
        get_latest_enrichment_consolidation_session()
    )  # noqa
    enrichment_consolidation_timestamp: str | None = None
    if latest_enrichment_consolidation_session is not None:
        enrichment_consolidation_timestamp = (
            latest_enrichment_consolidation_session[
                "enrichment_consolidation_timestamp"
            ]  # noqa
        )

    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = enrichment_consolidation_timestamp

    # load data
    preprocessed_posts: list[FilteredPreprocessedPostModel] = (
        load_latest_preprocessed_posts(timestamp=timestamp)
    )  # noqa
    perspective_api_labels: list[PerspectiveApiLabelsModel] = (
        load_latest_perspective_api_labels(timestamp=timestamp)
    )  # noqa
    sociopolitical_labels: list[SociopoliticalLabelsModel] = (
        load_latest_sociopolitical_labels(timestamp=timestamp)
    )  # noqa
    similarity_scores: list[PostSimilarityScoreModel] = load_latest_similarity_scores(
        timestamp=timestamp
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

    firehose_posts = [post for post in consolidated_posts if post.source == "firehose"]  # noqa
    most_liked_posts = [
        post for post in consolidated_posts if post.source == "most_liked"
    ]  # noqa

    # export results and update session metadata
    export_posts(consolidated_posts)
    timestamp = generate_current_datetime_str()
    enrichment_consolidation_session = {
        "enrichment_consolidation_timestamp": timestamp,
        "total_posts_consolidated": len(consolidated_posts),
        "consolidated_in_network_posts": len(firehose_posts),
        "consolidated_most_liked_posts": len(most_liked_posts),
    }
    insert_enrichment_consolidation_session(enrichment_consolidation_session)


if __name__ == "__main__":
    do_consolidate_enrichment_integrations()
