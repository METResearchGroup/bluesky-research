"""Service for generating vector embeddings for posts."""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import torch

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import load_latest_data
from lib.helper import track_performance
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from services.generate_vector_embeddings.embedding_generation import (
    DEFAULT_EMBEDDING_SCHEMA_VERSION,
    EmbeddingGenerator,
    collect_embedded_uris_from_versioned_post_embeddings,
    get_device,
    posts_missing_embedding_keys,
)
from services.generate_vector_embeddings.models import PostSimilarityScoreModel
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


DEFAULT_EMBEDDING_MODEL_NAME = "bert-base-uncased"
DEFAULT_EMBEDDING_MODEL_REVISION = os.getenv("HF_EMBEDDING_MODEL_REVISION", "main")
vector_embeddings_root_s3_key = "vector_embeddings"

logger = get_logger(__name__)

athena = Athena()
dynamodb = DynamoDB()
s3 = S3()

dynamodb_table_name = "vector_embedding_sessions"
batch_size = 64

_embedding_generator: EmbeddingGenerator | None = None


def _get_embedding_generator() -> EmbeddingGenerator:
    """Lazily construct the shared encoder (loads weights on first use)."""
    global _embedding_generator
    if _embedding_generator is None:
        device = get_device()
        _embedding_generator = EmbeddingGenerator(
            model_name=DEFAULT_EMBEDDING_MODEL_NAME,
            revision=DEFAULT_EMBEDDING_MODEL_REVISION,
            device=device,
            batch_size=batch_size,
        )
        _embedding_generator.ensure_loaded()
    return _embedding_generator


def get_latest_embedding_session() -> dict | None:
    try:
        sessions: list[dict] = dynamodb.get_all_items_from_table(
            table_name=dynamodb_table_name
        )  # noqa
        if not sessions:
            logger.info("No embedding sessions found.")
            return None
        sorted_sessions = sorted(
            sessions,
            key=lambda x: x.get("embedding_timestamp", ""),
            reverse=True,
        )  # noqa
        return sorted_sessions[0]
    except Exception as e:
        logger.error(f"Failed to get latest embedding session: {e}")
        raise


def insert_embedding_session(embedding_session: dict):
    try:
        dynamodb.insert_item_into_table(
            item=embedding_session, table_name=dynamodb_table_name
        )
        logger.info(f"Successfully inserted embedding session: {embedding_session}")
    except Exception as e:
        logger.error(f"Failed to insert embedding session: {e}")
        raise


def get_posts_to_embed() -> list[FilteredPreprocessedPostModel]:
    """Get the posts to embed."""
    latest_embedding_session: dict | None = get_latest_embedding_session()
    if latest_embedding_session is None:
        logger.info("No latest embedding session found. Embedding all posts...")
        latest_embedding_timestamp = None
    else:
        latest_embedding_timestamp = latest_embedding_session["embedding_timestamp"]

    logger.info("Getting posts to embed.")
    posts_df: pd.DataFrame = load_latest_data(
        service="generate_vector_embeddings",
        latest_timestamp=latest_embedding_timestamp,
    )
    if len(posts_df) == 0:
        logger.info("No posts to embed.")
        return []
    df_dicts = posts_df.to_dict(orient="records")
    df_dicts = parse_converted_pandas_dicts(df_dicts)
    return [FilteredPreprocessedPostModel(**post_dict) for post_dict in df_dicts]  # noqa


def get_embeddings(
    texts: list[str], model_name=DEFAULT_EMBEDDING_MODEL_NAME
) -> torch.Tensor:
    """
    Generate embeddings for a list of texts using a specified model.

    Args:
        texts: Strings to embed.
        model_name: Reserved for future multi-model support; the active encoder
            uses ``DEFAULT_EMBEDDING_MODEL_NAME``.

    Returns:
        Tensor of shape ``(batch, 1, hidden_dim)`` (legacy layout for callers).
    """
    if model_name != DEFAULT_EMBEDDING_MODEL_NAME:
        logger.warning(
            "Requested embedding model %s differs from default %s; using default.",
            model_name,
            DEFAULT_EMBEDDING_MODEL_NAME,
        )
    logger.info(
        "Getting embeddings for %s texts with embedding model %s...",
        len(texts),
        DEFAULT_EMBEDDING_MODEL_NAME,
    )

    generator = _get_embedding_generator()
    embeddings = generator.embed_texts(texts)
    if embeddings.numel() == 0:
        return embeddings.reshape(0, 1, 0)
    return embeddings.unsqueeze(1)


def get_average_embedding(embeddings: torch.Tensor) -> torch.Tensor:
    """
    Calculate the average embedding from a batch of embeddings.

    Args:
        embeddings: Tensor of shape ``(batch, 1, hidden_dim)`` or ``(batch, hidden_dim)``.

    Returns:
        Average embedding of shape ``(1, hidden_dim)``.
    """
    if embeddings.numel() == 0:
        raise ValueError("The batch of embeddings is empty")

    if embeddings.dim() == 3:
        embeddings = embeddings.squeeze(1)

    average_embedding = torch.mean(embeddings, dim=0)
    return average_embedding.unsqueeze(0)


def get_previously_embedded_post_uris() -> set[str]:
    """Get the URIs of the posts that have already been embedded."""
    source_tables = ["in_network_embeddings", "most_liked_feed_embeddings"]
    query = " UNION ALL ".join([f"SELECT uri FROM {table}" for table in source_tables])
    df = athena.query_results_as_df(query)
    return set(df["uri"].tolist())


def _already_embedded_uris_for_current_encoder() -> set[str]:
    """Strict idempotency for the active model revision and embedding schema."""
    return collect_embedded_uris_from_versioned_post_embeddings(
        s3,
        vector_embeddings_root_s3_key=vector_embeddings_root_s3_key,
        embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
        embedding_model=DEFAULT_EMBEDDING_MODEL_NAME,
        embedding_model_revision=DEFAULT_EMBEDDING_MODEL_REVISION,
    )


@track_performance
def generate_vector_embeddings_and_calculate_similarity_scores(
    in_network_user_activity_posts: list[FilteredPreprocessedPostModel],
    most_liked_posts: list[FilteredPreprocessedPostModel],
):
    """Generate vector embeddings for posts."""
    previously_embedded_post_uris = get_previously_embedded_post_uris()
    in_network_user_activity_posts = [
        post
        for post in in_network_user_activity_posts
        if post.uri not in previously_embedded_post_uris
    ]
    most_liked_posts = [
        post
        for post in most_liked_posts
        if post.uri not in previously_embedded_post_uris
    ]
    versioned_embedded = _already_embedded_uris_for_current_encoder()
    in_network_user_activity_posts, _skipped_in_net = posts_missing_embedding_keys(
        in_network_user_activity_posts,
        versioned_embedded,
    )
    most_liked_posts, _skipped_most_liked = posts_missing_embedding_keys(
        most_liked_posts,
        versioned_embedded,
    )
    if _skipped_in_net or _skipped_most_liked:
        logger.info(
            "Skipped %s in-network and %s most-liked posts already embedded "
            "for schema=%s model=%s revision=%s",
            len(_skipped_in_net),
            len(_skipped_most_liked),
            DEFAULT_EMBEDDING_SCHEMA_VERSION,
            DEFAULT_EMBEDDING_MODEL_NAME,
            DEFAULT_EMBEDDING_MODEL_REVISION,
        )
    if len(in_network_user_activity_posts) == 0:
        logger.info("No in-network user activity posts to embed.")
        return {}

    in_network_user_activity_embeddings: torch.Tensor = get_embeddings(
        [post.text for post in in_network_user_activity_posts]
    )  # [batch, 1, hidden_dim]

    if most_liked_posts:
        most_liked_embeddings: torch.Tensor = get_embeddings(
            [post.text for post in most_liked_posts]
        )  # [batch, 1, hidden_dim]
        most_liked_average_embedding: torch.Tensor = get_average_embedding(
            most_liked_embeddings
        )  # [1, hidden_dim]
        latest_key = None
    else:
        logger.info(
            "No most liked posts to embed. Loading latest averaged embedding from S3."
        )
        prefix = os.path.join("vector_embeddings", "average_most_liked_feed_embeddings")
        keys: list[str] = s3.list_keys_given_prefix(prefix)
        latest_key: str = max(keys)
        embedding_df: pd.DataFrame = s3.read_parquet_from_s3(latest_key)
        embedding_arr: np.ndarray = np.array(embedding_df["embedding"][0][0])
        most_liked_average_embedding: torch.Tensor = torch.tensor(
            embedding_arr
        ).reshape(1, -1)
        most_liked_embeddings = torch.empty(
            (0, embedding_arr.shape[0]), dtype=torch.float32
        )

    post_cosine_similarity_scores: list[float] = []
    for post_embedding in in_network_user_activity_embeddings:
        row = post_embedding.reshape(1, -1).detach().cpu().numpy()
        centroid = most_liked_average_embedding.detach().cpu().numpy()
        post_cosine_similarity_scores.append(
            float(cosine_similarity(row, centroid)[0][0])
        )

    return {
        "in_network_user_activity_embeddings": in_network_user_activity_embeddings,
        "most_liked_embeddings": most_liked_embeddings,
        "most_liked_average_embedding": most_liked_average_embedding,
        "post_cosine_similarity_scores": post_cosine_similarity_scores,
        "previous_embedding_key": latest_key,
    }


def _legacy_embedding_row(
    post: FilteredPreprocessedPostModel,
    raw_embedding: torch.Tensor,
    *,
    timestamp: str,
    embedding_schema_version: str,
) -> dict:
    """Athena-compatible row plus revision/schema for downstream consumers."""
    vec = raw_embedding.flatten()
    return {
        "uri": post.uri,
        "embedding": vec.cpu().tolist(),
        "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
        "embedding_model_revision": DEFAULT_EMBEDDING_MODEL_REVISION,
        "embedding_schema_version": embedding_schema_version,
        "insert_timestamp": timestamp,
    }


@track_performance
def do_vector_embeddings():
    """Generate vector embeddings for posts and store them in S3."""
    posts_to_embed: list[FilteredPreprocessedPostModel] = get_posts_to_embed()
    versioned_embedded = _already_embedded_uris_for_current_encoder()
    posts_to_embed, skipped_posts = posts_missing_embedding_keys(
        posts_to_embed,
        versioned_embedded,
    )
    if skipped_posts:
        logger.info(
            "Skipping %s posts already embedded for schema=%s model=%s revision=%s",
            len(skipped_posts),
            DEFAULT_EMBEDDING_SCHEMA_VERSION,
            DEFAULT_EMBEDDING_MODEL_NAME,
            DEFAULT_EMBEDDING_MODEL_REVISION,
        )

    in_network_user_activity_posts: list[FilteredPreprocessedPostModel] = [
        post for post in posts_to_embed if post.source == "firehose"
    ]
    most_liked_posts: list[FilteredPreprocessedPostModel] = [
        post for post in posts_to_embed if post.source == "most_liked"
    ]

    logger.info(
        "Getting embeddings for %s in-network posts and %s most liked posts",
        len(in_network_user_activity_posts),
        len(most_liked_posts),
    )

    # generate embeddings and similarity scores
    res: dict = generate_vector_embeddings_and_calculate_similarity_scores(
        in_network_user_activity_posts,
        most_liked_posts,
    )
    if not res:
        logger.info("No embeddings to export.")
        return

    # export embeddings and similarity scores
    in_network_user_activity_embeddings: torch.Tensor = res[
        "in_network_user_activity_embeddings"
    ]  # noqa
    most_liked_embeddings: torch.Tensor = res["most_liked_embeddings"]  # noqa
    most_liked_average_embedding: torch.Tensor = res["most_liked_average_embedding"]  # noqa
    post_cosine_similarity_scores: list[float] = res["post_cosine_similarity_scores"]  # noqa
    timestamp = generate_current_datetime_str()

    versioned_post_embedding_key = os.path.join(
        vector_embeddings_root_s3_key,
        "post_embeddings",
        DEFAULT_EMBEDDING_SCHEMA_VERSION,
        f"{timestamp}.parquet",
    )

    in_network_post_embedding_key = os.path.join(
        vector_embeddings_root_s3_key,
        "in_network_post_embeddings",
        f"{timestamp}.parquet",
    )
    similarity_scores_key = os.path.join(
        vector_embeddings_root_s3_key,
        "similarity_scores",
        f"{timestamp}.parquet",
    )

    in_network_post_embedding_results: list[dict] = [
        _legacy_embedding_row(
            post,
            post_embedding,
            timestamp=timestamp,
            embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
        )
        for (post, post_embedding) in zip(
            in_network_user_activity_posts,
            in_network_user_activity_embeddings,
            strict=True,
        )
    ]

    versioned_post_embedding_results = list(in_network_post_embedding_results)
    if len(most_liked_posts) > 0:
        most_liked_rows = [
            _legacy_embedding_row(
                post,
                emb,
                timestamp=timestamp,
                embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
            )
            for post, emb in zip(most_liked_posts, most_liked_embeddings, strict=True)
        ]
        versioned_post_embedding_results.extend(most_liked_rows)

    # only write out data about the most-liked posts if there are any.
    if len(most_liked_posts) > 0:
        # export the most liked post embeddings.
        most_liked_post_embedding_key = os.path.join(
            vector_embeddings_root_s3_key,
            "most_liked_post_embeddings",
            f"{timestamp}.parquet",
        )
        most_liked_post_embedding_results: list[dict] = [
            _legacy_embedding_row(
                post,
                post_embedding,
                timestamp=timestamp,
                embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
            )
            for (post, post_embedding) in zip(
                most_liked_posts, most_liked_embeddings, strict=True
            )
        ]
        s3.write_dicts_parquet_to_s3(
            most_liked_post_embedding_results, most_liked_post_embedding_key
        )

        # export the averaged embeddings, since it will be new.
        average_most_liked_feed_embeddings_key = os.path.join(
            vector_embeddings_root_s3_key,
            "average_most_liked_feed_embeddings",
            f"{timestamp}.parquet",
        )
        average_most_liked_feed_embeddings: dict = {
            "uris": [post.uri for post in most_liked_posts],
            "embedding": most_liked_average_embedding.cpu().tolist(),
            "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
            "embedding_model_revision": DEFAULT_EMBEDDING_MODEL_REVISION,
            "embedding_schema_version": DEFAULT_EMBEDDING_SCHEMA_VERSION,
            "insert_timestamp": timestamp,
        }
        s3.write_dict_parquet_to_s3(
            average_most_liked_feed_embeddings, average_most_liked_feed_embeddings_key
        )
    else:
        most_liked_post_embedding_key = "<No most-liked posts>"
        average_most_liked_feed_embeddings_key = res["previous_embedding_key"]

    similarity_scores_results: list[dict] = [
        PostSimilarityScoreModel(
            uri=post.uri,
            similarity_score=score,
            insert_timestamp=timestamp,
            most_liked_average_embedding_key=average_most_liked_feed_embeddings_key,  # noqa
        ).model_dump()
        for (post, score) in zip(
            in_network_user_activity_posts, post_cosine_similarity_scores, strict=True
        )
    ]

    s3.write_dicts_parquet_to_s3(
        versioned_post_embedding_results,
        versioned_post_embedding_key,
    )
    s3.write_dicts_parquet_to_s3(
        in_network_post_embedding_results, in_network_post_embedding_key
    )
    s3.write_dicts_parquet_to_s3(similarity_scores_results, similarity_scores_key)

    logger.info(
        "Exported embeddings and similarity scores to %s, %s, %s, %s, %s",
        versioned_post_embedding_key,
        in_network_post_embedding_key,
        most_liked_post_embedding_key,
        average_most_liked_feed_embeddings_key,
        similarity_scores_key,
    )

    labeling_session = {
        "embedding_timestamp": timestamp,
        "total_embedded_posts": len(in_network_user_activity_posts)
        + len(most_liked_posts),
        "total_embedded_posts_by_source": {
            "in_network_user_activity_posts": len(in_network_user_activity_posts),
            "most_liked_posts": len(most_liked_posts),
        },
        "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
        "embedding_model_revision": DEFAULT_EMBEDDING_MODEL_REVISION,
        "embedding_schema_version": DEFAULT_EMBEDDING_SCHEMA_VERSION,
        "s3_keys": {
            "post_embeddings_versioned": versioned_post_embedding_key,
            "in_network_post_embeddings": in_network_post_embedding_key,
            "most_liked_post_embeddings": most_liked_post_embedding_key,
            "average_most_liked_feed_embeddings": average_most_liked_feed_embeddings_key,
            "similarity_scores": similarity_scores_key,
        },
    }
    insert_embedding_session(labeling_session)


if __name__ == "__main__":
    do_vector_embeddings()
