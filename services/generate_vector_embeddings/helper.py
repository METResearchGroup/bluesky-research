"""Service for generating vector embeddings for posts."""

import json

from sklearn.metrics.pairwise import cosine_similarity
import torch
from transformers import AutoTokenizer, AutoModel

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


DEFAULT_EMBEDDING_MODEL_NAME = "bert-base-uncased"
vector_embeddings_root_s3_key = "vector_embeddings"

logger = get_logger(__name__)

athena = Athena()
dynamodb = DynamoDB()
s3 = S3()

dynamodb_table_name = "vector_embedding_sessions"


def get_latest_embedding_session() -> dict:
    try:
        sessions: list[dict] = dynamodb.get_all_items_from_table(
            table_name=dynamodb_table_name
        )  # noqa
        if not sessions:
            logger.info("No embedding sessions found.")
            return None
        sorted_sessions = sorted(
            sessions,
            key=lambda x: x.get("embedding_timestamp", {}).get("S", ""),
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
    latest_embedding_session = get_latest_embedding_session()
    if latest_embedding_session is None:
        logger.info("No latest embedding session found. Embedding all posts...")
        latest_embedding_timestamp = None
    else:
        latest_embedding_timestamp = latest_embedding_session["embedding_timestamp"]

    logger.info("Getting posts to embed.")
    posts = athena.get_latest_preprocessed_posts(timestamp=latest_embedding_timestamp)
    return posts


def get_embeddings(text: str, model_name=DEFAULT_EMBEDDING_MODEL_NAME) -> torch.Tensor:
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)  # noqa
    with torch.no_grad():
        outputs = model(**inputs)

    # Use the mean of the token embeddings as the sentence embedding
    embeddings = outputs.last_hidden_state.mean(dim=1)
    return embeddings


def get_average_embedding(embeddings: list[torch.Tensor]) -> torch.Tensor:
    """
    Calculate the average embedding from a list of embeddings.

    Args:
    embeddings (list[torch.Tensor]): A list of tensors, each of shape (1, 768)

    Returns:
    torch.Tensor: The average embedding of shape (1, 768)
    """
    if not embeddings:
        raise ValueError("The list of embeddings is empty")

    # Stack the tensors along a new dimension
    stacked_embeddings = torch.stack(embeddings)

    # Calculate the mean along the first dimension (across all embeddings)
    average_embedding = torch.mean(stacked_embeddings, dim=0)

    return average_embedding


def get_previously_embedded_post_uris() -> set[str]:
    """Get the URIs of the posts that have already been embedded."""
    return set()


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
    in_network_user_activity_embeddings: list[torch.Tensor] = [
        get_embeddings(post.text) for post in in_network_user_activity_posts
    ]
    most_liked_embeddings: list[torch.Tensor] = [
        get_embeddings(post.text) for post in most_liked_posts
    ]
    most_liked_average_embedding: torch.Tensor = get_average_embedding(
        most_liked_embeddings
    )
    post_cosine_similarity_scores = [
        cosine_similarity(post_embedding, most_liked_average_embedding)
        for post_embedding in in_network_user_activity_embeddings
    ]
    return {
        "in_network_user_activity_embeddings": in_network_user_activity_embeddings,
        "most_liked_embeddings": most_liked_embeddings,
        "most_liked_average_embedding": most_liked_average_embedding,
        "post_cosine_similarity_scores": post_cosine_similarity_scores,
    }


def do_vector_embeddings():
    """Generate vector embeddings for posts and store them in S3."""
    posts_to_embed: list[FilteredPreprocessedPostModel] = get_posts_to_embed()
    in_network_user_activity_posts: list[FilteredPreprocessedPostModel] = [
        post for post in posts_to_embed if post.source == "firehose"
    ]
    most_liked_posts: list[FilteredPreprocessedPostModel] = [
        post for post in posts_to_embed if post.source == "most_liked"
    ]

    logger.info(
        f"Getting embeddings for {len(in_network_user_activity_posts)} in-network posts and {len(most_liked_posts)} most liked posts"
    )  # noqa

    # generate embeddings and similarity scores
    res = generate_vector_embeddings_and_calculate_similarity_scores(
        in_network_user_activity_posts,
        most_liked_posts,
    )

    # export embeddings and similarity scores
    in_network_user_activity_embeddings = res["in_network_user_activity_embeddings"]
    most_liked_embeddings = res["most_liked_embeddings"]
    most_liked_average_embedding = res["most_liked_average_embedding"]
    post_cosine_similarity_scores = res["post_cosine_similarity_scores"]

    in_network_post_embedding_key = f"{vector_embeddings_root_s3_key}/in_network_post_embeddings/{current_datetime_str}.parquet"
    most_liked_post_embedding_key = f"{vector_embeddings_root_s3_key}/most_liked_post_embeddings/{current_datetime_str}.parquet"
    average_most_liked_feed_embeddings_key = f"{vector_embeddings_root_s3_key}/average_most_liked_feed_embeddings/{current_datetime_str}.parquet"
    similarity_scores_key = f"{vector_embeddings_root_s3_key}/similarity_scores/{current_datetime_str}.parquet"

    in_network_post_embedding_results: list[dict] = [
        {
            "uri": post.uri,
            "embedding": post_embedding.tolist(),  # convert tensor to list. Necessary?
            "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
            "insert_timestamp": current_datetime_str,
        }
        for (post, post_embedding) in zip(
            in_network_user_activity_posts, in_network_user_activity_embeddings
        )
    ]
    most_liked_post_embedding_results: list[dict] = [
        {
            "uri": post.uri,
            "embedding": post_embedding.tolist(),  # convert tensor to list. Necessary?
            "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
            "insert_timestamp": current_datetime_str,
        }
        for (post, post_embedding) in zip(most_liked_posts, most_liked_embeddings)
    ]
    average_most_liked_feed_embeddings: dict = {
        "uris": [post.uri for post in most_liked_posts],
        "embedding": most_liked_average_embedding.tolist(),
        "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
        "insert_timestamp": current_datetime_str,
    }
    similarity_scores_results: list[dict] = [
        {
            "uri": post.uri,
            "similarity_score": score,
            "insert_timestamp": current_datetime_str,
            "most_liked_average_embedding_key": average_most_liked_feed_embeddings_key,  # noqa
        }
        for (post, score) in zip(
            in_network_user_activity_posts, post_cosine_similarity_scores
        )
    ]

    with open(in_network_post_embedding_key, "w") as f:
        f.write(json.dumps(in_network_post_embedding_results))
    with open(most_liked_post_embedding_key, "w") as f:
        f.write(json.dumps(most_liked_post_embedding_results))
    with open(average_most_liked_feed_embeddings_key, "w") as f:
        f.write(json.dumps(average_most_liked_feed_embeddings))
    with open(similarity_scores_key, "w") as f:
        f.write(json.dumps(similarity_scores_results))

    logger.info(
        f"Exported embeddings and similarity scores to {in_network_post_embedding_key}, {most_liked_post_embedding_key}, {average_most_liked_feed_embeddings_key}, {similarity_scores_key}"
    )  # noqa

    labeling_session = {
        "embedding_timestamp": current_datetime_str,
        "total_embedded_posts": len(in_network_user_activity_posts)
        + len(most_liked_posts),
        "total_embedded_posts_by_source": {
            "in_network_user_activity_posts": len(in_network_user_activity_posts),
            "most_liked_posts": len(most_liked_posts),
        },
        "s3_keys": {
            "in_network_post_embeddings": in_network_post_embedding_key,
            "most_liked_post_embeddings": most_liked_post_embedding_key,
            "average_most_liked_feed_embeddings": average_most_liked_feed_embeddings_key,
            "similarity_scores": similarity_scores_key,
        },
    }
    insert_embedding_session(labeling_session)


if __name__ == "__main__":
    text = "The fact that the Democrats are in power is a good thing."
    text2 = "I am glad that Joe Biden is president."
    embeddings = get_embeddings(text)
    embeddings2 = get_embeddings(text2)
    print(cosine_similarity(embeddings, embeddings2))
    breakpoint()
