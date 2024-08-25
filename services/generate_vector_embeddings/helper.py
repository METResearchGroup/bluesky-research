"""Service for generating vector embeddings for posts."""

import os

from sklearn.metrics.pairwise import cosine_similarity
import torch
from transformers import AutoTokenizer, AutoModel

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


DEFAULT_EMBEDDING_MODEL_NAME = "bert-base-uncased"
vector_embeddings_root_s3_key = "vector_embeddings"

logger = get_logger(__name__)

athena = Athena()
dynamodb = DynamoDB()
s3 = S3()

dynamodb_table_name = "vector_embedding_sessions"
batch_size = 64


def get_device():
    if torch.cuda.is_available():
        print("CUDA backend available.")
        device = torch.device("cuda")
    elif torch.backends.mps.is_available() and torch.backends.mps.is_built():
        print("Arm mac GPU available, using GPU.")
        print(f"{torch.backends.mps.is_available()=}")
        print(f"{torch.backends.mps.is_built()=}")
        device = torch.device("mps")  # for Arm Macs
    else:
        print("GPU not available, using CPU")
        device = torch.device("cpu")
        raise ValueError("GPU not available, using CPU")
    return device


torch.cuda.empty_cache()
device = get_device()
tokenizer = AutoTokenizer.from_pretrained(DEFAULT_EMBEDDING_MODEL_NAME)
model = AutoModel.from_pretrained(DEFAULT_EMBEDDING_MODEL_NAME).to(device)


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


def get_embeddings(
    texts: list[str], model_name=DEFAULT_EMBEDDING_MODEL_NAME
) -> torch.Tensor:
    """
    Generate embeddings for a list of texts using a specified model.

    Args:
    texts (list[str]): A list of text strings to embed.
    model_name (str): The name of the pre-trained model to use for embedding.

    Returns:
    torch.Tensor: A tensor of shape (batch, 1, 768) containing the embeddings.

    This function tokenizes the input texts, passes them through the specified model,
    and returns the mean of the last hidden state as the embedding for each text.
    The output is reshaped to (batch, 1, 768) to match the expected format.
    """
    logger.info(
        f"Getting embeddings for {len(texts)} texts with embedding model {model_name}..."
    )  # noqa

    all_embeddings = []

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        inputs = tokenizer(
            batch_texts, return_tensors="pt", padding=True, truncation=True
        ).to(device)

        with torch.no_grad():
            outputs = model(**inputs)

        # Use the mean of the token embeddings as the sentence embedding
        embeddings = outputs.last_hidden_state.mean(dim=1)  # (batch, 768)

        # Reshape to (batch, 1, 768)
        embeddings = embeddings.unsqueeze(1)

        all_embeddings.append(
            embeddings.cpu()
        )  # Move the result back to CPU if it was on GPU

        # Clear cache to free up memory
        if device == torch.device("cuda") or device == torch.device("mps"):
            torch.cuda.empty_cache()

    return torch.cat(all_embeddings, dim=0)


def get_average_embedding(embeddings: torch.Tensor) -> torch.Tensor:
    """
    Calculate the average embedding from a batch of embeddings.

    Args:
    embeddings (torch.Tensor): A tensor of shape (batch, 1, 768)

    Returns:
    torch.Tensor: The average embedding of shape (1, 768)
    """
    if embeddings.size(0) == 0:
        raise ValueError("The batch of embeddings is empty")

    # Calculate the mean along the batch dimension
    average_embedding = torch.mean(embeddings, dim=0)

    # Squeeze the result to get a (1, 768) tensor
    average_embedding = average_embedding.squeeze(0)

    return average_embedding


# TODO: implement. Can fetch the URIs from Athena table.
def get_previously_embedded_post_uris() -> set[str]:
    """Get the URIs of the posts that have already been embedded."""
    return set()


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
    in_network_user_activity_embeddings: torch.Tensor = get_embeddings(
        [post.text for post in in_network_user_activity_posts]
    )  # [batch, 1, 768]
    most_liked_embeddings: torch.Tensor = get_embeddings(
        [post.text for post in most_liked_posts]
    )  # [batch, 1, 768]
    most_liked_average_embedding: torch.Tensor = get_average_embedding(
        most_liked_embeddings
    ).reshape(1, -1)  # [1, 768]

    post_cosine_similarity_scores: list[float] = [
        # [1, 768] -> [768] for post_embedding, to match most_liked_average_embedding
        cosine_similarity(post_embedding, most_liked_average_embedding)[0][0].item()
        for post_embedding in in_network_user_activity_embeddings
    ]

    return {
        "in_network_user_activity_embeddings": in_network_user_activity_embeddings,
        "most_liked_embeddings": most_liked_embeddings,
        "most_liked_average_embedding": most_liked_average_embedding,
        "post_cosine_similarity_scores": post_cosine_similarity_scores,
    }


@track_performance
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
    res: dict = generate_vector_embeddings_and_calculate_similarity_scores(
        in_network_user_activity_posts,
        most_liked_posts,
    )

    # export embeddings and similarity scores
    in_network_user_activity_embeddings: torch.Tensor = res[
        "in_network_user_activity_embeddings"
    ]  # noqa
    most_liked_embeddings: torch.Tensor = res["most_liked_embeddings"]  # noqa
    most_liked_average_embedding: torch.Tensor = res["most_liked_average_embedding"]  # noqa
    post_cosine_similarity_scores: list[float] = res["post_cosine_similarity_scores"]  # noqa

    in_network_post_embedding_key = os.path.join(
        vector_embeddings_root_s3_key,
        "in_network_post_embeddings",
        f"{current_datetime_str}.parquet",
    )
    most_liked_post_embedding_key = os.path.join(
        vector_embeddings_root_s3_key,
        "most_liked_post_embeddings",
        f"{current_datetime_str}.parquet",
    )
    average_most_liked_feed_embeddings_key = os.path.join(
        vector_embeddings_root_s3_key,
        "average_most_liked_feed_embeddings",
        f"{current_datetime_str}.parquet",
    )
    similarity_scores_key = os.path.join(
        vector_embeddings_root_s3_key,
        "similarity_scores",
        f"{current_datetime_str}.parquet",
    )

    in_network_post_embedding_results: list[dict] = [
        {
            "uri": post.uri,
            "embedding": post_embedding.cpu().tolist(),  # convert tensor to list. Necessary?
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
            "embedding": post_embedding.cpu().tolist(),  # convert tensor to list. Necessary?
            "embedding_model": DEFAULT_EMBEDDING_MODEL_NAME,
            "insert_timestamp": current_datetime_str,
        }
        for (post, post_embedding) in zip(most_liked_posts, most_liked_embeddings)
    ]
    average_most_liked_feed_embeddings: dict = {
        "uris": [post.uri for post in most_liked_posts],
        "embedding": most_liked_average_embedding.cpu().tolist(),
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

    s3.write_dicts_parquet_to_s3(
        in_network_post_embedding_results, in_network_post_embedding_key
    )
    s3.write_dicts_parquet_to_s3(
        most_liked_post_embedding_results, most_liked_post_embedding_key
    )
    s3.write_dict_parquet_to_s3(
        average_most_liked_feed_embeddings, average_most_liked_feed_embeddings_key
    )
    s3.write_dicts_parquet_to_s3(similarity_scores_results, similarity_scores_key)

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
    do_vector_embeddings()
