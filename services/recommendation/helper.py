"""Helper file for recommendation service."""
from typing import Literal

from lib.aws.s3 import current_timestamp, RECOMMENDATIONS_KEY_ROOT, S3


# TODO: update preprocessing code to write posts to a directory based on
# `current_timestamp`.
def load_preprocessed_posts(recency_window_days: int = 3) -> list[dict]:
    """Loads preprocessed posts from S3, within a certain recency window
    """
    return []


def load_users() -> dict[str, dict]:
    """Loads users from S3.

    Returns a dict: {
        user_id: user object (dict)
    }
    """
    return {}


def generate_recommendations(
    algorithm: str, preprocessed_posts: list[dict], users: dict[str, dict]
) -> dict:
    """Generates recommendations for users.

    Creates a dict of user_id: recommendations (list[dict] of posts).
    """
    return {}


def write_recommendations(recommendations: dict[str, list[dict]]) -> None:
    """Writes recommendations to S3."""
    s3 = S3()
    for (user_id, user_recommendations) in recommendations.items():
        key = s3.create_partitioned_key(
            key_root=RECOMMENDATIONS_KEY_ROOT,
            userid=user_id,
            timestamp=current_timestamp,
            filename=f"recommendations.jsonl"
        )
        s3.write_dicts_jsonl_to_s3(
            data=user_recommendations, key=key
        )


def main(
    algorithm: Literal["reverse_chronological", "attention", "representation"]
) -> None:
    preprocessed_posts: list[dict] = load_preprocessed_posts()
    users: dict[str, dict] = load_users()
    recommendations: dict[str, list[dict]] = generate_recommendations(
        algorithm=algorithm, preprocessed_posts=preprocessed_posts, users=users
    )
    write_recommendations(recommendations)
