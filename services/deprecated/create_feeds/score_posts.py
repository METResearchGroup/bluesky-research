import numpy as np

from services.ml_inference.models import RecordClassificationMetadataModel


TOXICITY_COEF = 0.965
CONSTRUCTIVENESS_COEF = 1.02
SUPERPOSTER_COEF = 0.90
SIMILAR_POST_COEF = 1.25


def calculate_similarity_to_previous_user_likes(post, user) -> float:
    """Calculates cosine similarity between the post and the user's previously
    liked posts.
    """
    pass


def calculate_weighted_coef() -> float:
    return 1


def score_post(
    post: RecordClassificationMetadataModel, calculate_similarity: bool = False
) -> float:
    pass
    log_likes = np.log(post.like_count + 1)
    coef = calculate_weighted_coef()
    return log_likes + (log_likes * coef)


def score_posts(posts: list) -> list[float]:
    # TODO: load the Perspective API labels here.
    return [score_post(post) for post in posts]
