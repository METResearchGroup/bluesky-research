import numpy as np

from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa

default_similarity_score = 0.75
average_popular_post_like_count = 250
coef_toxicity = 0.965
coef_constructiveness = 1.02


def calculate_post_score(post: ConsolidatedEnrichedPostModel) -> float:
    """Calculate a post's score.

    Calculates what a score's post would be, depending on whether or not it's
    used for the engagement or the treatment condition.
    """
    engagement_score = None
    treatment_score = None
    engagement_coef = 1.0
    if (
        post.sociopolitical_was_successfully_labeled
        and post.is_sociopolitical
        and post.perspective_was_successfully_labeled
    ):
        if post.prob_constructive is None:
            # backwards-compatbility bug where prob_constructive wasn't
            # set, but the endpoints for prob_reasoning and prob_constructive
            # are actually the same.
            post.prob_constructive = post.prob_reasoning
        # if sociopolitical, uprank/downrank based on toxicity/constructiveness.
        treatment_coef = (
            post.prob_toxic * coef_toxicity
            + post.prob_constructive * coef_constructiveness
        ) / (post.prob_toxic + post.prob_constructive)
    else:
        # if it's not sociopolitical, use a coef of 1.
        treatment_coef = 1.0
    if post.like_count:
        engagement_score = engagement_coef * np.log(post.like_count + 1)
        treatment_score = treatment_coef * np.log(post.like_count + 1)
    elif post.similarity_score:
        expected_like_count = average_popular_post_like_count * post.similarity_score
        engagement_score = engagement_coef * np.log(expected_like_count + 1)
        treatment_score = treatment_coef * np.log(expected_like_count + 1)
    else:
        # if we didn't have a like count or similarity score, return a
        # score where we assume both of those.
        expected_like_count = average_popular_post_like_count * default_similarity_score
        engagement_score = engagement_coef * np.log(expected_like_count + 1)
        treatment_score = treatment_coef * np.log(expected_like_count + 1)
    return {"engagement_score": engagement_score, "treatment_score": treatment_score}


def calculate_post_scores(posts: list[ConsolidatedEnrichedPostModel]) -> list[dict]:  # noqa
    """Calculate scores for a list of posts."""
    return [calculate_post_score(post) for post in posts]
