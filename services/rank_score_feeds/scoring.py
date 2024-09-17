import numpy as np

from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa

# TODO: I should check this empirically.
default_similarity_score = 0.75
# TODO: I should check this empirically.
average_popular_post_like_count = 250
coef_toxicity = 0.965
coef_constructiveness = 1.02


# TODO: add superposter information.
def score_treatment_algorithm(post: ConsolidatedEnrichedPostModel) -> dict:
    """Score a post's text attributes."""
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
        return treatment_coef
    else:
        return 1.0


# TODO: get a cap on how much I want this to affect scores
# e.g., if I want this to be limited to [0, 0.75], then I can
# set those as the limits and then create an appropriate
# exponential decay function
# NOTE: this should also relate to the max # of days
# that I want to consider for posts in the feed.
def score_post_freshness(post: ConsolidatedEnrichedPostModel) -> float:
    """Score a post's freshness.

    Implemented as an exponential decay function, where the older the post,
    the lower the score.
    """
    pass


def score_post_likeability(post: ConsolidatedEnrichedPostModel) -> float:
    """Score a post's likeability. We either use the actual like count or we
    use an estimation of the like count, based on how similar that post is to
    posts that are normally liked.
    """
    like_score = None
    if post.like_count:
        like_score = np.log(post.like_count + 1)
    elif post.similarity_score:
        expected_like_count = average_popular_post_like_count * post.similarity_score
        like_score = np.log(expected_like_count + 1)
    else:
        expected_like_count = average_popular_post_like_count * default_similarity_score
        like_score = np.log(expected_like_count + 1)
    return like_score


def calculate_post_score(post: ConsolidatedEnrichedPostModel) -> float:
    """Calculate a post's score.

    Calculates what a score's post would be, depending on whether or not it's
    used for the engagement or the treatment condition.
    """
    engagement_score = 0
    treatment_score = 0
    engagement_coef = 1.0
    treatment_coef: float = score_treatment_algorithm(post=post)

    # set the base score to be based on the likeability of the post
    # adn the freshness of the post.
    engagement_score += score_post_likeability(post=post)
    engagement_score += score_post_freshness(post=post)
    treatment_score += score_post_likeability(post=post)
    treatment_score += score_post_freshness(post=post)

    # multiply scores by the engagement/treatment coefs.
    engagement_score *= engagement_coef
    treatment_score *= treatment_coef

    return {"engagement_score": engagement_score, "treatment_score": treatment_score}


def calculate_post_scores(posts: list[ConsolidatedEnrichedPostModel]) -> list[dict]:  # noqa
    """Calculate scores for a list of posts."""
    return [calculate_post_score(post) for post in posts]
