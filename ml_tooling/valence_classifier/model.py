"""Use the Vader classifer to classify valence.

As per the Github page: https://github.com/cjhutto/vaderSentiment?tab=readme-ov-file#about-the-scoring
positive sentiment: compound score >= 0.05
neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
negative sentiment: compound score <= -0.05
"""


def batch_classify_posts(posts: list[dict]) -> dict:
    pass


def run_batch_classification(posts: list[dict]) -> dict:
    metadata = batch_classify_posts(posts=posts)
    return metadata
