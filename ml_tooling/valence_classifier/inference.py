"""Run VADER sentiment analysis on a list of posts."""

import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ml_tooling.valence_classifier.constants import (
    VADER_POSITIVE_THRESHOLD,
    VADER_NEGATIVE_THRESHOLD,
)

analyzer = SentimentIntensityAnalyzer()


def run_vader_on_posts(posts: list[dict]) -> pd.DataFrame:
    """
    Run VADER sentiment analysis on a list of posts.

    Args:
        posts (list[dict]): List of post dicts, each with at least a 'text' field.

    Returns:
        pd.DataFrame: DataFrame with columns: 'uri', 'text', 'compound', 'valence_label'
    """
    results = []
    for post in posts:
        text = post.get("text", "")
        uri = post.get("uri", None)
        scores = analyzer.polarity_scores(text)
        compound = scores["compound"]
        if compound >= VADER_POSITIVE_THRESHOLD:
            label = "positive"
        elif compound <= VADER_NEGATIVE_THRESHOLD:
            label = "negative"
        else:
            label = "neutral"
        results.append(
            {
                "uri": uri,
                "text": text,
                "compound": compound,
                "valence_label": label,
            }
        )
    return pd.DataFrame(results)
