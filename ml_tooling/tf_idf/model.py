"""
TF-IDF model implementation for political content analysis.

This module implements the core TF-IDF analysis functionality, including
vectorization, stratified analysis, and keyword extraction for political content.
"""

import numpy as np
import yaml
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer


def load_config():
    """Load TF-IDF configuration from config.yaml file."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


posts = [
    "Election officials report high turnout in Arizona.",
    "Turnout was lower in some counties but results are still coming in.",
    "Officials in Georgia discuss ballot counting and certification.",
    "Media outlets report results and projections after the election.",
    "County offices share updates on vote tabulation.",
    "Campaign rally highlights policy and jobs.",
    "Debate clips trend as candidates spar over the economy.",
    "Certification timelines differ across states and counties.",
    "Security and administration issues raised by observers.",
    "Local reporters cover results and recount discussions.",
]


def fit_model(posts_list):
    """
    Fit a global TF-IDF model on the provided posts.

    Args:
        posts_list: List of text posts to analyze

    Returns:
        tuple: (vectorizer, X)
            - vectorizer: Fitted TfidfVectorizer
            - X: TF-IDF matrix (sparse [n_docs x n_terms])
    """
    # Load configuration
    config = load_config()

    # Initialize vectorizer with configuration parameters
    vectorizer = TfidfVectorizer(
        lowercase=config["lowercase"],
        stop_words=config["stop_words"],
        norm=config["norm"],
        max_features=config["max_features"],
        ngram_range=tuple(config["ngram_range"]),
        min_df=config["min_df"],
        max_df=config["max_df"],
    )

    # Fit and transform the posts
    X = vectorizer.fit_transform(posts_list)  # X is a sparse [n_docs x n_terms] matrix

    return vectorizer, X


def fit_model_and_return_top_terms(posts_list, top_n=None):
    """
    Fit a global TF-IDF model and calculate mean TF-IDF scores.

    Args:
        posts_list: List of text posts to analyze
        top_n: Number of top terms to return (if None, uses config value)

    Returns:
        tuple: (vectorizer, X, top_terms_with_scores)
            - vectorizer: Fitted TfidfVectorizer
            - X: TF-IDF matrix (sparse [n_docs x n_terms])
            - top_terms_with_scores: List of tuples (term, mean_tfidf_score)
    """
    # Load configuration
    config = load_config()

    # Use config top_n if not provided
    if top_n is None:
        top_n = config["top_n_terms"]

    # Fit the model
    vectorizer, X = fit_model(posts_list)

    # Get vocabulary terms
    terms = vectorizer.get_feature_names_out()

    # Calculate mean TF-IDF scores across the corpus
    # This implements: Mean TFIDF = Document-level TF-IDF * (Number of posts that mention the term / Total number of posts)
    mean_tfidf_scores = np.asarray(X.mean(axis=0)).ravel()

    # Rank terms by their mean TF-IDF scores
    top_indices = mean_tfidf_scores.argsort()[::-1][:top_n]

    # Create list of top terms with their scores
    top_terms_with_scores = [
        (terms[i], float(mean_tfidf_scores[i])) for i in top_indices
    ]

    return vectorizer, X, top_terms_with_scores


def main():
    """
    Demonstrate the TF-IDF model functionality with dummy data.
    """
    print("TF-IDF Model for Political Content Analysis")
    print("=" * 50)

    # Load and display configuration
    config = load_config()
    print("\nConfiguration:")
    print(f"  min_df: {config['min_df']} (drop terms in < {config['min_df']} posts)")
    print(
        f"  max_df: {config['max_df']} (drop terms in >= {config['max_df']*100:.0f}% of posts)"
    )
    print(f"  max_features: {config['max_features']}")
    print(f"  ngram_range: {config['ngram_range']}")
    print(f"  norm: {config['norm']}")
    print(f"  stop_words: {config['stop_words']}")

    # Upsample the demo posts 100 times to meet min_df requirements
    upsample_factor = 100
    demo_posts = posts * upsample_factor
    print("\nDemo dataset:")
    print(f"  Original posts: {len(posts)}")
    print(f"  Upsampled posts: {len(demo_posts)} (x{upsample_factor})")

    # Fit the model on upsampled demo posts and get top terms
    vectorizer, X, top_terms = fit_model_and_return_top_terms(demo_posts)

    print(f"\nMatrix shape: {X.shape}")
    print(f"Total vocabulary size: {len(vectorizer.get_feature_names_out())}")

    print(f"\nTop {len(top_terms)} terms by mean TF-IDF score:")
    print("-" * 40)
    for i, (term, score) in enumerate(top_terms, 1):
        print(f"{i:2d}. {term:15s} {score:.4f}")

    # Demonstrate basic fit_model function (just fitting, no top terms)
    print("\nDemonstrating basic fit_model function:")
    print("-" * 40)
    vectorizer_basic, X_basic = fit_model(
        demo_posts
    )  # Use full dataset to meet min_df requirements
    print(f"Basic fit result: vectorizer + matrix shape {X_basic.shape}")

    # Demonstrate transforming new posts
    print("\nTransforming new posts:")
    print("-" * 20)
    new_posts = [
        "Officials announce certification results for Georgia counties.",
        "Campaign volunteers organize voter registration drives.",
    ]

    X_new = vectorizer.transform(new_posts)
    print(f"New posts matrix shape: {X_new.shape}")

    # Show non-zero features for first new post
    print("\nNon-zero features in first new post:")
    nz = X_new[0].nonzero()
    for col, val in zip(nz[1], X_new[0].data):
        term = vectorizer.get_feature_names_out()[col]
        print(f"  {term:15s} {val:.4f}")

    return vectorizer, X, top_terms


if __name__ == "__main__":
    main()
