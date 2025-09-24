"""
TF-IDF Demo Training Script

This script demonstrates TF-IDF analysis on simulated social media posts
with 3 sources (A, B, C) and 2 time periods (pre, post) for a total of 6 groups.

The script:
1. Generates fake social media posts using Faker
2. Assigns posts to sources and time periods
3. Trains TF-IDF models (both global and per-group)
4. Exports results for visualization

Usage:
    python tfidf_demo_train.py
"""

import json
import os
import pickle
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
from faker import Faker
from sklearn.feature_extraction.text import TfidfVectorizer

# Set random seeds for reproducibility
np.random.seed(42)
fake = Faker()
Faker.seed(42)

# Configuration
N_POSTS_PER_GROUP = 100  # Number of posts per source-period combination
OUTPUT_DIR = "demo_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define sources and time periods
SOURCES = ["A", "B", "C"]
TIME_PERIODS = ["pre", "post"]

# Note: Removed topic-specific constraints - posts will be generated with general content

# Note: Removed political topic constraints - posts will be generated with general content


def generate_fake_posts(n_posts: int, source: str, period: str) -> List[str]:
    """
    Generate fake social media posts with general content (no topic constraints).

    Args:
        n_posts: Number of posts to generate
        source: Source identifier (A, B, or C)
        period: Time period (pre or post)

    Returns:
        List of generated post texts
    """
    posts = []

    # Define general sentence templates (not topic-specific)
    general_templates = [
        "Today was an interesting day",
        "I've been thinking about this lately",
        "Something important happened recently",
        "I wanted to share my thoughts",
        "This has been on my mind",
        "I had a great experience today",
        "There's something I need to discuss",
        "I learned something new recently",
    ]

    # Define period-specific sentence templates
    period_templates = {
        "pre": [
            "Looking forward to what's coming",
            "The anticipation is building",
            "Preparations are underway",
            "Expectations are high",
            "Getting ready for the next phase",
        ],
        "post": [
            "The results are finally in",
            "Reflecting on what happened",
            "The outcome was surprising",
            "Analysis of the situation shows",
            "Lessons learned from this experience",
        ],
    }

    for _ in range(n_posts):
        # Select general content
        general_template = np.random.choice(general_templates)

        # Select period-specific content
        period_template = np.random.choice(period_templates[period])

        # Add some variety with random content
        additional_content = fake.sentence()

        # Sometimes add another random sentence for variety
        if np.random.random() < 0.3:
            extra_content = fake.sentence()
            post_parts = [
                general_template,
                period_template,
                additional_content,
                extra_content,
            ]
        else:
            post_parts = [general_template, period_template, additional_content]

        post = ". ".join(post_parts) + "."
        posts.append(post)

    return posts


def create_dataset() -> pd.DataFrame:
    """
    Create the complete dataset with posts from all source-period combinations.

    Returns:
        DataFrame with columns matching the expected schema from ticket-001.md
    """
    all_posts = []
    post_id = 0

    for source in SOURCES:
        for period in TIME_PERIODS:
            posts = generate_fake_posts(N_POSTS_PER_GROUP, source, period)

            for post in posts:
                # Generate fake user_id (simulating different users)
                user_id = f"user_{np.random.randint(1, 51)}"  # 50 different users

                all_posts.append(
                    {
                        "post_id": post_id,
                        "user_id": user_id,
                        "text": post,
                        "condition": source,  # Map source to condition (A=control, B=treatment, C=engagement)
                        "period": period,
                        "group": f"{source}_{period}",
                        "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
                    }
                )
                post_id += 1

    return pd.DataFrame(all_posts)


def train_global_tfidf(
    dataset: pd.DataFrame,
) -> Tuple[TfidfVectorizer, np.ndarray, List[str]]:
    """
    Train a global TF-IDF model on the entire corpus.

    Args:
        dataset: DataFrame with all posts

    Returns:
        Tuple of (vectorizer, tfidf_matrix, feature_names)
    """
    print("Training global TF-IDF model...")

    vectorizer = TfidfVectorizer(
        max_features=500,  # Smaller vocabulary to focus on key terms
        ngram_range=(1, 2),
        min_df=3,  # Higher threshold to filter out noise
        max_df=0.8,  # Lower threshold to remove very common terms
        stop_words="english",
    )

    tfidf_matrix = vectorizer.fit_transform(dataset["text"])
    feature_names = vectorizer.get_feature_names_out().tolist()

    print(f"Global TF-IDF trained with {len(feature_names)} features")
    return vectorizer, tfidf_matrix, feature_names


def train_group_tfidf(
    dataset: pd.DataFrame,
) -> Dict[str, Tuple[TfidfVectorizer, np.ndarray, List[str]]]:
    """
    Train separate TF-IDF models for each group.

    Args:
        dataset: DataFrame with all posts

    Returns:
        Dictionary mapping group names to (vectorizer, tfidf_matrix, feature_names)
    """
    print("Training group-specific TF-IDF models...")

    group_models = {}

    for group in dataset["group"].unique():
        group_data = dataset[dataset["group"] == group]

        vectorizer = TfidfVectorizer(
            max_features=200,  # Smaller vocabulary per group
            ngram_range=(1, 2),
            min_df=2,  # Still filter out very rare terms
            max_df=0.8,
            stop_words="english",
        )

        tfidf_matrix = vectorizer.fit_transform(group_data["text"])
        feature_names = vectorizer.get_feature_names_out().tolist()

        group_models[group] = (vectorizer, tfidf_matrix, feature_names)
        print(f"Group {group}: {len(feature_names)} features")

    return group_models


def get_top_keywords(
    tfidf_matrix: np.ndarray, feature_names: List[str], top_n: int = 20
) -> pd.DataFrame:
    """
    Extract top keywords by TF-IDF score.

    Args:
        tfidf_matrix: TF-IDF matrix
        feature_names: List of feature names
        top_n: Number of top keywords to return

    Returns:
        DataFrame with top keywords and scores
    """
    # Calculate mean TF-IDF scores across all documents
    mean_scores = np.mean(tfidf_matrix.toarray(), axis=0)

    # Get top N keywords
    top_indices = np.argsort(mean_scores)[-top_n:][::-1]

    results = []
    for idx in top_indices:
        results.append({"term": feature_names[idx], "score": mean_scores[idx]})

    return pd.DataFrame(results)


def analyze_stratified(
    dataset: pd.DataFrame, global_vectorizer: TfidfVectorizer
) -> Dict[str, pd.DataFrame]:
    """
    Perform stratified analysis using the global vectorizer.

    Args:
        dataset: DataFrame with all posts
        global_vectorizer: Trained global TF-IDF vectorizer

    Returns:
        Dictionary with stratified results
    """
    print("Performing stratified analysis...")

    results = {}

    # Overall analysis
    tfidf_matrix = global_vectorizer.transform(dataset["text"])
    results["overall"] = get_top_keywords(
        tfidf_matrix, global_vectorizer.get_feature_names_out().tolist()
    )

    # Analysis by condition (mapped from source)
    condition_mapping = {"A": "control", "B": "treatment", "C": "engagement"}
    for source, condition in condition_mapping.items():
        condition_data = dataset[dataset["condition"] == source]
        tfidf_matrix = global_vectorizer.transform(condition_data["text"])
        results[f"condition_{condition}"] = get_top_keywords(
            tfidf_matrix, global_vectorizer.get_feature_names_out().tolist()
        )

    # Analysis by period
    for period in TIME_PERIODS:
        period_data = dataset[dataset["period"] == period]
        tfidf_matrix = global_vectorizer.transform(period_data["text"])
        results[f"period_{period}"] = get_top_keywords(
            tfidf_matrix, global_vectorizer.get_feature_names_out().tolist()
        )

    # Analysis by group (condition + period combination)
    for group in dataset["group"].unique():
        group_data = dataset[dataset["group"] == group]
        tfidf_matrix = global_vectorizer.transform(group_data["text"])
        results[f"group_{group}"] = get_top_keywords(
            tfidf_matrix, global_vectorizer.get_feature_names_out().tolist()
        )

    return results


def create_detailed_tfidf_results(
    dataset: pd.DataFrame, global_vectorizer: TfidfVectorizer
) -> pd.DataFrame:
    """
    Create detailed TF-IDF results matching the ticket schema.

    Args:
        dataset: Original dataset with posts
        global_vectorizer: Trained global TF-IDF vectorizer

    Returns:
        DataFrame with schema: post_id, user_id, topic, tfidf_vector, top_terms, timestamp, condition, period
    """
    print("Creating detailed TF-IDF results...")

    # Transform all texts to TF-IDF vectors
    tfidf_matrix = global_vectorizer.transform(dataset["text"])
    feature_names = global_vectorizer.get_feature_names_out().tolist()

    # Create results DataFrame
    results = dataset[["post_id", "user_id", "condition", "period", "timestamp"]].copy()

    # Add TF-IDF vector as string representation
    tfidf_vectors = []
    top_terms_list = []

    for i in range(len(dataset)):
        # Get TF-IDF scores for this document
        doc_scores = tfidf_matrix[i].toarray().flatten()

        # Create vector representation (top 10 non-zero scores)
        non_zero_indices = np.where(doc_scores > 0)[0]
        if len(non_zero_indices) > 0:
            # Sort by score and take top 10
            top_indices = non_zero_indices[
                np.argsort(doc_scores[non_zero_indices])[-10:][::-1]
            ]
            vector_parts = [
                f"{feature_names[idx]}:{doc_scores[idx]:.4f}" for idx in top_indices
            ]
            tfidf_vectors.append("|".join(vector_parts))

            # Create top terms string
            top_terms = [feature_names[idx] for idx in top_indices[:5]]  # Top 5 terms
            top_terms_list.append(", ".join(top_terms))
        else:
            tfidf_vectors.append("")
            top_terms_list.append("")

    results["tfidf_vector"] = tfidf_vectors
    results["top_terms"] = top_terms_list

    return results


def save_results(
    dataset: pd.DataFrame,
    global_vectorizer: TfidfVectorizer,
    group_models: Dict,
    stratified_results: Dict,
) -> None:
    """
    Save all results to files.

    Args:
        dataset: Original dataset
        global_vectorizer: Global TF-IDF vectorizer
        group_models: Group-specific models
        stratified_results: Stratified analysis results
    """
    print("Saving results...")

    # Create timestamped directory structure as per ticket requirements
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    training_dir = os.path.join(OUTPUT_DIR, "training", timestamp)
    os.makedirs(training_dir, exist_ok=True)

    # Create subdirectories for stratified results
    overall_dir = os.path.join(training_dir, "overall")
    condition_dir = os.path.join(training_dir, "condition")
    period_dir = os.path.join(training_dir, "period")

    os.makedirs(overall_dir, exist_ok=True)
    os.makedirs(condition_dir, exist_ok=True)
    os.makedirs(period_dir, exist_ok=True)

    # Save original dataset
    dataset.to_csv(os.path.join(training_dir, "dataset.csv"), index=False)

    # Create and save detailed TF-IDF results (matching ticket schema)
    detailed_results = create_detailed_tfidf_results(dataset, global_vectorizer)
    detailed_results.to_csv(
        os.path.join(training_dir, "tfidf_analysis_results.csv"), index=False
    )

    # Save global model
    with open(os.path.join(training_dir, "vectorizer.pkl"), "wb") as f:
        pickle.dump(global_vectorizer, f)

    # Save feature names
    feature_names = global_vectorizer.get_feature_names_out().tolist()
    with open(os.path.join(training_dir, "feature_names.json"), "w") as f:
        json.dump(feature_names, f, indent=2)

    # Save group models
    with open(os.path.join(training_dir, "group_models.pkl"), "wb") as f:
        pickle.dump(group_models, f)

    # Save stratified results in organized directories
    for analysis_type, results_df in stratified_results.items():
        if analysis_type == "overall":
            results_df.to_csv(
                os.path.join(overall_dir, "top_keywords_overall.csv"), index=False
            )
        elif analysis_type.startswith("condition_"):
            condition_name = analysis_type.replace("condition_", "")
            results_df.to_csv(
                os.path.join(condition_dir, f"top_keywords_{condition_name}.csv"),
                index=False,
            )
        elif analysis_type.startswith("period_"):
            period_name = analysis_type.replace("period_", "")
            results_df.to_csv(
                os.path.join(period_dir, f"top_keywords_{period_name}.csv"), index=False
            )
        else:
            # Save other analysis types in main directory
            results_df.to_csv(
                os.path.join(training_dir, f"{analysis_type}_keywords.csv"), index=False
            )

    # Save comprehensive metadata
    metadata = {
        "run_timestamp": timestamp,
        "mode": "demo",
        "total_documents_analyzed": len(dataset),
        "tfidf_model_params": {
            "max_features": 500,
            "ngram_range": [1, 2],
            "min_df": 3,
            "max_df": 0.8,
            "stop_words": "english",
        },
        "vectorizer_path": os.path.join(training_dir, "vectorizer.pkl"),
        "feature_names_path": os.path.join(training_dir, "feature_names.json"),
        "study_start_date": "2024-01-01",  # Demo dates
        "study_end_date": "2024-12-31",
        "election_date": "2024-11-05",
        "output_schema": "post_id, user_id, tfidf_vector, top_terms, timestamp_YYYY-MM-DD_HH:MM:SS, condition, period",
        "n_posts_per_group": N_POSTS_PER_GROUP,
        "sources": SOURCES,
        "time_periods": TIME_PERIODS,
        "global_features": len(global_vectorizer.get_feature_names_out()),
        "group_features": {
            group: len(features) for group, (_, _, features) in group_models.items()
        },
        "analysis_types": list(stratified_results.keys()),
    }

    with open(os.path.join(training_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Results saved to {training_dir}")


def main():
    """Main execution function."""
    print("=== TF-IDF Demo Training ===")
    print(
        f"Generating {N_POSTS_PER_GROUP} posts per group ({len(SOURCES)} sources × {len(TIME_PERIODS)} periods)"
    )

    # Create dataset
    dataset = create_dataset()
    print(f"Created dataset with {len(dataset)} posts")
    print(f"Group distribution:\n{dataset['group'].value_counts()}")

    # Train global TF-IDF
    global_vectorizer, global_tfidf_matrix, global_features = train_global_tfidf(
        dataset
    )

    # Train group-specific TF-IDF models
    group_models = train_group_tfidf(dataset)

    # Perform stratified analysis
    stratified_results = analyze_stratified(dataset, global_vectorizer)

    # Save results
    save_results(dataset, global_vectorizer, group_models, stratified_results)

    print("\n=== Training Complete ===")
    print("Next steps:")
    print("1. Run: python tfidf_demo_visualize.py")
    print("2. Check the generated visualizations in demo_results/")

    # Print sample results
    print("\n=== Sample Results ===")
    print("Top 10 keywords overall:")
    print(stratified_results["overall"].head(10).to_string(index=False))

    print("\nTop 5 keywords by condition:")
    condition_mapping = {"A": "control", "B": "treatment", "C": "engagement"}
    for source, condition in condition_mapping.items():
        print(f"\nCondition {condition} (Source {source}):")
        print(
            stratified_results[f"condition_{condition}"].head(5).to_string(index=False)
        )

    print("\nTop 5 keywords by period:")
    for period in TIME_PERIODS:
        print(f"\nPeriod {period}:")
        print(stratified_results[f"period_{period}"].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
