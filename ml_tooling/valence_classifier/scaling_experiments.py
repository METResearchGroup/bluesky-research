"""
Scaling experiments with the VADER sentiment analysis tool for valence classification.

This module extends the experiments.py functionality to handle a larger volume of posts
(50,000) and provides statistical summaries of the sentiment scores rather than
printing individual results.

Metrics from experiments:
- Total posts: 50000, Total runtime: 5.84 seconds
- Total posts: 100000, Total runtime: 11.80 seconds
- Total posts: 200000, Total runtime: 22.91 seconds
- Total posts: 400000, Total runtime: 45.91 seconds
- Total posts: 800000, Total runtime: 91.10 seconds
- Total posts: 1600000, Total runtime: 190.51 seconds"""

import time
from typing import List, Dict, Any, Tuple
import random
import numpy as np
from faker import Faker
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def generate_large_dummy_dataset(num_posts: int = 50000) -> List[Dict[str, Any]]:
    """
    Generate a large dataset of dummy posts for sentiment analysis testing.
    
    Args:
        num_posts: Number of dummy posts to generate (default: 50000)
        
    Returns:
        List of post dictionaries with text and uri fields
    """
    fake = Faker()
    
    posts = []
    for i in range(num_posts):
        # Generate random text with Faker
        text = fake.paragraph(nb_sentences=random.randint(1, 5))
        
        posts.append({
            "text": text,
            "uri": f"dummy_post_{i}",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "batch_id": "large_scale_batch"
        })
    
    return posts

def calculate_five_number_summary(values: List[float]) -> Dict[str, float]:
    """
    Calculate the five-number summary (min, Q1, median, Q3, max) for a list of values.
    
    Args:
        values: List of numerical values
        
    Returns:
        Dictionary containing the five-number summary
    """
    return {
        "min": np.min(values),
        "q1": np.percentile(values, 25),
        "median": np.median(values),
        "q3": np.percentile(values, 75),
        "max": np.max(values)
    }

def classify_posts_with_vader(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Classify posts using VADER sentiment analysis.
    
    Args:
        posts: List of post dictionaries with text content
        
    Returns:
        List of posts with added sentiment scores and labels
    """
    analyzer = SentimentIntensityAnalyzer()
    
    for post in posts:
        # Get sentiment scores
        sentiment_scores = analyzer.polarity_scores(post["text"])
        
        # Add scores to post
        post["vader_scores"] = sentiment_scores
        
        # Add sentiment label based on compound score
        if sentiment_scores["compound"] >= 0.05:
            post["sentiment_label"] = "positive"
        elif sentiment_scores["compound"] <= -0.05:
            post["sentiment_label"] = "negative"
        else:
            post["sentiment_label"] = "neutral"
    
    return posts

def summarize_sentiment_results(classified_posts: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, Dict[str, float]]]:
    """
    Summarize the sentiment analysis results.
    
    Args:
        classified_posts: List of posts with sentiment scores and labels
        
    Returns:
        Tuple containing:
        - Dictionary with counts for each sentiment label
        - Dictionary with five-number summaries for each score type
    """
    # Count sentiment labels
    sentiment_counts = {
        "positive": sum(1 for post in classified_posts if post["sentiment_label"] == "positive"),
        "neutral": sum(1 for post in classified_posts if post["sentiment_label"] == "neutral"),
        "negative": sum(1 for post in classified_posts if post["sentiment_label"] == "negative")
    }
    
    # Extract scores for summary statistics
    compound_scores = [post["vader_scores"]["compound"] for post in classified_posts]
    pos_scores = [post["vader_scores"]["pos"] for post in classified_posts]
    neu_scores = [post["vader_scores"]["neu"] for post in classified_posts]
    neg_scores = [post["vader_scores"]["neg"] for post in classified_posts]
    
    # Calculate five-number summaries
    score_summaries = {
        "compound": calculate_five_number_summary(compound_scores),
        "positive": calculate_five_number_summary(pos_scores),
        "neutral": calculate_five_number_summary(neu_scores),
        "negative": calculate_five_number_summary(neg_scores)
    }
    
    return sentiment_counts, score_summaries

def run_large_scale_vader_experiment(num_posts: int = 50000):
    """
    Run a large-scale experiment with VADER sentiment analysis.
    
    This function:
    1. Generates a large number of dummy posts
    2. Classifies them with VADER
    3. Provides statistical summaries of the results
    
    Args:
        num_posts: Number of posts to generate and analyze (default: 50000)
    """
    print(f"Running large-scale VADER sentiment analysis experiment with {num_posts} posts...")
    
    # Track execution time
    start_time = time.time()
    
    # Generate dummy posts
    print(f"Generating {num_posts} dummy posts...")
    posts = generate_large_dummy_dataset(num_posts)
    
    generation_time = time.time() - start_time
    print(f"Post generation completed in {generation_time:.2f} seconds")
    
    # Classify posts
    print("Classifying posts with VADER...")
    classification_start = time.time()
    classified_posts = classify_posts_with_vader(posts)
    classification_time = time.time() - classification_start
    print(f"Classification completed in {classification_time:.2f} seconds")
    
    # Summarize results
    sentiment_counts, score_summaries = summarize_sentiment_results(classified_posts)
    
    # Print summary results
    print("\nSentiment Distribution:")
    print("-" * 50)
    print(f"Total posts: {len(classified_posts)}")
    print(f"Positive: {sentiment_counts['positive']} ({sentiment_counts['positive']/len(classified_posts)*100:.2f}%)")
    print(f"Neutral: {sentiment_counts['neutral']} ({sentiment_counts['neutral']/len(classified_posts)*100:.2f}%)")
    print(f"Negative: {sentiment_counts['negative']} ({sentiment_counts['negative']/len(classified_posts)*100:.2f}%)")

    print("\nFive-Number Summaries:")
    print("-" * 50)
    for score_type, summary in score_summaries.items():
        print(f"{score_type.capitalize()} scores:")
        print(f"  Min: {summary['min']:.4f}")
        print(f"  Q1: {summary['q1']:.4f}")
        print(f"  Median: {summary['median']:.4f}")
        print(f"  Q3: {summary['q3']:.4f}")
        print(f"  Max: {summary['max']:.4f}")
        print()
    
    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds")
    return total_time

if __name__ == "__main__":
    total_posts = [50_000, 100_000, 200_000, 400_000, 800_000, 1_600_000]
    total_times = []
    for num_posts in total_posts:
        total_time = run_large_scale_vader_experiment(num_posts)
        total_times.append(total_time)

    for post_count, runtime in zip(total_posts, total_times):
        print(f"Total posts: {post_count}, Total runtime: {runtime:.2f} seconds")
