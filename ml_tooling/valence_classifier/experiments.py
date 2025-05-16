"""
Experiments with the VADER sentiment analysis tool for valence classification.

This module demonstrates how to use the VADER (Valence Aware Dictionary and sEntiment Reasoner)
sentiment analysis tool to classify text posts based on their sentiment polarity.

As per the Github page: https://github.com/cjhutto/vaderSentiment?tab=readme-ov-file#about-the-scoring
positive sentiment: compound score >= 0.05
neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
negative sentiment: compound score <= -0.05
"""

from typing import List, Dict, Any
import random
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def generate_dummy_posts(num_posts: int = 10) -> List[Dict[str, Any]]:
    """
    Generate a list of dummy posts for sentiment analysis testing.
    
    Args:
        num_posts: Number of dummy posts to generate
        
    Returns:
        List of post dictionaries with text and uri fields
    """
    example_texts = [
        "I love this new feature! It's amazing and works perfectly.",
        "This is the worst update ever. Everything is broken now.",
        "I'm feeling neutral about these changes. Some good, some bad.",
        "The developers really outdid themselves this time! Fantastic work!",
        "Disappointed with the lack of progress on fixing bugs.",
        "Not sure how I feel about this yet. Need more time to evaluate.",
        "This makes me so angry! Why would they remove that feature?",
        "Grateful for all the hard work the team has put into this project.",
        "Meh, it's okay I guess. Nothing special.",
        "Absolutely hate the new interface. So confusing and unintuitive."
    ]
    
    posts = []
    for i in range(num_posts):
        text = random.choice(example_texts)
        posts.append({
            "text": text,
            "uri": f"dummy_post_{i}",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "batch_id": "dummy_batch"
        })
    
    return posts

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

def run_vader_experiment():
    """
    Run a complete experiment with VADER sentiment analysis on dummy posts.
    
    This function:
    1. Generates dummy posts
    2. Classifies them with VADER
    3. Prints the results
    """
    print("Running VADER sentiment analysis experiment...")
    
    # Generate dummy posts
    posts = generate_dummy_posts(5)
    
    # Classify posts
    classified_posts = classify_posts_with_vader(posts)
    
    # Print results
    print("\nVADER Sentiment Analysis Results:")
    print("-" * 50)
    
    for post in classified_posts:
        print(f"Post: {post['text']}")
        print(f"Sentiment: {post['sentiment_label']}")
        print(f"Scores: {post['vader_scores']}")
        print("-" * 50)
    
    # Summarize results
    sentiment_counts = {
        "positive": sum(1 for post in classified_posts if post["sentiment_label"] == "positive"),
        "neutral": sum(1 for post in classified_posts if post["sentiment_label"] == "neutral"),
        "negative": sum(1 for post in classified_posts if post["sentiment_label"] == "negative")
    }
    
    print("\nSummary:")
    print(f"Total posts: {len(classified_posts)}")
    print(f"Positive: {sentiment_counts['positive']}")
    print(f"Neutral: {sentiment_counts['neutral']}")
    print(f"Negative: {sentiment_counts['negative']}")

if __name__ == "__main__":
    run_vader_experiment()
