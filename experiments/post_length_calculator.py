"""Example code for calculating average length of social media posts.

This module demonstrates how to work with social media post data
using pandas DataFrames and calculate basic metrics like average post length.
"""

import pandas as pd
from typing import List, Dict, Any


def create_mock_posts() -> pd.DataFrame:
    """Create a DataFrame with 10 mock social media posts.
    
    Returns:
        pd.DataFrame: DataFrame containing mock posts with 'text' and 'id' columns
    """
    mock_posts_data: List[Dict[str, Any]] = [
        {"id": 1, "text": "Just had the best coffee ever! ☕"},
        {"id": 2, "text": "Working on an exciting new project today. Can't wait to share more details soon!"},
        {"id": 3, "text": "Beautiful sunset tonight 🌅"},
        {"id": 4, "text": "Reading a fascinating book about distributed systems. Highly recommend it for any engineer!"},
        {"id": 5, "text": "Weekend vibes! 🎉"},
        {"id": 6, "text": "Just finished a great workout session. Feeling energized and ready for the day ahead."},
        {"id": 7, "text": "Technology keeps evolving at such an incredible pace. Always something new to learn."},
        {"id": 8, "text": "Hello world! 👋"},
        {"id": 9, "text": "Thinking about the future of social media and how AI will transform user experiences in the coming years."},
        {"id": 10, "text": "Quick update: the weather is perfect for a walk today!"}
    ]
    
    return pd.DataFrame(mock_posts_data)


def calculate_average_post_length(posts_df: pd.DataFrame) -> float:
    """Calculate the average length of posts in a DataFrame.
    
    Args:
        posts_df (pd.DataFrame): DataFrame containing posts with a 'text' column
        
    Returns:
        float: Average length of posts in characters
        
    Raises:
        ValueError: If DataFrame is empty or doesn't contain 'text' column
        KeyError: If 'text' column is missing from DataFrame
    """
    if posts_df.empty:
        raise ValueError("DataFrame cannot be empty")
    
    if 'text' not in posts_df.columns:
        raise KeyError("DataFrame must contain a 'text' column")
    
    # Calculate length of each post and return the mean
    post_lengths = posts_df['text'].str.len()
    average_length = post_lengths.mean()
    
    return float(average_length)


def main() -> None:
    """Main function demonstrating the post length calculation.
    
    Creates mock social media posts and calculates their average length.
    """
    # Create mock posts
    posts_df = create_mock_posts()
    
    print("Mock Social Media Posts:")
    print("=" * 50)
    for idx, row in posts_df.iterrows():
        print(f"Post {row['id']}: {row['text']}")
        print(f"Length: {len(row['text'])} characters")
        print("-" * 30)
    
    # Calculate average length
    avg_length = calculate_average_post_length(posts_df)
    
    print(f"\nTotal posts: {len(posts_df)}")
    print(f"Average post length: {avg_length:.2f} characters")


if __name__ == "__main__":
    main()