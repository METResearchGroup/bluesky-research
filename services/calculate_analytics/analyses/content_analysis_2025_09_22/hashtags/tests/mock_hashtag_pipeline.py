"""Mock hashtag pipeline for testing purposes.

This module creates mock hashtag data and runs the hashtag analysis pipeline
to test the visualization and aggregation functionality.
"""

import pandas as pd
import random
from datetime import datetime
from services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.model import (
    analyze_hashtags_for_posts,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.transform import (
    aggregate_hashtags_by_condition_and_pre_post,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.visualize import (
    create_all_visualizations,
)


def create_mock_posts():
    """Create mock posts with hashtags for testing."""
    
    # Set random seed for reproducibility
    random.seed(42)
    
    # Create posts with different hashtag patterns
    posts = []
    
    # Political hashtags (more common)
    political_hashtags = [
        "#election2024", "#vote", "#democracy", "#politics", "#biden", "#trump",
        "#harris", "#republican", "#democrat", "#campaign", "#debate", "#polling"
    ]
    
    # Social hashtags (moderate frequency)
    social_hashtags = [
        "#community", "#together", "#change", "#hope", "#future", "#progress",
        "#unity", "#justice", "#equality", "#freedom", "#rights", "#voice"
    ]
    
    # News hashtags (less common)
    news_hashtags = [
        "#breaking", "#news", "#update", "#alert", "#report", "#analysis",
        "#coverage", "#media", "#journalism", "#factcheck", "#investigation"
    ]
    
    # Create 30 posts with different hashtag distributions
    for i in range(30):
        post_text = f"This is mock post {i+1}. "
        
        # Add hashtags based on post number
        if i < 10:  # First 10 posts: mostly political hashtags
            hashtags = random.sample(political_hashtags, random.randint(2, 4))
            hashtags.extend(random.sample(social_hashtags, random.randint(1, 2)))
        elif i < 20:  # Next 10 posts: mix of political and social
            hashtags = random.sample(political_hashtags, random.randint(1, 3))
            hashtags.extend(random.sample(social_hashtags, random.randint(1, 3)))
            hashtags.extend(random.sample(news_hashtags, random.randint(0, 1)))
        else:  # Last 10 posts: more social and news hashtags
            hashtags = random.sample(social_hashtags, random.randint(2, 4))
            hashtags.extend(random.sample(news_hashtags, random.randint(1, 2)))
            hashtags.extend(random.sample(political_hashtags, random.randint(0, 1)))
        
        # Add hashtags to post text
        post_text += " ".join(hashtags)
        
        posts.append({
            "uri": f"at://did:plc:mock{i+1}/app.bsky.feed.post/{i+1}",
            "text": post_text
        })
    
    return posts


def create_mock_user_data():
    """Create mock user data with conditions."""
    
    users = []
    conditions = ["reverse_chronological", "engagement", "representative_diversification"]
    
    # Create 6 users (2 per condition)
    for i in range(6):
        condition = conditions[i // 2]  # 2 users per condition
        users.append({
            "bluesky_user_did": f"did:plc:mockuser{i+1}",
            "condition": condition
        })
    
    return pd.DataFrame(users)


def create_mock_user_feeds():
    """Create mock user feeds with different post distributions."""
    
    posts = create_mock_posts()
    
    # Manually assign posts to pre/post election periods
    # Pre-election posts (more political hashtags)
    pre_election_posts = posts[:15]  # First 15 posts
    
    # Post-election posts (more social/news hashtags)  
    post_election_posts = posts[15:]  # Last 15 posts
    
    user_to_content_in_feeds = {}
    users = create_mock_user_data()
    
    for _, user in users.iterrows():
        user_did = user["bluesky_user_did"]
        user_to_content_in_feeds[user_did] = {
            "2024-10-01": set([post["uri"] for post in random.sample(pre_election_posts, 5)]),
            "2024-11-25": set([post["uri"] for post in random.sample(post_election_posts, 5)])
        }
    
    return user_to_content_in_feeds


def run_mock_hashtag_pipeline():
    """Run the complete hashtag analysis pipeline with mock data."""
    
    print("Creating mock data...")
    posts = create_mock_posts()
    user_df = create_mock_user_data()
    user_to_content_in_feeds = create_mock_user_feeds()
    
    # Create URI to text mapping
    uri_to_text = {post["uri"]: post["text"] for post in posts}
    
    print(f"Created {len(posts)} mock posts")
    print(f"Created {len(user_df)} mock users")
    print(f"Created feeds for {len(user_to_content_in_feeds)} users")
    
    # Run hashtag analysis
    print("\nRunning hashtag analysis...")
    uri_to_hashtags = analyze_hashtags_for_posts(uri_to_text)
    
    print(f"Extracted hashtags from {len(uri_to_hashtags)} posts")
    
    # Run aggregation
    print("\nRunning aggregation...")
    aggregated_data = aggregate_hashtags_by_condition_and_pre_post(
        uri_to_hashtags, user_df, user_to_content_in_feeds
    )
    
    print("Aggregation completed")
    
    # Create visualizations
    print("\nCreating visualizations...")
    output_dir = create_all_visualizations(aggregated_data)
    
    print(f"Visualizations created in: {output_dir}")
    
    # Print summary statistics
    print("\n=== SUMMARY STATISTICS ===")
    
    # Condition statistics
    condition_data = aggregated_data["condition"]
    for condition, top_hashtags_obj in condition_data.items():
        total_count = sum(top_hashtags_obj.get_all_hashtags().values())
        top_5 = top_hashtags_obj.get_top_n(5)
        print(f"{condition}: {total_count} total hashtags, top 5: {list(top_5.keys())}")
    
    # Election period statistics
    election_data = aggregated_data["election_date"]
    pre_total = sum(election_data["pre_election"].get_all_hashtags().values())
    post_total = sum(election_data["post_election"].get_all_hashtags().values())
    print(f"Pre-election: {pre_total} hashtags")
    print(f"Post-election: {post_total} hashtags")
    
    # Overall statistics
    overall_data = aggregated_data["overall"]
    overall_total = sum(overall_data["overall"].get_all_hashtags().values())
    top_10_overall = overall_data["overall"].get_top_n(10)
    print(f"Overall: {overall_total} hashtags, top 10: {list(top_10_overall.keys())}")
    
    print(f"\nMock hashtag pipeline completed successfully!")
    print(f"Check results in: {output_dir}")


if __name__ == "__main__":
    run_mock_hashtag_pipeline()
