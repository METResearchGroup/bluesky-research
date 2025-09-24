"""Mock entity pipeline for testing NER analysis.

This module creates mock data similar to experiments.py to test the
do_ner_and_export_results function without requiring real data setup.
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Set

# Add the parent directory to the path to import the main module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import do_ner_and_export_results


def create_mock_posts() -> List[Dict[str, str]]:
    """Create mock posts with focused entity distribution:
    - 1/3 have Trump (10 posts)
    - 1/3 have Biden (10 posts) 
    - 1/6 have Trump AND Biden (5 posts)
    - 1/6 have neither (5 posts)
    """
    return [
        # Posts with Trump only (10 posts = 1/3)
        {"uri": "at://did:plc:abc123/post1", "text": "Donald Trump announces campaign rally in Arizona and Georgia counties."},
        {"uri": "at://did:plc:abc123/post2", "text": "Trump criticizes current administration infrastructure policy decisions."},
        {"uri": "at://did:plc:abc123/post3", "text": "The Supreme Court ruled on abortion rights while Trump was president."},
        {"uri": "at://did:plc:abc123/post4", "text": "Trump supporters in Massachusetts criticize corporate tax policies."},
        {"uri": "at://did:plc:abc123/post5", "text": "Governor DeSantis of Florida supports Trump education legislation."},
        {"uri": "at://did:plc:abc123/post6", "text": "The Federal Reserve under Trump announced interest rate changes."},
        {"uri": "at://did:plc:abc123/post7", "text": "Congressional Republicans debate healthcare reform with Trump."},
        {"uri": "at://did:plc:abc123/post8", "text": "Mayor Adams of New York City addresses Trump-era crime statistics."},
        {"uri": "at://did:plc:abc123/post9", "text": "Pentagon confirmed military operations in Afghanistan under Trump."},
        {"uri": "at://did:plc:abc123/post10", "text": "Secretary Blinken met with European Union leaders after Trump."},
        
        # Posts with Biden only (10 posts = 1/3)
        {"uri": "at://did:plc:abc123/post11", "text": "President Biden and Vice President Harris discuss infrastructure policy."},
        {"uri": "at://did:plc:abc123/post12", "text": "The White House under Biden administration addresses climate change policies."},
        {"uri": "at://did:plc:abc123/post13", "text": "Biden meets with European Union leaders in Brussels this week."},
        {"uri": "at://did:plc:abc123/post14", "text": "President Biden signs new education legislation in California today."},
        {"uri": "at://did:plc:abc123/post15", "text": "Biden administration announces immigration policy changes affecting Texas."},
        {"uri": "at://did:plc:abc123/post16", "text": "The Department of Justice under Biden investigates corporate violations."},
        {"uri": "at://did:plc:abc123/post17", "text": "Biden addresses crime statistics in New York City publicly."},
        {"uri": "at://did:plc:abc123/post18", "text": "Pentagon confirms military operations continue under Biden leadership."},
        {"uri": "at://did:plc:abc123/post19", "text": "Secretary Blinken works with Biden on foreign policy initiatives."},
        {"uri": "at://did:plc:abc123/post20", "text": "Biden administration opposes climate change legislation in Texas."},
        
        # Posts with Trump AND Biden (5 posts = 1/6)
        {"uri": "at://did:plc:abc123/post21", "text": "Trump and Biden debate healthcare reform policies in Texas."},
        {"uri": "at://did:plc:abc123/post22", "text": "The White House announces new policies comparing Trump and Biden administrations."},
        {"uri": "at://did:plc:abc123/post23", "text": "Biden signs environmental protection laws while Trump criticizes them."},
        {"uri": "at://did:plc:abc123/post24", "text": "Department of Justice investigates antitrust violations under both Trump and Biden."},
        {"uri": "at://did:plc:abc123/post25", "text": "Biden discusses urban development plans in Chicago while Trump opposes them."},
        
        # Posts with neither Trump nor Biden (5 posts = 1/6)
        {"uri": "at://did:plc:abc123/post26", "text": "Senator Sanders from Vermont proposed Medicare for All legislation."},
        {"uri": "at://did:plc:abc123/post27", "text": "The Department of Education announced new student loan forgiveness programs."},
        {"uri": "at://did:plc:abc123/post28", "text": "Governor Abbott of Texas signed controversial voting restrictions into law."},
        {"uri": "at://did:plc:abc123/post29", "text": "The Environmental Protection Agency released new climate regulations."},
        {"uri": "at://did:plc:abc123/post30", "text": "Senator McConnell from Kentucky blocked infrastructure bill negotiations."},
    ]


def create_mock_user_df() -> pd.DataFrame:
    """Create mock user DataFrame with conditions."""
    users_data = []
    conditions = ["reverse_chronological", "engagement", "representative_diversification"]
    
    # Create 6 users (2 per condition)
    for i in range(6):
        user_did = f"did:plc:mock_user_{i+1}"
        condition = conditions[i % 3]
        users_data.append({
            "bluesky_user_did": user_did,
            "condition": condition,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        })
    
    return pd.DataFrame(users_data)


def create_mock_user_to_content_in_feeds() -> Dict[str, Dict[str, Set[str]]]:
    """Create mock user feeds data with pre/post election dates.
    
    Manually assigns different posts to pre/post periods to create realistic
    entity distribution differences for testing rank change visualization.
    """
    posts = create_mock_posts()
    user_df = create_mock_user_df()
    
    # Manually assign posts to create realistic pre/post differences
    # Pre-election posts (more campaign-focused, early political content)
    pre_election_posts = [
        f"at://did:plc:abc123/post{i}" for i in range(1, 16)
    ]
    
    # Post-election posts (more post-election analysis, different focus)
    post_election_posts = [
        f"at://did:plc:abc123/post{i}" for i in range(16, 31)
    ]
    
    user_to_content_in_feeds = {}
    feed_dates = ["2024-10-01", "2024-11-25"]  # pre-election, post-election

    for _, user_row in user_df.iterrows():
        user_did = user_row["bluesky_user_did"]
        user_to_content_in_feeds[user_did] = {}
        
        # Assign different posts to pre/post periods
        import random
        random.seed(42 + hash(user_did) % 1000)  # Deterministic but varied per user
        
        # Pre-election: sample from pre_election_posts
        sampled_pre_posts = random.sample(pre_election_posts, min(5, len(pre_election_posts)))
        user_to_content_in_feeds[user_did][feed_dates[0]] = set(sampled_pre_posts)
        
        # Post-election: sample from post_election_posts  
        sampled_post_posts = random.sample(post_election_posts, min(5, len(post_election_posts)))
        user_to_content_in_feeds[user_did][feed_dates[1]] = set(sampled_post_posts)
    
    return user_to_content_in_feeds


def create_mock_uri_to_text() -> Dict[str, str]:
    """Create mock URI to text mapping."""
    posts = create_mock_posts()
    return {post["uri"]: post["text"] for post in posts}


def run_mock_entity_pipeline():
    """Run the mock entity pipeline to test do_ner_and_export_results."""
    print("=" * 80)
    print("MOCK ENTITY PIPELINE TEST")
    print("=" * 80)
    
    # Create mock data
    print("\n1. CREATING MOCK DATA")
    print("-" * 40)
    
    user_df = create_mock_user_df()
    print(f"Created {len(user_df)} mock users:")
    for _, row in user_df.iterrows():
        print(f"  {row['bluesky_user_did']}: {row['condition']}")
    
    user_to_content_in_feeds = create_mock_user_to_content_in_feeds()
    print(f"\nCreated feeds for {len(user_to_content_in_feeds)} users:")
    for user_did, feeds in user_to_content_in_feeds.items():
        print(f"  {user_did}:")
        for feed_date, post_uris in feeds.items():
            print(f"    {feed_date}: {len(post_uris)} posts")
    
    uri_to_text = create_mock_uri_to_text()
    print(f"\nCreated {len(uri_to_text)} mock posts")
    
    # Run the NER and export results
    print("\n2. RUNNING NER AND EXPORT RESULTS")
    print("-" * 40)
    
    try:
        uri_to_entities_map, aggregated_data = do_ner_and_export_results(
            uri_to_text=uri_to_text,
            user_df=user_df,
            user_to_content_in_feeds=user_to_content_in_feeds
        )
        
        print("✅ NER and export results completed successfully!")
        
        # Display some results
        print("\n3. RESULTS SUMMARY")
        print("-" * 40)
        
        total_entities = sum(len(entities) for entities in uri_to_entities_map.values())
        print(f"Total entities extracted: {total_entities}")
        print(f"Posts processed: {len(uri_to_entities_map)}")
        
        # Show condition analysis
        condition_data = aggregated_data["condition"]
        print(f"\nTop entities by condition:")
        for condition, top_entities_obj in condition_data.items():
            entities_dict = top_entities_obj.get_top_n(5)  # Show top 5
            print(f"\n{condition.upper()}:")
            for i, (entity, count) in enumerate(entities_dict.items(), 1):
                print(f"  {i}. {entity}: {count}")
        
        # Show election date analysis
        election_data = aggregated_data["election_date"]
        print(f"\nTop entities by election period:")
        
        pre_entities = election_data["pre_election"].get_top_n(5)
        print(f"\nPRE-ELECTION:")
        for i, (entity, count) in enumerate(pre_entities.items(), 1):
            print(f"  {i}. {entity}: {count}")
        
        post_entities = election_data["post_election"].get_top_n(5)
        print(f"\nPOST-ELECTION:")
        for i, (entity, count) in enumerate(post_entities.items(), 1):
            print(f"  {i}. {entity}: {count}")
        
        print("\n" + "=" * 80)
        print("MOCK ENTITY PIPELINE TEST COMPLETED SUCCESSFULLY")
        print("=" * 80)
        
        return uri_to_entities_map, aggregated_data
        
    except Exception as e:
        print(f"❌ Error running mock entity pipeline: {e}")
        import traceback
        traceback.print_exc()
        return None, None


if __name__ == "__main__":
    run_mock_entity_pipeline()
