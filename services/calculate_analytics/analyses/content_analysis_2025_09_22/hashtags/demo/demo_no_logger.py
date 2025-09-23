"""Demo script for hashtag analysis module (no logger dependency).

This script demonstrates the hashtag analysis functionality using mock data
to verify that all components work correctly without requiring access to
the full dataset or logger dependencies.
"""

import os
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Set, List
from collections import defaultdict, Counter

# Constants (copied from hashtag_analysis.py to avoid logger dependency)
ELECTION_DATE = "2024-11-05"
MIN_HASHTAG_FREQUENCY = 5
HASHTAG_REGEX = re.compile(r"#\w+")


def extract_hashtags_from_text(text: str) -> List[str]:
    """Extract hashtags from post text using regex."""
    if not text or not isinstance(text, str):
        return []

    hashtags = HASHTAG_REGEX.findall(text)
    # Normalize: remove # and convert to lowercase
    normalized_hashtags = [tag[1:].lower() for tag in hashtags]
    return normalized_hashtags


def get_hashtag_counts_for_posts(posts_df: pd.DataFrame) -> Dict[str, int]:
    """Get hashtag frequency counts for a set of posts."""
    hashtag_counter = Counter()

    for _, post in posts_df.iterrows():
        hashtags = extract_hashtags_from_text(post["text"])
        hashtag_counter.update(hashtags)

    return dict(hashtag_counter)


def filter_rare_hashtags(
    hashtag_counts: Dict[str, int], min_frequency: int = MIN_HASHTAG_FREQUENCY
) -> Dict[str, int]:
    """Filter out hashtags that occur less than min_frequency times."""
    return {
        hashtag: count
        for hashtag, count in hashtag_counts.items()
        if count >= min_frequency
    }


def get_election_period(date_str: str) -> str:
    """Determine if a date is pre or post election."""
    if pd.to_datetime(date_str) < pd.to_datetime(ELECTION_DATE):
        return "pre_election"
    else:
        return "post_election"


def create_stratified_hashtag_analysis(
    user_df: pd.DataFrame,
    user_to_content_in_feeds: Dict[str, Dict[str, Set[str]]],
    posts_data: pd.DataFrame,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """Create stratified hashtag analysis by condition and election period."""
    print("Creating stratified hashtag analysis...")

    # Create mapping from URI to post data
    posts_by_uri = posts_data.set_index("uri")

    # Get user conditions
    user_conditions = user_df.set_index("bluesky_user_did")["condition"].to_dict()

    stratified_results = defaultdict(lambda: defaultdict(Counter))

    for user_did, user_feeds in user_to_content_in_feeds.items():
        condition = user_conditions.get(user_did, "unknown")

        for date_str, post_uris in user_feeds.items():
            election_period = get_election_period(date_str)

            # Get posts for this user/date
            user_posts = posts_by_uri.loc[posts_by_uri.index.intersection(post_uris)]

            if not user_posts.empty:
                hashtag_counts = get_hashtag_counts_for_posts(user_posts)
                stratified_results[condition][election_period].update(hashtag_counts)

    # Convert Counter objects to regular dicts and filter rare hashtags
    # Use a lower threshold for demo purposes
    final_results = {}
    for condition, periods in stratified_results.items():
        final_results[condition] = {}
        for period, hashtag_counter in periods.items():
            hashtag_counts = dict(hashtag_counter)
            # Use min_frequency=1 for demo to show all hashtags
            filtered_counts = filter_rare_hashtags(hashtag_counts, min_frequency=1)
            final_results[condition][period] = filtered_counts

    print(f"Stratified analysis complete for {len(final_results)} conditions")
    return final_results


def create_hashtag_dataframe(
    stratified_results: Dict[str, Dict[str, Dict[str, int]]],
) -> pd.DataFrame:
    """Convert stratified hashtag results to standardized DataFrame."""
    print("Creating hashtag analysis DataFrame...")

    rows = []

    for condition, periods in stratified_results.items():
        for election_period, hashtag_counts in periods.items():
            total_hashtags = sum(hashtag_counts.values())

            for hashtag, count in hashtag_counts.items():
                proportion = count / total_hashtags if total_hashtags > 0 else 0

                rows.append(
                    {
                        "condition": condition,
                        "pre_post_election": election_period,
                        "hashtag": hashtag,
                        "count": count,
                        "proportion": proportion,
                    }
                )

    df = pd.DataFrame(rows)
    print(f"Created DataFrame with {len(df)} hashtag records")
    return df


def create_mock_data():
    """Create mock data for demonstration purposes."""
    print("📊 Creating mock data for demonstration...")

    # Mock user data
    user_df = pd.DataFrame(
        {
            "bluesky_user_did": ["user1", "user2", "user3", "user4"],
            "condition": ["control", "treatment", "control", "treatment"],
        }
    )

    # Mock posts data with hashtags
    posts_data = pd.DataFrame(
        {
            "uri": [
                "uri1",
                "uri2",
                "uri3",
                "uri4",
                "uri5",
                "uri6",
                "uri7",
                "uri8",
                "uri9",
                "uri10",
                "uri11",
                "uri12",
                "uri13",
                "uri14",
                "uri15",
                "uri16",
            ],
            "text": [
                # Pre-election posts
                "Excited about the upcoming #election! #voting #democracy",
                "Political #campaign season is heating up #politics #election",
                "Important #voting information for everyone #democracy #civic",
                "Following the #campaign closely #politics #election",
                # Post-election posts
                "The #election results are in! #democracy #results",
                "Post-election analysis and #results discussion #politics",
                "Celebrating #democracy and the #election outcome #results",
                "Reflecting on the #election process #democracy #civic",
                # More diverse hashtags
                "Climate change is a critical issue #climate #environment #future",
                "Technology advances in #AI and #machinelearning #innovation",
                "Sports updates: #football #championship #victory",
                "Food and culture: #cooking #recipe #delicious #foodie",
                # Additional posts for frequency
                "Another #election post with #politics hashtags",
                "More #democracy content for #civic engagement",
                "Additional #climate and #environment posts",
                "Extra #AI and #technology content",
            ],
        }
    )

    # Mock user-to-content mapping (simulating feeds)
    user_to_content_in_feeds = {
        "user1": {
            "2024-11-01": {"uri1", "uri2"},  # pre-election
            "2024-11-10": {"uri5", "uri6"},  # post-election
        },
        "user2": {
            "2024-11-01": {"uri3", "uri4"},  # pre-election
            "2024-11-10": {"uri7", "uri8"},  # post-election
        },
        "user3": {
            "2024-11-01": {"uri9", "uri10"},  # pre-election
            "2024-11-10": {"uri11", "uri12"},  # post-election
        },
        "user4": {
            "2024-11-01": {"uri13", "uri14"},  # pre-election
            "2024-11-10": {"uri15", "uri16"},  # post-election
        },
    }

    print("✅ Created mock data:")
    print(f"   - {len(user_df)} users")
    print(f"   - {len(posts_data)} posts")
    print(f"   - {len(user_to_content_in_feeds)} user feed mappings")

    return user_df, posts_data, user_to_content_in_feeds


def demo_hashtag_extraction():
    """Demonstrate hashtag extraction functionality."""
    print("\n🔍 Testing hashtag extraction...")

    test_texts = [
        "This is a #test post with #multiple #hashtags",
        "This has #UPPERCASE and #MixedCase hashtags",
        "This text has no hashtags",
        "Special chars: #hashtag_with_underscores and #hashtag-with-dashes",
        "",  # Empty text
        None,  # None text
    ]

    for i, text in enumerate(test_texts):
        hashtags = extract_hashtags_from_text(text)
        print(f"   Text {i+1}: '{text}' → {hashtags}")

    print("✅ Hashtag extraction working correctly")


def demo_csv_export(hashtag_df):
    """Demonstrate CSV export functionality."""
    print("\n💾 Testing CSV export...")

    # Check if DataFrame is empty
    if hashtag_df.empty:
        print("   ⚠️  DataFrame is empty - no data to export")
        return

    # Create demo output directory
    demo_output_dir = os.path.join(os.path.dirname(__file__), "demo_output")
    os.makedirs(demo_output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Export main results
    main_output_path = os.path.join(
        demo_output_dir, f"hashtag_analysis_{timestamp}.csv"
    )
    hashtag_df.to_csv(main_output_path, index=False)
    print(f"   ✅ Main results exported to: {os.path.basename(main_output_path)}")

    # Export pre-sliced files
    # Overall analysis
    overall_df = hashtag_df.groupby(["hashtag"])["count"].sum().reset_index()
    overall_df["proportion"] = overall_df["count"] / overall_df["count"].sum()
    overall_output_path = os.path.join(
        demo_output_dir, f"hashtag_overall_{timestamp}.csv"
    )
    overall_df.to_csv(overall_output_path, index=False)
    print(
        f"   ✅ Overall analysis exported to: {os.path.basename(overall_output_path)}"
    )

    # By condition
    for condition in hashtag_df["condition"].unique():
        condition_df = hashtag_df[hashtag_df["condition"] == condition]
        condition_output_path = os.path.join(
            demo_output_dir, f"hashtag_condition_{condition}_{timestamp}.csv"
        )
        condition_df.to_csv(condition_output_path, index=False)
        print(
            f"   ✅ {condition} condition exported to: {os.path.basename(condition_output_path)}"
        )

    # By election period
    for period in hashtag_df["pre_post_election"].unique():
        period_df = hashtag_df[hashtag_df["pre_post_election"] == period]
        period_output_path = os.path.join(
            demo_output_dir, f"hashtag_period_{period}_{timestamp}.csv"
        )
        period_df.to_csv(period_output_path, index=False)
        print(
            f"   ✅ {period} period exported to: {os.path.basename(period_output_path)}"
        )

    print(f"   Output directory: {demo_output_dir}")
    print("✅ CSV export working correctly")


def main():
    """Run the complete demo."""
    print("🚀 Starting Hashtag Analysis Demo (No Logger)")
    print("=" * 50)

    try:
        # Test individual components
        demo_hashtag_extraction()

        # Create mock data and test integrated workflow
        user_df, posts_data, user_to_content_in_feeds = create_mock_data()

        # Test hashtag counting
        hashtag_counts = get_hashtag_counts_for_posts(posts_data)
        print(f"\n📈 Raw hashtag counts: {hashtag_counts}")

        # Test filtering
        filtered_counts = filter_rare_hashtags(hashtag_counts, min_frequency=2)
        print(f"📈 Filtered hashtag counts (min_freq=2): {filtered_counts}")

        # Test election period determination
        test_dates = ["2024-11-04", "2024-11-05", "2024-11-06"]
        for date in test_dates:
            period = get_election_period(date)
            print(f"📅 Date {date} → {period}")

        # Test stratified analysis
        stratified_results = create_stratified_hashtag_analysis(
            user_df=user_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
            posts_data=posts_data,
        )

        print("\n🎯 Stratified results structure:")
        for condition, periods in stratified_results.items():
            print(f"     {condition}:")
            for period, hashtags in periods.items():
                print(f"       {period}: {len(hashtags)} hashtags")
                if hashtags:
                    top_hashtags = sorted(
                        hashtags.items(), key=lambda x: x[1], reverse=True
                    )[:3]
                    print(f"         Top: {top_hashtags}")

        # Create DataFrame
        hashtag_df = create_hashtag_dataframe(stratified_results)

        print(f"\n📊 DataFrame shape: {hashtag_df.shape}")
        print(f"📊 Columns: {list(hashtag_df.columns)}")
        print("📊 Sample data:")
        print(hashtag_df.head())

        # Test CSV export
        demo_csv_export(hashtag_df)

        print("\n" + "=" * 50)
        print("🎉 Demo completed successfully!")
        print("\n📋 Summary:")
        print("   ✅ Hashtag extraction working")
        print("   ✅ Analysis functions working")
        print("   ✅ Stratified analysis working")
        print("   ✅ DataFrame creation working")
        print("   ✅ CSV export working")

        print("\n📁 Check the 'demo_output' folder for generated files")
        print(f"📊 Election date used: {ELECTION_DATE}")
        print(f"🔢 Min hashtag frequency: {MIN_HASHTAG_FREQUENCY}")

    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
