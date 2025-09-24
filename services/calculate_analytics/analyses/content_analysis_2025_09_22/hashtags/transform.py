import pandas as pd
from collections import Counter
from datetime import datetime
from typing import Dict, Optional


class TopNHashtags:
    """Container for top N hashtags with frequency counts."""

    def __init__(self, hashtags_dict: Optional[Dict[str, int]] = None):
        self.hashtags_dict = hashtags_dict or {}

    def get_top_n(self, n: int = 10) -> Dict[str, int]:
        """Get top N hashtags by frequency."""
        return dict(Counter(self.hashtags_dict).most_common(n))

    def get_all_hashtags(self) -> Dict[str, int]:
        """Get all hashtags with their counts."""
        return self.hashtags_dict.copy()

    def add_hashtag(self, hashtag: str, count: int = 1):
        """Add hashtag with count."""
        self.hashtags_dict[hashtag] = self.hashtags_dict.get(hashtag, 0) + count


def aggregate_hashtags_by_condition_and_pre_post(
    uri_to_hashtags: dict[str, dict],
    user_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
    top_n: int = 20,
    election_cutoff_date: str = "2024-11-05",
    frequency_threshold: int = 5,
) -> Dict:
    """
    Aggregate hashtags by condition and pre/post-election periods.

    Returns structured data suitable for visualization similar to NER implementation.
    """

    # Initialize containers
    condition_data = {}
    election_date_data = {}
    overall_data = {}

    # Create user condition mapping
    user_to_condition = dict(zip(user_df["bluesky_user_did"], user_df["condition"]))

    # Initialize TopNHashtags objects for each condition
    conditions = [
        "reverse_chronological",
        "engagement",
        "representative_diversification",
    ]
    for condition in conditions:
        condition_data[condition] = TopNHashtags()

    # Initialize TopNHashtags objects for pre/post election
    election_date_data["pre_election"] = TopNHashtags()
    election_date_data["post_election"] = TopNHashtags()

    # Initialize overall TopNHashtags
    overall_data["overall"] = TopNHashtags()

    # Process each user's feeds
    for user_did, feeds in user_to_content_in_feeds.items():
        condition = user_to_condition.get(user_did)
        if not condition:
            continue

        # Process each feed date for this user
        for feed_date, post_uris in feeds.items():
            # Determine if this is pre or post election
            feed_datetime = datetime.strptime(feed_date, "%Y-%m-%d")
            election_datetime = datetime.strptime(election_cutoff_date, "%Y-%m-%d")

            period = (
                "pre_election"
                if feed_datetime <= election_datetime
                else "post_election"
            )

            # Aggregate hashtags for posts in this feed
            for post_uri in post_uris:
                if post_uri in uri_to_hashtags:
                    hashtag_counts = uri_to_hashtags[post_uri]

                    # Add to condition-specific aggregation
                    for hashtag, count in hashtag_counts.items():
                        condition_data[condition].add_hashtag(hashtag, count)

                    # Add to election period aggregation
                    for hashtag, count in hashtag_counts.items():
                        election_date_data[period].add_hashtag(hashtag, count)

                    # Add to overall aggregation
                    for hashtag, count in hashtag_counts.items():
                        overall_data["overall"].add_hashtag(hashtag, count)

    # Apply frequency threshold filtering
    def filter_by_threshold(topn_obj: TopNHashtags, threshold: int) -> TopNHashtags:
        """Filter hashtags by frequency threshold."""
        filtered_dict = {
            hashtag: count
            for hashtag, count in topn_obj.get_all_hashtags().items()
            if count >= threshold
        }
        return TopNHashtags(filtered_dict)

    # Apply filtering to all aggregations
    for condition in conditions:
        condition_data[condition] = filter_by_threshold(
            condition_data[condition], frequency_threshold
        )

    election_date_data["pre_election"] = filter_by_threshold(
        election_date_data["pre_election"], frequency_threshold
    )
    election_date_data["post_election"] = filter_by_threshold(
        election_date_data["post_election"], frequency_threshold
    )
    overall_data["overall"] = filter_by_threshold(
        overall_data["overall"], frequency_threshold
    )

    # Create ranking comparison for election periods
    pre_hashtags = election_date_data["pre_election"].get_top_n(top_n)
    post_hashtags = election_date_data["post_election"].get_top_n(top_n)

    # Create ranking comparison
    ranking_comparison = create_ranking_comparison(pre_hashtags, post_hashtags)

    return {
        "condition": condition_data,
        "election_date": election_date_data,
        "overall": overall_data,
        "ranking_comparison": ranking_comparison,
    }


def create_ranking_comparison(
    pre_hashtags: Dict[str, int], post_hashtags: Dict[str, int]
) -> Dict:
    """Create ranking comparison between pre and post election periods."""

    # Create ranking dictionaries
    pre_ranking = {
        hashtag: rank + 1 for rank, hashtag in enumerate(pre_hashtags.keys())
    }
    post_ranking = {
        hashtag: rank + 1 for rank, hashtag in enumerate(post_hashtags.keys())
    }

    # Find common hashtags
    common_hashtags = set(pre_hashtags.keys()) & set(post_hashtags.keys())

    # Create comparison data
    pre_to_post = {}
    post_to_pre = {}

    # Process pre-to-post changes
    for hashtag in common_hashtags:
        pre_rank = pre_ranking[hashtag]
        post_rank = post_ranking[hashtag]
        change = pre_rank - post_rank  # Positive means improved (lower rank number)

        pre_to_post[hashtag] = {
            "pre_rank": pre_rank,
            "post_rank": post_rank,
            "change": change,
        }

    # Process post-to-pre changes (for hashtags that appeared in post but not pre)
    for hashtag in set(post_hashtags.keys()) - set(pre_hashtags.keys()):
        post_rank = post_ranking[hashtag]
        post_to_pre[hashtag] = {
            "pre_rank": None,
            "post_rank": post_rank,
            "change": "N/A",
        }

    return {
        "pre_to_post": pre_to_post,
        "post_to_pre": post_to_pre,
    }
