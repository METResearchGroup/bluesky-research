"""Core hashtag analysis functions.

This module contains the core hashtag extraction, analysis, and processing
functions for the hashtag analysis module.
"""

import re
from collections import defaultdict, Counter
from typing import Dict, Set, List
import pandas as pd

from lib.log.logger import get_logger

logger = get_logger(__file__)

# Constants
ELECTION_DATE = "2024-11-05"
MIN_HASHTAG_FREQUENCY = 5
HASHTAG_REGEX = re.compile(r"#\w+")


def extract_hashtags_from_text(text: str) -> List[str]:
    """Extract hashtags from post text using regex.

    Args:
        text: Post text content

    Returns:
        List of normalized hashtags (lowercase, without #)
    """
    if not text or not isinstance(text, str):
        return []

    hashtags = HASHTAG_REGEX.findall(text)
    # Normalize: remove # and convert to lowercase
    normalized_hashtags = [tag[1:].lower() for tag in hashtags]
    return normalized_hashtags


def get_hashtag_counts_for_posts(posts_df: pd.DataFrame) -> Dict[str, int]:
    """Get hashtag frequency counts for a set of posts.

    Args:
        posts_df: DataFrame containing posts with 'text' column

    Returns:
        Dictionary mapping hashtag to count
    """
    hashtag_counter = Counter()

    for _, post in posts_df.iterrows():
        hashtags = extract_hashtags_from_text(post["text"])
        hashtag_counter.update(hashtags)

    return dict(hashtag_counter)


def filter_rare_hashtags(
    hashtag_counts: Dict[str, int], min_frequency: int = MIN_HASHTAG_FREQUENCY
) -> Dict[str, int]:
    """Filter out hashtags that occur less than min_frequency times.

    Args:
        hashtag_counts: Dictionary mapping hashtag to count
        min_frequency: Minimum frequency threshold

    Returns:
        Filtered dictionary with rare hashtags removed
    """
    return {
        hashtag: count
        for hashtag, count in hashtag_counts.items()
        if count >= min_frequency
    }


def get_election_period(date_str: str) -> str:
    """Determine if a date is pre or post election.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        'pre_election' or 'post_election'
    """
    if pd.to_datetime(date_str) < pd.to_datetime(ELECTION_DATE):
        return "pre_election"
    else:
        return "post_election"


def create_stratified_hashtag_analysis(
    user_df: pd.DataFrame,
    user_to_content_in_feeds: Dict[str, Dict[str, Set[str]]],
    posts_data: pd.DataFrame,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """Create stratified hashtag analysis by condition and election period.

    Args:
        user_df: User data with conditions
        user_to_content_in_feeds: Posts used in feeds per user per day
        posts_data: Preprocessed posts data

    Returns:
        Nested dictionary: {condition: {election_period: {hashtag: count}}}
    """
    logger.info("Creating stratified hashtag analysis...")

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
    final_results = {}
    for condition, periods in stratified_results.items():
        final_results[condition] = {}
        for period, hashtag_counter in periods.items():
            hashtag_counts = dict(hashtag_counter)
            filtered_counts = filter_rare_hashtags(hashtag_counts)
            final_results[condition][period] = filtered_counts

    logger.info(f"Stratified analysis complete for {len(final_results)} conditions")
    return final_results


def create_hashtag_dataframe(
    stratified_results: Dict[str, Dict[str, Dict[str, int]]],
) -> pd.DataFrame:
    """Convert stratified hashtag results to standardized DataFrame.

    Args:
        stratified_results: Nested dictionary with hashtag counts

    Returns:
        DataFrame with columns: condition, pre_post_election, hashtag, count, proportion
    """
    logger.info("Creating hashtag analysis DataFrame...")

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
    logger.info(f"Created DataFrame with {len(df)} hashtag records")
    return df


def get_top_hashtags_by_condition(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Get top N hashtags by condition.

    Args:
        df: Hashtag analysis DataFrame
        top_n: Number of top hashtags to return per condition

    Returns:
        DataFrame with top hashtags by condition
    """
    condition_totals = df.groupby(["condition", "hashtag"])["count"].sum().reset_index()
    top_hashtags_by_condition = (
        condition_totals.groupby("condition")
        .apply(lambda x: x.nlargest(top_n, "count"))
        .reset_index(drop=True)
    )

    return top_hashtags_by_condition


def get_hashtag_summary_stats(df: pd.DataFrame) -> Dict[str, any]:
    """Get summary statistics for hashtag analysis.

    Args:
        df: Hashtag analysis DataFrame

    Returns:
        Dictionary with summary statistics
    """
    return {
        "total_hashtags": len(df["hashtag"].unique()),
        "total_records": len(df),
        "conditions": list(df["condition"].unique()),
        "election_periods": list(df["pre_post_election"].unique()),
        "most_common_hashtag": df.groupby("hashtag")["count"].sum().idxmax(),
        "most_common_hashtag_count": df.groupby("hashtag")["count"].sum().max(),
    }


def validate_hashtag_data(df: pd.DataFrame) -> bool:
    """Validate hashtag analysis DataFrame.

    Args:
        df: Hashtag analysis DataFrame

    Returns:
        True if validation passes, False otherwise
    """
    required_columns = [
        "condition",
        "pre_post_election",
        "hashtag",
        "count",
        "proportion",
    ]

    # Check required columns
    if not all(col in df.columns for col in required_columns):
        logger.error(
            f"Missing required columns. Expected: {required_columns}, Got: {list(df.columns)}"
        )
        return False

    # Check for empty DataFrame
    if df.empty:
        logger.error("DataFrame is empty")
        return False

    # Check for negative counts
    if (df["count"] < 0).any():
        logger.error("Found negative counts in data")
        return False

    # Check for invalid proportions
    if (df["proportion"] < 0).any() or (df["proportion"] > 1).any():
        logger.error("Found invalid proportions (outside 0-1 range)")
        return False

    # Check election periods
    valid_periods = {"pre_election", "post_election"}
    if not set(df["pre_post_election"].unique()).issubset(valid_periods):
        logger.error(f"Invalid election periods found. Expected: {valid_periods}")
        return False

    logger.info("Hashtag data validation passed")
    return True
