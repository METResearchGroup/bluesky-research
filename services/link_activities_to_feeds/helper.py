"""Helper code for linking activities to feeds."""

from datetime import datetime, timedelta
import json

import pandas as pd

from lib.constants import timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger

# how many days to try and fetch data from.
default_lookback_days = 2

logger = get_logger(__name__)


def load_latest_study_user_activities(latest_timestamp: str) -> pd.DataFrame:
    df = load_data_from_local_storage(
        service="aggregated_study_user_activities",
        latest_timestamp=latest_timestamp,
    )
    return df


default_post_conversation_dict = {
    "uri": "",
    "llm_model_name": "",
    "sociopolitical_was_successfully_labeled": "",
    "sociopolitical_reason": "",
    "sociopolitical_label_timestamp": "",
    "political_ideology_label": "",
    "perspective_was_successfully_labeled": "",
    "perspective_reason": "",
    "perspective_label_timestamp": "",
    "prob_toxic": "",
    "prob_constructive": "",
}

default_post_ime_score_dict = {
    "uri": "",
    "text": "",
    "prob_emotion": "",
    "prob_intergroup": "",
    "prob_moral": "",
    "prob_other": "",
    "label_emotion": "",
    "label_intergroup": "",
    "label_moral": "",
    "label_other": "",
    "source": "",
    "label_timestamp": "",
}

@track_performance
def get_user_session_logs_with_feeds_shown(
    latest_study_user_activities: pd.DataFrame
) -> tuple[pd.DataFrame, set[str]]:
    """Gets the user session logs with feeds shown, loaded from JSON."""
    user_session_logs = latest_study_user_activities[
        latest_study_user_activities["data_type"] == "user_session_log"
    ]
    user_session_log_data: list[dict] = user_session_logs["data"].apply(json.loads)
    
    # list of lists of dicts, corresponding to lists of feeds shown.
    feeds_shown: list[list[dict]] = [
        json.loads(data["feed"]) for data in user_session_log_data
    ]

    user_session_logs["feeds_shown"] = feeds_shown
    user_session_logs["cursor"] = user_session_log_data.apply(lambda x: x["cursor"])

    uris_of_posts_shown_in_feeds: set[str] = set()
    for feed in feeds_shown:
        for post in feed:
            uris_of_posts_shown_in_feeds.add(post["post"])

    return user_session_logs, uris_of_posts_shown_in_feeds


@track_performance
def load_latest_consolidated_posts(latest_timestamp: str) -> pd.DataFrame:
    consolidated_posts: pd.DataFrame = load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        latest_timestamp=latest_timestamp,
    )
    return consolidated_posts


@track_performance
def load_post_conversation_traits(
    post_uris: list[str], latest_timestamp: str
) -> pd.DataFrame:
    """Loads the conversation traits of posts.

    Loads the previous consolidated posts and returns the conversation traits of the posts.
    """
    consolidated_posts: pd.DataFrame = load_latest_consolidated_posts(
        latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(consolidated_posts)} total consolidated posts.")
    post_conversation_traits: pd.DataFrame = consolidated_posts.loc[
        consolidated_posts["uri"].isin(post_uris),
        [
            "uri",
            "author_did",
            "llm_model_name",
            "sociopolitical_was_successfully_labeled",
            "sociopolitical_reason",
            "sociopolitical_label_timestamp",
            "political_ideology_label",
            "perspective_was_successfully_labeled",
            "perspective_reason",
            "perspective_label_timestamp",
            "prob_toxic",
            "prob_constructive",
        ],
    ].reset_index(drop=True)
    return post_conversation_traits


@track_performance
def load_post_ime_scores(post_uris: list[str], latest_timestamp: str) -> pd.DataFrame:
    """Loads the IM scores of posts."""
    latest_ime_scores: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        latest_timestamp=latest_timestamp,
    )
    logger.info(f"Loaded {len(latest_ime_scores)} total IME scores.")
    latest_ime_scores = latest_ime_scores.loc[
        latest_ime_scores["uri"].isin(post_uris),
    ].reset_index(drop=True)
    return latest_ime_scores


def assign_session_hash(timestamp: str, window_period_minutes: int) -> str:
    """Assigns a session hash to a user session log based on the timestamp.

    The hash will be a compound string of the date plus the window period
    number plus the bucket that the hour/minute combination falls into if
    the window period is used.

    For example, if the window period is 5 minutes, then a post from
    12:39am would be in the 8th bucket (between 12:35am and 12:40am).

    If the post is from 2024-10-01, then the hash would be
    2024_10_01_5_8, corresponding to:
        - 2024-10-01 (date)
        - 5 (window period in minutes)
        - 8 (bucket number)

    Note that this uses a fixed window period instead of a sliding window period.
    As a result, for example, in a 5 minute period, 12:39am and 12:41am will
    be in separate windows.
    """
    # Parse the timestamp into a datetime object
    dt = datetime.strptime(timestamp, timestamp_format)
    
    # Extract date components
    date_str = dt.strftime("%Y_%m_%d")
    
    # Calculate which bucket the time falls into based on window period
    minutes_since_midnight = dt.hour * 60 + dt.minute
    bucket_number = minutes_since_midnight // window_period_minutes
    
    # Combine components into hash string
    session_hash = f"{date_str}_{window_period_minutes}_{bucket_number}"
    
    return session_hash


def split_user_session_logs_by_sliding_window(
    user_session_logs_df: pd.DataFrame,
    window_period_minutes: int
) -> list[pd.DataFrame]:
    """Splits user session logs into multiple dataframes, each corresponding to a
    single sliding window period.
    
    Uses a sliding window approach where any rows within window_period_minutes of each
    other will be grouped together. For example, if window_period_minutes=5:
    - A row at 12:39 and a row at 12:42 will be grouped together since they're within 5 min
    - A row at 12:39 and a row at 12:45 will be in separate groups since they're 6 min apart
    
    Returns a list of dataframes, where each dataframe contains rows that fall within
    the same sliding window period.

    This makes the following assumptions:
    - The first user session log in a window is the first request for that feed
    (we have no way to 100% check this, but if there are multiple feed requests,
    it's reasonable to assume that the first one is the initial request and the
    others are paginated requests).
    - The last request in a series of paginated requests will have the eof
    cursor.

    We know that all feeds will be 4 unique feed requests long (feeds are
    guaranteed to have length of 100, and 4 requests are made to fetch all of
    them), so we can actually work backwards from the last request to piece
    together the full feed.

    (note: this only works if we have 4 requests per feed. We'll have to
    figure out a more clever way of tracking if the # of requests changes.
    One way of doing this is prepend future user session logs with 'start_',
    so we know which request was first).

    Otherwise, this approach is a reasonable shorthand.

    As a first pass, we can just iterate through each user session log and then
    stop when we hit the eof cursor, and assume that everything between eof
    cursors corresponds to the same feed. We need to do this within a sliding
    window, since not all users scroll through the end of their feeds. This
    does introduce an edge case of half a feed showing up in one window
    and the other half in another window, but that's probably fine and my
    testing shows that it's much more common for users to not scroll through
    their feeds completely (i.e., never hit the 'eof' cursor) than for this
    edge case to happen.

    So, within a given sliding window interval, we can assume that all of the
    user session logs before an eof cursor are part of the same feed.

    The algorithm works as follows:
    - Sort the user session logs by timestamp.
    - Initialize a current group with the first user session log.
    - Initialize a window start time with the timestamp of the first user session log.
    - For the user session logs within that window, we do the following:
        - If it's an eof cursor, we assume that everything up to and including
        that cursor is part of that same feed.
        - Otherwise, we add to the current group and wait for the next eof cursor.
    - Once we've iterated across that window, we treat whatever is left (if we
    haven't hit an eof cursor) as its own feed.
    - We then move our pointer to the next user session log outside of this
    window and repeat the process.
    """
    if len(user_session_logs_df) == 0:
        return []

    # Sort logs by timestamp
    sorted_logs = user_session_logs_df.sort_values("activity_timestamp").reset_index(drop=True)
    
    result_groups = []
    current_group = []
    current_window_start = None
    
    for idx, row in sorted_logs.iterrows():
        current_timestamp = datetime.strptime(row["activity_timestamp"], "%Y-%m-%d-%H:%M:%S")
        
        # Start new window if this is first row or outside current window
        if current_window_start is None:
            current_window_start = current_timestamp
            current_group = [row]
            continue
            
        time_diff = (current_timestamp - current_window_start).total_seconds() / 60
        
        if time_diff <= window_period_minutes:
            # Within window - add to current group
            current_group.append(row)
            
            # If this is an eof cursor, save current group and start new one
            if "eof" in str(row["cursor"]):
                result_groups.append(pd.DataFrame(current_group))
                current_group = []
                
        else:
            # Outside window - save current group if not empty
            if current_group:
                result_groups.append(pd.DataFrame(current_group))
            
            # Start new window
            current_window_start = current_timestamp
            current_group = [row]
            
    # Add final group if not empty
    if current_group:
        result_groups.append(pd.DataFrame(current_group))
        
    return result_groups


def consolidate_paginated_user_session_logs(session_logs_df: pd.DataFrame) -> pd.DataFrame:
    """Consolidates (and dedupes) paginated requests for the same feed."""
    if len(session_logs_df) == 1:
        return session_logs_df
    else:
        # TODO: implement
        breakpoint()
        start_timestamp = min(session_logs_df["activity_timestamp"])
        full_feed = []
        seen_post_uris = set()

        # order session logs by activity timestamp ascending.

        # loop through each session log and add the posts from the feed
        # to the full feed if the posts aren't in the seen post uris

        # create a new dataframe that has the consolidated into
        res = {}
        consolidated_paginated_session_log_df = pd.DataFrame(res)
        return consolidated_paginated_session_log_df


def consolidate_user_session_log_paginated_requests(
    user_session_logs_with_feeds: pd.DataFrame,
    window_period_minutes: int = 5
) -> pd.DataFrame:
    """Consolidates paginated requests for the same feed.
    
    Bluesky only fetches 30 results for a feed at a time, so if a user
    scrolls through far enough, they'll incur multiple requests for the same
    feed. This function collapses them together.

    It assumes that the requests for a user within a given window period are
    in fact paginated requests for the same feed. It dedupes those requests
    and then creates a single user session log that has the initial timestamp
    of the first request and then the full feed of posts shown (by concatenating
    the posts across multiple paginated requests).

    It does so by grouping the user session logs by user and then assigning each
    user session log row to a context window that's the size of the window
    period. Then it groups by that context window and concatenates the user session
    logs together. We assume that a user is unlikely to log into the app multiple
    times in a 2-minute window, for example, so this is a safe assumption. Plus
    we don't really want to count intervals that short as separate events
    anyways. Plus the posts that a person will see won't differ in a
    sufficiently short window.

    NOTE: I think I should implement this as a sliding window instead
    of a fixed window period.
    """
    output_session_logs: list[pd.DataFrame] = []

    # group by user, and then assign each user session log row to a context
    # window that's double the size of the window period. Then group by that
    # context window and concatenate the user session logs together.

    # iterate through each user and their session logs.
    for _, user_session_logs_df in user_session_logs_with_feeds.groupby("author_did"):
        # split up user session logs by sliding window approach
        split_user_session_logs: list[pd.DataFrame] = split_user_session_logs_by_sliding_window(
            user_session_logs_df=user_session_logs_df,
            window_period_minutes=window_period_minutes
        )
        for session_logs_df in split_user_session_logs:
            consolidated_session_logs_df = consolidate_paginated_user_session_logs(session_logs_df)
            output_session_logs.append(consolidated_session_logs_df)
    logger.info(f"After consolidating paginated user session logs, there are {len(output_session_logs)} user session logs (out of {len(user_session_logs_with_feeds)} originally).") # noqa
    return pd.concat(output_session_logs, ignore_index=True)
    


@track_performance
def enrich_feed_posts_with_attributes(
    user_session_logs_with_feeds: pd.DataFrame,
    post_conversation_traits: pd.DataFrame,
    post_ime_scores: pd.DataFrame
) -> list[list[dict]]:
    """Given the posts in the feeds, enrich them with their conversation traits
    and IM scores."""
    feeds = user_session_logs_with_feeds["data"].tolist()
    updated_feeds: list[list[dict]] = []

    post_ime_score_map: dict[str, dict] = {
        post["uri"]: {
            "uri": post["uri"],
            "text": post["text"],
            "prob_emotion": post["prob_emotion"],
            "prob_intergroup": post["prob_intergroup"],
            "prob_moral": post["prob_moral"],
            "prob_other": post["prob_other"],
            "label_emotion": post["label_emotion"],
            "label_intergroup": post["label_intergroup"],
            "label_moral": post["label_moral"],
            "label_other": post["label_other"],
            "source": post["source"],
            "label_timestamp": post["label_timestamp"],
        }
        for post in post_ime_scores.to_dict(orient="records")
    }

    post_conversation_trait_map: dict[str, dict] = {
        post["uri"]: {
            "uri": post["uri"],
            "author_did": post["author_did"],
            "llm_model_name": post["llm_model_name"],
            "sociopolitical_was_successfully_labeled": post["sociopolitical_was_successfully_labeled"],
            "sociopolitical_reason": post["sociopolitical_reason"],
            "sociopolitical_label_timestamp": post["sociopolitical_label_timestamp"],
            "political_ideology_label": post["political_ideology_label"],
            "perspective_was_successfully_labeled": post["perspective_was_successfully_labeled"],
            "perspective_reason": post["perspective_reason"],
            "perspective_label_timestamp": post["perspective_label_timestamp"],
            "prob_toxic": post["prob_toxic"],
            "prob_constructive": post["prob_constructive"],
        }
        for post in post_conversation_traits.to_dict(orient="records")
    }

    total_posts: int = 0
    total_posts_not_in_post_ime_scores: int = 0
    total_posts_not_in_post_conversation_traits: int = 0

    for feed in feeds:
        updated_feed: list[dict] = []
        for post in feed:
            post_ime_score_dict = post_ime_score_map.get(post["post"], default_post_ime_score_dict)
            if not post_ime_score_dict["uri"]:
                total_posts_not_in_post_ime_scores += 1
            post_conversation_trait_dict = post_conversation_trait_map.get(post["post"], default_post_conversation_dict)
            if not post_conversation_trait_dict["uri"]:
                total_posts_not_in_post_conversation_traits += 1
            updated_feed.append({
                **post,
                **post_ime_score_dict,
                **post_conversation_trait_dict,
            })
            total_posts += 1
        updated_feeds.append(updated_feed)

    logger.info(f"Total posts from feeds: {total_posts}")
    logger.info(f"Total posts without post IME scores: {total_posts_not_in_post_ime_scores}/{total_posts}")
    logger.info(f"Total posts without post conversation traits (toxicity/constructiveness): {total_posts_not_in_post_conversation_traits}/{total_posts}")

    return updated_feeds



def map_likes_to_feeds(
    likes: pd.DataFrame,
    user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps likes to feeds."""
    pass


def map_comments_to_feeds(
    posts: pd.DataFrame,
    user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps posts to feeds, to check to see if they're potential comments
    to posts in the feeds.
    
    Checks to see if a new post has a parent or a root post (i.e., if it's a
    part of a thread), and if so, checks to see if that parent/root post
    was in the original feed.
    """
    map_user_to_comment = {}
    map_user_to_user_session_logs = {}

    posts["loaded_data"] = posts["data"].apply(json.loads)

    # a post is a comment if it has either a parent or a root post
    # (meaning that it's a part of a thread).
    comments: pd.DataFrame = posts[
        posts["loaded_data"].apply(
            lambda x: x.get("reply_parent") is not None or x.get("reply_root") is not None
        )
    ]

    logger.info(f"Out of {len(posts)} total posts, there are {len(comments)} comments.")

    comments_by_author = comments.groupby("author_did")

    for author_did, posts_df in comments_by_author:
        user_session_logs_for_user = user_session_logs_with_feeds[
            user_session_logs_with_feeds["user_did"] == author_did
        ]
        # TODO: loop through each of the user sessions and check to see if the 
        # comment is linked to a post in that feed. Then check to see if the
        # timestamps match up. Take note then of the comment, the post from the 
        # feed that it responds to, and the feed itself. Can add a new column to
        # the user_session_log with something like "user_commented_on_post_from_feed"
        # which is a boolean, and then a "comments_on_post_from_feed" column which
        # has the URIs of the posts that were commented on.

        # TODO: requires that user_session_logs are consolidated (i.e., multiple
        # requests for the same feed are collapsed together).
        pass

    breakpoint()



def map_follows_to_feeds(
    follows: pd.DataFrame,
    user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps follows to feeds.
    
    Checks to see if users followed any new accounts based on the posts that
    they were shown in their feeds.

    They should only be having new follows for posts that were originally
    out-of-network posts.

    NOTE: unsure how to check if a post was originally in-network or
    out-of-network.
    """
    pass


@track_performance
def map_activities_to_feeds(
    latest_study_user_activities: pd.DataFrame,
    user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps activities (e.g., likes/posts/follows/shares) to feeds."""
    mapped_df = pd.DataFrame()
    # mapped_likes_df = map_likes_to_feeds(
    #     likes=latest_study_user_activities[latest_study_user_activities["data_type"] == "like"],
    #     user_session_logs_with_feeds=user_session_logs_with_feeds
    # )
    mapped_comments_df = map_comments_to_feeds(
        posts=latest_study_user_activities[latest_study_user_activities["data_type"] == "post"],
        user_session_logs_with_feeds=user_session_logs_with_feeds
    )
    # mapped_follows_df = map_follows_to_feeds(
    #     follows=latest_study_user_activities[latest_study_user_activities["data_type"] == "follow"],
    #     user_session_logs_with_feeds=user_session_logs_with_feeds
    # )
    breakpoint()
    return mapped_df


@track_performance
def link_activities_to_feeds(lookback_days: int = default_lookback_days):
    """Links activities to feeds.

    Requires that we load study user activities, enrich the posts that were shown
    in the feeds, and then map the activities to the feeds.
    """
    latest_timestamp: str = (datetime.now() - timedelta(days=lookback_days)).strftime(
        timestamp_format
    )
    latest_study_user_activities: pd.DataFrame = load_latest_study_user_activities(
        latest_timestamp=latest_timestamp
    )

    # fetch the user session logs and separate them out from the study user activities
    # so that they can be processed separately. Also load the URIs of the posts
    # that have been shown in the feeds.
    user_session_logs_with_feeds, uris_of_posts_shown_in_feeds = get_user_session_logs_with_feeds_shown(
        latest_study_user_activities=latest_study_user_activities
    )
    latest_study_user_activities = latest_study_user_activities[
        latest_study_user_activities["data_type"] != "user_session_log"
    ].reset_index(drop=True)

    logger.info(f"Loaded {len(latest_study_user_activities)} study user activities.")
    logger.info(f"Loaded {len(user_session_logs_with_feeds)} user session logs with feeds shown.")


    user_session_logs_with_feeds: pd.DataFrame = consolidate_user_session_log_paginated_requests(
        user_session_logs_with_feeds=user_session_logs_with_feeds
    )

    post_conversation_traits: pd.DataFrame = load_post_conversation_traits(
        post_uris=uris_of_posts_shown_in_feeds,
        latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(post_conversation_traits)} posts with conversation traits.")
    post_ime_scores: pd.DataFrame = load_post_ime_scores(
        post_uris=uris_of_posts_shown_in_feeds,
        latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(post_ime_scores)} post IM scores.")

    enriched_feeds: list[list[dict]] = enrich_feed_posts_with_attributes(
        user_session_logs_with_feeds=user_session_logs_with_feeds,
        post_conversation_traits=post_conversation_traits,
        post_ime_scores=post_ime_scores
    )

    user_session_logs_with_feeds["enriched_feeds"] = enriched_feeds

    mapped_df: pd.DataFrame = map_activities_to_feeds(
        latest_study_user_activities=latest_study_user_activities,
        user_session_logs_with_feeds=user_session_logs_with_feeds
    )

    breakpoint()


if __name__ == "__main__":
    link_activities_to_feeds()
