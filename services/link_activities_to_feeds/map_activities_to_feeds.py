"""Contains logic for mapping activities to feeds."""

import json

import pandas as pd

from lib.helper import get_time_difference_in_minutes, track_performance
from lib.log.logger import get_logger


logger = get_logger(__name__)


def map_likes_to_feeds(
    likes: pd.DataFrame, user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps likes to feeds."""
    return pd.DataFrame()


def map_comments_to_feeds(
    posts: pd.DataFrame, user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps posts to feeds, to check to see if they're potential comments
    to posts in the feeds.

    Checks to see if a new post has a parent or a root post (i.e., if it's a
    part of a thread), and if so, checks to see if that parent/root post
    was in the original feed.

    Returns a dataframe of the comments and feeds, with the following fields:
    - author (of the comment)
        - author_did
        - author_handle
    - comment_uri: URI of the comment post.
    - comment_text: text of the comment post.
    - comment_timestamp: timestamp when the comment was posted.
    - reply_parent: URI of the parent post that the user made a
    comment on.
    - reply_root: URI of the post that the user made a comment on.
    - thread_post_uri: URI of the post from the thread that the user made a
    comment on.
        - If both the parent and the root post from the thread were in the feed
        that we showed them, we pick the root post in the thread (there's
        other ways to tie-break too).
        - If the post is a comment but not to a post in the thread, that's OK,
        we just don't have a thread_post_uri.
    - feed_activity_timestamp: timestamp when the user logged on to the feed.
        - If the post is a comment but not to a post in the thread, that's OK,
        we just don't have a feed_activity_timestamp.
    - feed_generation_timestamp: timestamp when the feed was generated.
        - If the post is a comment but not to a post in the thread, that's OK,
        we just don't have a feed_generation_timestamp.

    Returns a dataframe of all comments that were made as well as, if applicable,
    the feed with the post that the comment was made on. Also includes comments
    that were not made on a post in the thread that was shown in their feed.

    If a post showed up in multiple feeds, we map the activity to the feed instance
    that is closest to the time that the comment was posted (but before the comment
    was posted).
    """
    posts["loaded_data"] = posts["data"].apply(json.loads)

    # a post is a comment if it has either a parent or a root post
    # (meaning that it's a part of a thread).
    comments: pd.DataFrame = posts[
        posts["loaded_data"].apply(
            lambda x: x.get("reply_parent") is not None
            or x.get("reply_root") is not None
        )
    ]

    logger.info(f"Out of {len(posts)} total posts, there are {len(comments)} comments.")

    comments_by_author = comments.groupby("author_did")

    all_comment_dicts: list[dict] = []

    for author_did, comments_df in comments_by_author:
        user_session_logs_for_user: pd.DataFrame = user_session_logs_with_feeds[
            user_session_logs_with_feeds["author_did"] == author_did
        ]
        if len(user_session_logs_for_user) == 0:
            continue

        comment_dicts: list[dict] = []

        for _, comment in comments_df.iterrows():
            res = {
                "comment_uri": comment["loaded_data"]["uri"],
                "comment_timestamp": comment["activity_timestamp"],
                "comment_text": comment["loaded_data"]["text"],
                "author_did": comment["author_did"],
                "author_handle": comment["author_handle"],
                "reply_parent": comment["loaded_data"]["reply_parent"],
                "reply_root": comment["loaded_data"]["reply_root"],
                "comment_time_minute_difference_between_comment_and_feed_open": None,
                "thread_post_uri": None,
                "feed_activity_timestamp": None,
                "feed_generation_timestamp": None,
            }

            # check to see if the comment's parent or root post (the post that
            # it's responding to) is in the given feed.
            user_session_logs_for_user["comment_parent_post_is_post_in_feed"] = (
                user_session_logs_for_user[
                    "set_of_post_uris_in_feed"
                ].apply(lambda x: res["reply_parent"] in x)
            )
            user_session_logs_for_user["comment_root_post_is_post_in_feed"] = (
                user_session_logs_for_user[
                    "set_of_post_uris_in_feed"
                ].apply(lambda x: res["reply_root"] in x)
            )
            user_session_logs_for_user["comment_is_of_post_in_feed"] = (
                user_session_logs_for_user["comment_parent_post_is_post_in_feed"]
                | user_session_logs_for_user["comment_root_post_is_post_in_feed"]
            )

            # if the comment is of a post in the feed, we need to find the
            # user session log that most closely corresponds to the time that
            # the comment was posted. This will correspond to the user session
            # log whose "activity_timestamp" is closest to (but before) the
            # comment's "comment_timestamp".
            if any(user_session_logs_for_user["comment_is_of_post_in_feed"]):
                applicable_user_session_logs: pd.DataFrame = user_session_logs_for_user[
                    user_session_logs_for_user["comment_is_of_post_in_feed"]
                ].sort_values("activity_timestamp", ascending=False)

                # check to see which user session logs are before the comment
                # was posted.
                applicable_user_session_logs["user_session_log_is_before_comment"] = (
                    applicable_user_session_logs["activity_timestamp"]
                    < res["comment_timestamp"]
                )

                # get the latest user session log that is before the comment
                # was posted.
                possible_applicable_user_session_logs = applicable_user_session_logs[
                    applicable_user_session_logs["user_session_log_is_before_comment"]
                ].iloc[-1]

                if len(possible_applicable_user_session_logs) == 0:
                    logger.info(
                        f"No user session log found that is before the comment was posted for comment {res['comment_uri']}."
                    )
                    continue

                applicable_user_session_log = (
                    possible_applicable_user_session_logs.iloc[-1]
                )

                # Validate that the timestamp of the user session log is
                # both before the comment was posted (meaning they opened the
                # feed before it was posted) and it's within a certain time
                # interval (e.g., it's unlikely for someone to post 45 minutes
                # after they open a feed, for example, so if the comment was
                # posted 45 minutes after the user opened the feed, we'll assume
                # that the comment wasn't made in response to the feed). We can,
                # for now, just track how long it was between when the comment
                # was made and when the feed was first opened during that
                # user login session.
                time_difference_in_minutes = get_time_difference_in_minutes(
                    timestamp_1=res["comment_timestamp"],
                    timestamp_2=applicable_user_session_log["activity_timestamp"],
                )
                if time_difference_in_minutes > 45:
                    logger.info(
                        f"Comment was posted {time_difference_in_minutes} minutes after the user opened the feed."
                    )

                res["comment_time_minute_difference_between_comment_and_feed_open"] = (
                    time_difference_in_minutes
                )

                # map the comment to the feed that the user was on when they
                # made the comment, as well as the URI of the post in the thread
                # that the comment was made on.
                res["feed_activity_timestamp"] = applicable_user_session_log[
                    "activity_timestamp"
                ]
                res["feed_generation_timestamp"] = applicable_user_session_log[
                    "feed_generation_timestamp"
                ]
                res["thread_post_uri"] = (
                    res["reply_parent"]
                    if applicable_user_session_log[
                        "comment_parent_post_is_post_in_feed"
                    ]
                    else res["reply_root"]
                )

            comment_dicts.append(res)

        all_comment_dicts.extend(comment_dicts)

    logger.info(f"Mapped {len(all_comment_dicts)} comments to feeds.")

    return pd.DataFrame(all_comment_dicts)


def map_follows_to_feeds(
    follows: pd.DataFrame, user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps follows to feeds.

    Checks to see if users followed any new accounts based on the posts that
    they were shown in their feeds.

    They should only be having new follows for posts that were originally
    out-of-network posts.

    NOTE: unsure how to check if a post was originally in-network or
    out-of-network.
    """
    return pd.DataFrame()


@track_performance
def export_mapped_activities(
    mapped_likes_df: pd.DataFrame,
    mapped_comments_df: pd.DataFrame,
    mapped_follows_df: pd.DataFrame,
) -> None:
    pass


@track_performance
def map_activities_to_feeds(
    latest_study_user_activities: pd.DataFrame,
    user_session_logs_with_feeds: pd.DataFrame,
) -> pd.DataFrame:
    """Maps activities (e.g., likes/posts/follows/shares) to feeds."""
    mapped_likes_df: pd.DataFrame = map_likes_to_feeds(
        likes=latest_study_user_activities[
            latest_study_user_activities["data_type"] == "like"
        ],
        user_session_logs_with_feeds=user_session_logs_with_feeds,
    )
    mapped_comments_df: pd.DataFrame = map_comments_to_feeds(
        posts=latest_study_user_activities[
            latest_study_user_activities["data_type"] == "post"
        ],
        user_session_logs_with_feeds=user_session_logs_with_feeds,
    )
    mapped_follows_df: pd.DataFrame = map_follows_to_feeds(
        follows=latest_study_user_activities[
            latest_study_user_activities["data_type"] == "follow"
        ],
        user_session_logs_with_feeds=user_session_logs_with_feeds,
    )

    logger.info("Successfully mapped activities to feeds. Exporting results...")
    export_mapped_activities(
        mapped_likes_df=mapped_likes_df,
        mapped_comments_df=mapped_comments_df,
        mapped_follows_df=mapped_follows_df,
    )
    logger.info("Successfully mapped activities to feeds and exported results.")
