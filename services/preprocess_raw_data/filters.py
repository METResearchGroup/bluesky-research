"""Performs filtering steps."""

from typing import Any, cast

import pandas as pd

from lib.helper import track_performance
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import Logger
from services.preprocess_raw_data.classify_language.helper import filter_text_is_english  # noqa
from services.preprocess_raw_data.classify_nsfw_content.helper import (
    filter_post_content_nsfw,
    filter_post_author_nsfw,
)  # noqa
from services.preprocess_raw_data.classify_spam.helper import filter_posts_have_spam  # noqa

logger = Logger(__name__)


def _stage_remove_posts_without_text(posts: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove posts with missing text."""
    text_series = cast(pd.Series, posts["text"])
    num_posts_without_text = int(text_series.isna().sum())
    filtered_posts = cast(pd.DataFrame, posts[text_series.notna()].copy())
    return filtered_posts, num_posts_without_text


def _stage_mark_english(posts: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Add is_english classification and return count of english posts."""
    posts_with_language = posts.copy()
    text_series = cast(pd.Series, posts_with_language["text"])
    posts_with_language["is_english"] = filter_text_is_english(texts=text_series)
    num_english_posts = int(cast(pd.Series, posts_with_language["is_english"]).sum())
    return posts_with_language, num_english_posts


def _stage_keep_english(posts: pd.DataFrame) -> pd.DataFrame:
    """Keep only english posts."""
    english_series = cast(pd.Series, posts["is_english"])
    return cast(pd.DataFrame, posts[english_series].copy())


def _stage_add_nsfw_and_spam_flags(posts: pd.DataFrame) -> pd.DataFrame:
    """Add nsfw/spam helper columns."""
    flagged_posts = posts.copy()
    text_series = cast(pd.Series, flagged_posts["text"])
    labels_series = cast(pd.Series, flagged_posts["labels"])
    author_did_series = cast(pd.Series, flagged_posts["author_did"])
    author_handle_series = cast(pd.Series, flagged_posts["author_handle"])
    flagged_posts["post_is_nsfw"] = filter_post_content_nsfw(
        texts=text_series, labels=labels_series
    )
    flagged_posts["author_is_nsfw"] = filter_post_author_nsfw(
        author_dids=author_did_series,
        author_handles=author_handle_series,
    )
    flagged_posts["is_spam"] = filter_posts_have_spam(text_series)
    return flagged_posts


def _stage_add_filter_decision(posts: pd.DataFrame) -> pd.DataFrame:
    """Add pass/fail decision columns."""
    posts_with_decisions = posts.copy()
    posts_with_decisions["passed_filters"] = ~(
        posts_with_decisions["post_is_nsfw"]
        | posts_with_decisions["author_is_nsfw"]
        | posts_with_decisions["is_spam"]
    )
    posts_with_decisions["filtered_by_func"] = None
    for column in ["post_is_nsfw", "author_is_nsfw", "is_spam"]:
        posts_with_decisions.loc[posts_with_decisions[column], "filtered_by_func"] = (
            column
        )
    return posts_with_decisions


def _stage_add_timestamps(
    posts: pd.DataFrame, custom_args: dict[str, Any]
) -> pd.DataFrame:
    """Add preprocessing and filtered timestamps."""
    posts_with_timestamps = posts.copy()
    if custom_args:
        timestamp_field = custom_args["new_timestamp_field"]
        posts_with_timestamps["preprocessing_timestamp"] = posts_with_timestamps[
            timestamp_field
        ]
        posts_with_timestamps["filtered_at"] = posts_with_timestamps[timestamp_field]
    else:
        timestamp = generate_current_datetime_str()
        posts_with_timestamps["preprocessing_timestamp"] = timestamp
        posts_with_timestamps["filtered_at"] = timestamp
    return posts_with_timestamps


def _build_filter_to_count_map(posts: pd.DataFrame) -> dict[str, int]:
    """Build breakdown counts for each non-language filter."""
    return {
        "post_is_nsfw": int(posts["post_is_nsfw"].sum()),
        "author_is_nsfw": int(posts["author_is_nsfw"].sum()),
        "is_spam": int(posts["is_spam"].sum()),
    }


def _emit_filtering_logs(
    posts: pd.DataFrame,
    num_posts_without_text: int,
    num_posts_after_null_filter: int,
    num_english_posts: int,
    filter_to_count_map: dict[str, int],
) -> None:
    """Emit logs from computed stats without mutating data."""
    logger.info(f"Total posts for filtering: {len(posts) + num_posts_without_text}")
    logger.info(f"Number of posts without text: {num_posts_without_text}")
    logger.info(
        f"After English filtering, number of posts that passed filter: {num_english_posts}"
    )
    logger.info(
        f"After English filtering, number of posts that failed filter: {num_posts_after_null_filter - num_english_posts}"
    )
    for filter_col, count in filter_to_count_map.items():
        logger.info(f"Number of posts failed `{filter_col}`: {count}")
    print(posts["filtered_by_func"].value_counts())


def _build_updated_posts_metadata(
    posts: pd.DataFrame, num_english_posts: int, filter_to_count_map: dict[str, int]
) -> dict[str, Any]:
    """Build metadata payload expected by upstream callers."""
    return {
        "num_posts": len(posts),
        "num_records_after_filtering": {
            "posts": {
                "passed": len(posts[posts["passed_filters"]]),
                "failed_total": len(posts[~posts["passed_filters"]]),
                "failed_breakdown": {
                    "is_english": num_english_posts,
                    "post_is_nsfw": filter_to_count_map["post_is_nsfw"],
                    "author_is_nsfw": filter_to_count_map["author_is_nsfw"],
                    "is_spam": filter_to_count_map["is_spam"],
                },
            }
        },
    }


def _drop_internal_columns(posts: pd.DataFrame) -> pd.DataFrame:
    """Drop internal helper columns before return."""
    return posts.drop(columns=["post_is_nsfw", "author_is_nsfw", "is_spam"])


@track_performance
def filter_posts(
    posts: pd.DataFrame, custom_args: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Applies the filtering steps."""  # noqa
    posts_without_null_text, num_posts_without_text = _stage_remove_posts_without_text(
        posts
    )
    posts_with_english, num_english_posts = _stage_mark_english(posts_without_null_text)
    english_posts = _stage_keep_english(posts_with_english)
    posts_with_flags = _stage_add_nsfw_and_spam_flags(english_posts)
    posts_with_decisions = _stage_add_filter_decision(posts_with_flags)
    posts_with_timestamps = _stage_add_timestamps(posts_with_decisions, custom_args)

    filter_to_count_map = _build_filter_to_count_map(posts_with_timestamps)
    _emit_filtering_logs(
        posts=posts_with_timestamps,
        num_posts_without_text=num_posts_without_text,
        num_posts_after_null_filter=len(posts_without_null_text),
        num_english_posts=num_english_posts,
        filter_to_count_map=filter_to_count_map,
    )
    updated_posts_metadata = _build_updated_posts_metadata(
        posts=posts_with_timestamps,
        num_english_posts=num_english_posts,
        filter_to_count_map=filter_to_count_map,
    )
    posts_for_return = _drop_internal_columns(posts_with_timestamps)
    logger.info("Completed post filtering in preprocessing pipeline.")
    return (posts_for_return, updated_posts_metadata)
