"""Performs filtering steps."""

import pandas as pd

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import Logger
from services.preprocess_raw_data.classify_bots.helper import (
    filter_posts_written_by_bot_accounts,
)  # noqa
from services.preprocess_raw_data.classify_hate_speech.helper import (
    filter_posts_have_hate_speech,
)  # noqa
from services.preprocess_raw_data.classify_language.helper import filter_text_is_english  # noqa
from services.preprocess_raw_data.classify_nsfw_content.helper import (
    filter_post_content_nsfw,
    filter_post_author_nsfw,
)  # noqa
from services.preprocess_raw_data.classify_spam.helper import filter_posts_have_spam  # noqa

logger = Logger(__name__)


@track_performance
def filter_posts(posts: pd.DataFrame, custom_args: dict) -> tuple[pd.DataFrame, dict]:
    """Applies the filtering steps."""  # noqa
    logger.info(f"Total posts for filtering: {len(posts)}")

    # remove posts without text
    num_posts_without_text = len(posts[posts["text"].isna()])
    logger.info(f"Number of posts without text: {num_posts_without_text}")
    posts = posts[posts["text"].notna()]

    # add language filter. This is the most important filter and likely
    # filters out the most posts.
    posts["is_english"] = filter_text_is_english(texts=posts["text"])

    num_english_posts = len(posts[posts["is_english"]])
    logger.info(
        f"After English filtering, number of posts that passed filter: {num_english_posts}"
    )  # noqa
    logger.info(
        f"After English filtering, number of posts that failed filter: {len(posts) - num_english_posts}"
    )  # noqas

    posts = posts[posts["is_english"]]

    # add other filters:

    # 1. not from possible bot account
    posts["author_is_bot"] = filter_posts_written_by_bot_accounts(
        author_dids=posts["author_did"], author_handles=posts["author_handle"]
    )

    # 2. no nsfw content
    posts["post_is_nsfw"] = filter_post_content_nsfw(
        texts=posts["text"], labels=posts["labels"]
    )
    posts["author_is_nsfw"] = filter_post_author_nsfw(
        author_dids=posts["author_did"], author_handles=posts["author_handle"]
    )

    # 3. no spam
    posts["is_spam"] = filter_posts_have_spam(posts["text"])

    # 4. no hate speech
    posts["is_hate_speech"] = filter_posts_have_hate_speech(posts["text"])

    # Add a column 'passed_filters' to the posts dataframe
    posts["passed_filters"] = ~(
        posts["post_is_nsfw"]
        | posts["author_is_nsfw"]
        | posts["is_spam"]
        | posts["is_hate_speech"]
    )

    posts["filtered_by_func"] = None
    filter_columns = [
        "author_is_bot",
        "post_is_nsfw",
        "author_is_nsfw",
        "is_spam",
        "is_hate_speech",
    ]
    for col in filter_columns:
        posts.loc[posts[col], "filtered_by_func"] = col

    if custom_args:
        ts_field = custom_args["new_timestamp_field"]
        posts["preprocessing_timestamp"] = posts[ts_field]
        posts["filtered_at"] = posts[ts_field]
    else:
        ts = generate_current_datetime_str()
        posts["preprocessing_timestamp"] = ts
        posts["filtered_at"] = ts

    # count up the # of posts that failed each filter.
    filter_to_count_map = {
        "author_is_bot": sum(posts["author_is_bot"]),
        "post_is_nsfw": sum(posts["post_is_nsfw"]),
        "author_is_nsfw": sum(posts["author_is_nsfw"]),
        "is_spam": sum(posts["is_spam"]),
        "is_hate_speech": sum(posts["is_hate_speech"]),
    }
    for filter_col, count in filter_to_count_map.items():
        logger.info(f"Number of posts failed `{filter_col}`: {count}")

    print(posts["filtered_by_func"].value_counts())

    updated_posts_metadata = {
        "num_posts": len(posts),
        "num_records_after_filtering": {
            "posts": {
                "passed": len(posts[posts["passed_filters"]]),
                "failed_total": len(posts[~posts["passed_filters"]]),
                "failed_breakdown": {
                    "is_english": num_english_posts,
                    "author_is_bot": filter_to_count_map["author_is_bot"],
                    "post_is_nsfw": filter_to_count_map["post_is_nsfw"],
                    "author_is_nsfw": filter_to_count_map["author_is_nsfw"],
                    "is_spam": filter_to_count_map["is_spam"],
                    "is_hate_speech": filter_to_count_map["is_hate_speech"],
                },
            }
        },
    }

    cols_to_drop = [
        "author_is_bot",
        "post_is_nsfw",
        "author_is_nsfw",
        "is_spam",
        "is_hate_speech",
    ]
    posts = posts.drop(columns=cols_to_drop)

    logger.info("Completed post filtering in preprocessing pipeline.")

    return (posts, updated_posts_metadata)
