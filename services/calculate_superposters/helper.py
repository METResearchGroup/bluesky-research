"""Helper functions for calculating superposters."""
import ast
from datetime import datetime, timedelta, timezone
from typing import Optional

from lib.constants import current_datetime_str
from lib.db.sql.preprocessing_database import FilteredPreprocessedPosts
from lib.db.sql.superposter_database import batch_insert_superposters
from services.calculate_superposters.models import SuperposterModel
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

superposter_threshold = 5


# set default_superposter_date as yesterday's date, in YYYY-MM-DD format
default_superposter_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")  # noqa


def calculate_day_before_day_after_superposter_date(
    superposter_date: str
) -> tuple[str, str]:
    """Calculate the day before and day after a given superposter_date."""
    day_before = (datetime.strptime(superposter_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")  # noqa
    day_after = (datetime.strptime(superposter_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")  # noqa
    return day_before, day_after


def load_posts_from_superposter_date(superposter_date: str) -> list[FilteredPreprocessedPostModel]:  # noqa
    """Load preprocessed posts from a given superposter_date."""
    # load preprocessed posts whose synctimestamp is > day_before
    # and < day_after
    day_before, day_after = calculate_day_before_day_after_superposter_date(superposter_date)  # noqa
    query = FilteredPreprocessedPosts.select().where(
        FilteredPreprocessedPosts.synctimestamp > day_before,
        FilteredPreprocessedPosts.synctimestamp < day_after
    )
    res = list(query)
    output: list[FilteredPreprocessedPostModel] = []
    for post in res:
        # synctimestamp is a string, but has to be in YYYY-MM-DD format
        formatted_date = "-".join(post.synctimestamp.split("-")[:3])
        if formatted_date == superposter_date:
            post_dict = post.__dict__["__data__"]
            formatted_post = FilteredPreprocessedPostModel(
                uri=post_dict["uri"],
                cid=post_dict["cid"],
                indexed_at=post_dict["indexed_at"],
                author=ast.literal_eval(post_dict["author"]),
                metadata=ast.literal_eval(post_dict["metadata"]),
                record=ast.literal_eval(post_dict["record"]),
                metrics=ast.literal_eval(post_dict["metrics"]),
                passed_filters=post_dict["passed_filters"],
                filtered_at=post_dict["filtered_at"],
                filtered_by_func=post_dict["filtered_by_func"],
                synctimestamp=post_dict["synctimestamp"],
                preprocessing_timestamp=post_dict["preprocessing_timestamp"]  # noqa
            )
            output.append(formatted_post)
    return output


def get_counts_of_posts_by_author_id(posts: list[FilteredPreprocessedPostModel]) -> dict:  # noqa
    """Get the counts of posts by user ID."""
    author_did_to_post_count: dict = {}
    for post in posts:
        author_did = post.author.did
        author_handle = post.author.handle
        if author_did in author_did_to_post_count:
            author_did_to_post_count[author_did]["count"] += 1
        else:
            author_did_to_post_count[author_did] = {
                "author_did": author_did,
                "author_handle": author_handle,
                "count": 1
            }
    return author_did_to_post_count


def get_superposter_users_with_counts(user_post_counts: dict) -> list[dict]:
    """Get superposter users with counts."""
    superposter_users_with_counts_list: list[dict] = []
    for author_did, user_count_obj in user_post_counts.items():
        number_of_posts = user_count_obj["count"]
        if number_of_posts >= superposter_threshold:
            superposter_users_with_counts_list.append(
                {
                    "user_did": author_did,
                    "user_handle": user_count_obj["author_handle"],
                    "number_of_posts": number_of_posts
                }
            )
    return superposter_users_with_counts_list


def calculate_superposters(
    superposter_date: Optional[str] = default_superposter_date
) -> list[SuperposterModel]:
    """Calculate superposters for a given date."""
    posts: list[FilteredPreprocessedPostModel] = load_posts_from_superposter_date(  # noqa
        superposter_date=superposter_date
    )
    user_post_counts: dict = get_counts_of_posts_by_author_id(posts)
    superposter_users_with_counts: list[dict] = (
        get_superposter_users_with_counts(user_post_counts=user_post_counts)
    )
    res: list[SuperposterModel] = []
    for superposter_dict in superposter_users_with_counts:
        superposter = SuperposterModel(
            user_did=superposter_dict["user_did"],
            user_handle=superposter_dict["user_handle"],
            number_of_posts=superposter_dict["number_of_posts"],
            superposter_date=superposter_date,
            insert_timestamp=current_datetime_str,
        )
        res.append(superposter)
    return res


def calculate_latest_superposters(
    superposter_date: Optional[str] = default_superposter_date
):
    """Calculate superposters for the latest date."""
    superposters: list[SuperposterModel] = calculate_superposters(
        superposter_date=superposter_date
    )
    batch_insert_superposters(superposters=superposters)
