"""Database for tracking user engagement."""
import os
import peewee
import sqlite3
from typing import Optional

from lib.helper import create_batches
from services.update_user_bluesky_engagement.models import (
    PostWrittenByStudyUserModel, UserEngagementMetricsModel, UserLikeModel,
    UserLikedPostModel
)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SYNC_SQLITE_DB_NAME = "user_engagement.db"
SYNC_SQLITE_DB_PATH = os.path.join(current_file_directory, SYNC_SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SYNC_SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SYNC_SQLITE_DB_PATH)
cursor = conn.cursor()

insert_batch_size = 100


class BaseModel(peewee.Model):
    class Meta:
        database = db


class UserLike(BaseModel):
    """Class for tracking user likes."""
    created_at = peewee.CharField()
    author_did = peewee.CharField()
    author_handle = peewee.CharField()
    liked_by_user_did = peewee.CharField()
    liked_by_user_handle = peewee.CharField()
    uri = peewee.CharField()
    cid = peewee.CharField()
    like_synctimestamp = peewee.CharField()


class UserLikedPost(BaseModel):
    """Class for tracking user liked posts."""
    uri = peewee.CharField()
    cid = peewee.CharField()
    url = peewee.CharField()
    source_feed = peewee.CharField()
    synctimestamp = peewee.CharField()
    created_at = peewee.CharField()
    text = peewee.TextField()
    embed = peewee.TextField(null=True)
    entities = peewee.TextField(null=True)
    facets = peewee.TextField(null=True)
    labels = peewee.TextField(null=True)
    langs = peewee.TextField(null=True)
    reply_parent = peewee.TextField(null=True)
    reply_root = peewee.TextField(null=True)
    tags = peewee.TextField(null=True)


class PostsWrittenByStudyUsers(BaseModel):
    """Class for the posts written by study users."""
    uri = peewee.CharField(unique=True)
    cid = peewee.CharField(index=True)
    indexed_at = peewee.CharField()
    created_at = peewee.CharField()
    author_did = peewee.CharField()
    author_handle = peewee.CharField()
    record = peewee.TextField()
    text = peewee.TextField()
    synctimestamp = peewee.CharField()
    url = peewee.CharField(null=True)
    like_count = peewee.IntegerField(null=True)
    reply_count = peewee.IntegerField(null=True)
    repost_count = peewee.IntegerField(null=True)
    post_type = peewee.CharField()


def create_initial_tables(drop_all_tables: bool = False) -> None:
    """Create the initial tables, optionally dropping all existing tables first.

    :param drop_all_tables: If True, drops all existing tables before creating new ones.
    """  # noqa
    with db.atomic():
        if drop_all_tables:
            print("Dropping all tables in database...")
            db.drop_tables([PostsWrittenByStudyUsers], safe=True)
        print("Creating tables in database...")
        db.create_tables([PostsWrittenByStudyUsers])


def get_most_recent_post_timestamp(author_handle: str) -> Optional[str]:
    """Get the most recent post timestamp for a given author handle."""
    query = f"""
    SELECT created_at
    FROM PostsWrittenByStudyUsers
    WHERE author_handle = '{author_handle}'
    ORDER BY created_at DESC
    LIMIT 1;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


def get_most_recent_like_timestamp(author_handle: str) -> Optional[str]:
    """Get the most recent like timestamp for a given author handle."""
    query = f"""
    SELECT created_at
    FROM UserLike
    WHERE author_handle = '{author_handle}'
    ORDER BY created_at DESC
    LIMIT 1;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


def batch_insert_likes(likes: list[UserLikeModel]):
    batches = create_batches(likes, insert_batch_size)
    for batch in batches:
        batch_dicts = [like.dict() for like in batch]
        UserLike.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(likes)} likes into the database.")


def get_previously_liked_post_uris() -> set[str]:
    """Get previously liked post URIs."""
    previous_uris = UserLikedPost.select(UserLikedPost.uri)
    return set([p.uri for p in previous_uris])


def batch_insert_liked_posts(liked_posts: list[UserLikedPostModel]):  # noqa
    """Batch insert liked posts into the database.

    Dedupes posts to make sure that we don't insert any duplicates.
    """
    existing_liked_post_uris: set[str] = get_previously_liked_post_uris()
    seen_uris = set()
    seen_uris.update(existing_liked_post_uris)
    unique_posts = []
    for post in liked_posts:
        if post.uri not in seen_uris:
            seen_uris.add(post.uri)
            unique_posts.append(post)
    batches = create_batches(unique_posts, insert_batch_size)
    for batch in batches:
        batch_dicts = [post.dict() for post in batch]
        UserLikedPost.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(liked_posts)} liked posts into the database.")  # noqa


def batch_insert_posts_written(posts_written: list[PostWrittenByStudyUserModel]):  # noqa
    batches = create_batches(posts_written, insert_batch_size)
    for batch in batches:
        batch_dicts = [post.dict() for post in batch]
        PostsWrittenByStudyUsers.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(posts_written)} posts written into the database.")  # noqa


def batch_insert_engagement_metrics(
    latest_engagement_metrics: list[UserEngagementMetricsModel]
):
    """Batch insert the latest user engagement metrics for a given user."""
    with db.atomic():
        for user_engagement_metrics in latest_engagement_metrics:
            batch_insert_posts_written(user_engagement_metrics.latest_posts_written)  # noqa
            batch_insert_likes(user_engagement_metrics.latest_likes)
            batch_insert_liked_posts(user_engagement_metrics.latest_liked_posts)  # noqa
            new_posts_written = len(user_engagement_metrics.latest_posts_written)  # noqa
            new_likes = len(user_engagement_metrics.latest_likes)
            new_liked_posts = len(user_engagement_metrics.latest_liked_posts)
            print(f"Finished inserting engagement metrics for user {user_engagement_metrics.user_handle}.")  # noqa
            print(f"Inserted {new_posts_written} new posts written, {new_likes} new likes, and {new_liked_posts} new liked posts.")  # noqa
        print(f"Finished inserting engagement metrics for {len(latest_engagement_metrics)} users.")  # noqa


if __name__ == "__main__":
    create_initial_tables(drop_all_tables=True)
