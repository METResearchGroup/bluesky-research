"""Helper functionalities for getting streamed data."""
from typing import Optional
import pandas as pd

from services.sync.stream.database import FirehosePost


def get_posts(k: Optional[int]=None) -> list[FirehosePost]:
    """Get all posts from the database."""
    if k:
        return list(FirehosePost.select().limit(k))
    return list(FirehosePost.select())


def get_most_recent_posts(k: Optional[int]=None) -> list[FirehosePost]:
    """Get the most recent posts from the database."""
    if k:
        return list(
            FirehosePost
            .select()
            .order_by(FirehosePost.synctimestamp.desc())
            .limit(k)
        )
    return list(
        FirehosePost
        .select()
        .order_by(FirehosePost.synctimestamp.desc())
    )


def get_posts_as_list_dicts(
    k: Optional[int]=None,
    order_by: Optional[str]="synctimestamp",
    desc: Optional[bool]=True
) -> list[dict]:
    """Get all posts from the database as a list of dictionaries."""
    if order_by:
        if desc:
            posts = (
                FirehosePost
                .select()
                .order_by(getattr(FirehosePost, order_by).desc())
                .limit(k)
            )
        else:
            posts = (
                FirehosePost
                .select()
                .order_by(getattr(FirehosePost, order_by))
                .limit(k)
            )
    else:
        posts = get_posts(k=k)
    return [post.__dict__['__data__'] for post in posts]


def get_posts_as_df(k: Optional[int]=None) -> pd.DataFrame:
    """Get all posts from the database as a pandas DataFrame."""
    return pd.DataFrame(get_posts_as_list_dicts(k=k))


def get_num_posts() -> int:
    """Get the number of posts in the database."""
    return FirehosePost.select().count()


if __name__ == "__main__":
    num_posts = get_num_posts()
    print(f"Total number of posts: {num_posts}")
