"""Helper functionalities for getting streamed data."""
import pandas as pd

from services.sync.stream.database import FirehosePost


def get_all_posts() -> list[FirehosePost]:
    """Get all posts from the database."""
    return list(FirehosePost.select())


def get_all_posts_as_list_dicts() -> list[dict]:
    """Get all posts from the database as a list of dictionaries."""
    return [post.__dict__['__data__'] for post in get_all_posts()]


def get_all_posts_as_df() -> pd.DataFrame:
    """Get all posts from the database as a pandas DataFrame."""
    return pd.DataFrame(get_all_posts_as_list_dicts())


def get_num_posts() -> int:
    """Get the number of posts in the database."""
    return FirehosePost.select().count()


def get_top_k_posts(k: int) -> list[FirehosePost]:
    """Get the top k posts from the database."""
    return list(FirehosePost.select().limit(k))


if __name__ == "__main__":
    num_posts = get_num_posts()
    print(f"Total number of posts: {num_posts}")
