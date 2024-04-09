"""Classifies posts as coming from bot accounts"""
from services.filter_raw_data.classify_bots.helper import (
    classify_if_posts_not_from_bot_accounts
)

def filter_posts_not_written_by_bot(posts: list[dict]) -> list[dict]:
    return classify_if_posts_not_from_bot_accounts(posts=posts)


if __name__ == "__main__":
    filter_posts_not_written_by_bot()
