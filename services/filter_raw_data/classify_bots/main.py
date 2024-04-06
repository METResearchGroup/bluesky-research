"""Classifies posts as coming from bot accounts"""
from services.filter_raw_data.classify_bots.helper import (
    classify_if_posts_from_bot_accounts
)

def main(posts: list[dict]) -> list[dict]:
    return classify_if_posts_from_bot_accounts(posts=posts)


if __name__ == "__main__":
    pass
