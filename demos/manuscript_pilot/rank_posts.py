"""Given a .csv of files, rank posts."""

# inspired by https://github.com/twitter/the-algorithm-ml/blob/main/projects/home/recap/README.md # noqa
LIKES_SCORE = 1
RETWEETS_SCORE = 1.5
COMMENTS_SCORE = 2


def score_engagement(post: dict) -> int:
    return (
        LIKES_SCORE * post["Likes"]
        + RETWEETS_SCORE * post["Retweets"]
        + COMMENTS_SCORE * post["Comments"]
    )


def score_post(post: dict):
    pass


def score_posts(posts: list[dict]) -> list[float]:
    pass


def rank_posts(posts: list[dict]) -> list[dict]:
    pass


def main():
    pass


if __name__ == "__main__":
    pass
