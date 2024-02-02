from datetime import datetime
import os

from services.sync.search.helper import load_author_feeds_from_file

current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
classifications_filename = f"classified_posts_{current_datetime}.jsonl"
current_file_directory = os.path.dirname(os.path.abspath(__file__))
user_profiles_fp = os.path.join(current_file_directory, classifications_filename) # noqa


def load_posts_from_author_feeds() -> list[dict]:
    author_feeds = load_author_feeds_from_file()


def export_classified_posts(classified_posts: list[dict]) -> None:
    pass


def classify_post(post: dict) -> dict:
    return {
        "prob": 0.7,
        "label": 1
    }


def classify_posts(posts: list[dict]) -> list[dict]:
    classified_posts: list[dict] = []
    for post in posts:
        label_dict = classify_post(post)
        classified_posts.append(
            {
                **post,
                **label_dict
            }
        )
    return classified_posts



def main(event: dict, context: dict) -> int:
    """Run analyses"""
    posts: list[dict] = load_posts_from_author_feeds()
    classified_posts: list[dict] = classify_posts(posts)
    export_classified_posts(classified_posts)
    return 0


if __name__ == "__main__":
    event = {}
    context = {}
    main(event=event, context=context)
