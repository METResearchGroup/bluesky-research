"""We want to see how the Google Perspective API classifiers perform on the
data that we previously classified with an in-house moral outrage classifier.

We previously classified comments on Reddit with the in-house classifier. Let's
run the Google Perspective API classifiers on the same data and compare the
results.
"""
from datetime import datetime
import time

import pandas as pd

from services.classify.inference import perform_classification

raw_data_url = "https://raw.githubusercontent.com/mark-torres10/redditResearch/main/src/data/classified_comments/2023-10-16_0427.csv" # noqa
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
export_filename = f"reddit_comments_labeled_with_perspective_classifiers_{current_datetime}.csv" # noqa


def load_previously_classified_comments() -> list[dict]:
    df = pd.read_csv(raw_data_url)
    return df.to_dict(orient="records")


def perform_inference(comments: list[dict]) -> list[dict]:
    labeled_comments = []
    for idx, comment in enumerate(comments):
        if idx % 30 == 0:
            print(f"Processed {idx} posts...")
        if idx >= 30:
            break
        processed_comment = {**perform_classification(comment["body"])}
        comment_plus_labels = {**comment, **processed_comment}
        labeled_comments.append(comment_plus_labels)
        # Wait for 1.1 seconds before making the next request, due to rate
        # limit of 60 queries per minute for Perspective API
        time.sleep(1.2)
    return labeled_comments


def main() -> None:
    comments = load_previously_classified_comments()
    labeled_comments = perform_inference(comments)
    # export comments
    df = pd.DataFrame(labeled_comments)
    df.to_csv(export_filename, index=False)
    print(f"Exported {len(labeled_comments)} labeled comments.")

if __name__ == "__main__":
    main()
