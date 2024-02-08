"""We want to see how the Google Perspective API classifiers perform on the
data that we previously classified with an in-house moral outrage classifier.

We previously classified comments on Reddit with the in-house classifier. Let's
run the Google Perspective API classifiers on the same data and compare the
results.

We will classify ~1,000 posts that we've previously classified.
"""
from datetime import datetime
import time

import pandas as pd

from services.classify.inference import perform_classification

raw_data_url = "https://raw.githubusercontent.com/mark-torres10/redditResearch/main/src/data/classified_comments/2023-10-16_0427.csv" # noqa
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
export_filename = f"reddit_comments_labeled_with_perspective_classifiers_{current_datetime}.csv" # noqa

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def load_previously_classified_comments() -> list[dict]:
    df = pd.read_csv(raw_data_url)
    return df.to_dict(orient="records")


def perform_inference(comments: list[dict]) -> list[dict]:
    labeled_comments = []
    for idx, comment in enumerate(comments):
        if idx % 30 == 0:
            print(f"Processed {idx} posts...")
        processed_comment = {**perform_classification(comment["body"])}
        comment_plus_labels = {**comment, **processed_comment}
        labeled_comments.append(comment_plus_labels)
        # Wait for 1.1 seconds before making the next request, due to rate
        # limit of 60 queries per minute for Perspective API
        time.sleep(1.2)
    return labeled_comments


def main(event: dict, context: dict) -> None:
    if event.get("classify_comments", False):
        comments = load_previously_classified_comments()
        labeled_comments = perform_inference(comments)
        # export comments
        df = pd.DataFrame(labeled_comments)
        df.to_csv(export_filename, index=False)
        print(f"Exported {len(labeled_comments)} labeled comments.")
    if event.get("qa_comments", False):
        print("QA-ing comments...")
        df = pd.read_csv("reddit_comments_labeled_with_perspective_classifiers_2024-02-03_12-10-26.csv") # noqa
        print('\n')
        # count of comments with label = 1
        print(f'Count of comments that our moral outrage classifier labeled = 1: {len(df[df["label"] == 1])}') # noqa
        # count of comments that the Google Perspective API labeled = 1
        print(f"Count of comments that Google labeled = 1: {len(df[df['label_moral_outrage'] == 1])}") # noqa
        # get count of comments with label = 0 but label_moral_outrage = 1
        print(f'Count of comments that our moral outrage classifier labeled = 0 but Google labeled = 1: {len(df[(df["label"] == 0) & (df["label_moral_outrage"] == 1)])}') # noqa
        # print examples of comments with label = 0 but label_moral_outrage = 1
        print("Examples of comments that our moral outrage classifier labeled = 0 but Google labeled = 1:") # noqa
        print(df[(df["label"] == 0) & (df["label_moral_outrage"] == 1)]["body"].head()) # noqa
        print('\n')
        # get count of comments with label = 1 but label_moral_outrage = 0
        print(f'Count of comments that our moral outrage classifier labeled = 1 but Google labeled = 0: {len(df[(df["label"] == 1) & (df["label_moral_outrage"] == 0)])}') # noqa
        # print examples of comments with label = 1 but label_moral_outrage = 0
        print("Examples of comments that our moral outrage classifier labeled = 1 but Google labeled = 0:") # noqa
        print(df[(df["label"] == 1) & (df["label_moral_outrage"] == 0)]["body"].head()) # noqa
        print('\n')
        # get count of comments with label = 1 and label_moral_outrage = 1
        print(f'Count of comments that our moral outrage classifier labeled = 1 and Google labeled = 1: {len(df[(df["label"] == 1) & (df["label_moral_outrage"] == 1)])}') # noqa
        # print examples of comments with label = 1 and label_moral_outrage = 1
        print("Examples of comments that our moral outrage classifier labeled = 1 and Google labeled = 1:") # noqa
        print(df[(df["label"] == 1) & (df["label_moral_outrage"] == 1)]["body"].head()) # noqa
        print('\n')
        # get count of comments with label = 0 and label_moral_outrage = 0
        print(f'Count of comments that our moral outrage classifier labeled = 0 and Google labeled = 0: {len(df[(df["label"] == 0) & (df["label_moral_outrage"] == 0)])}') # noqa
        # print examples of comments with label = 0 and label_moral_outrage = 0
        print("Examples of comments that our moral outrage classifier labeled = 0 and Google labeled = 0:") # noqa
        print(df[(df["label"] == 0) & (df["label_moral_outrage"] == 0)]["body"].head()) # noqa


if __name__ == "__main__":
    event = {
        "classify_comments": False,
        "qa_comments": True
    }
    context = {}
    main(event=event, context=context)
