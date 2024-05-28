"""Get baseline metrics for our data."""
import json

import matplotlib.pyplot as plt

from lib.constants import current_datetime_str
from lib.db.mongodb import get_mongodb_collection, chunk_insert_posts
from lib.db.sql.preprocessing_database import get_filtered_posts
from ml_tooling.perspective_api.model import classify as perspective_api_classify # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

mongo_task = "perspective_api_labels"
mongo_collection = get_mongodb_collection(task=mongo_task)[1]


def load_data() -> dict:
    posts: list[FilteredPreprocessedPostModel] = get_filtered_posts()
    firehose_posts: list[FilteredPreprocessedPostModel] = [
        post for post in posts if post.metadata.source == "firehose"
    ]
    most_liked_posts: list[FilteredPreprocessedPostModel] = [
        post for post in posts if post.metadata.source == "most_liked"
    ]
    return {"firehose": firehose_posts, "most_liked": most_liked_posts}


def get_perspective_api_labels_for_post(
    post: FilteredPreprocessedPostModel, source_feed: str
) -> dict:
    try:
        classifications: dict = perspective_api_classify(text=post.record.text)
    except Exception as e:
        # error might happen if there's no text for the API to classify.
        print(f"Error in classification: {e}")
        breakpoint()
        classifications = None
    finally:
        res = {
            "_id": post.uri,
            "uri": post.uri,
            "text": post.record.text,
            "classifications": classifications,
            "metadata": {
                "source_feed": source_feed,
                "classification_timestamp": current_datetime_str
            }
        }
        return res


def get_perspective_api_labels_for_posts(
    posts: list[FilteredPreprocessedPostModel], source_feed: str
) -> list[dict]:
    return [
        get_perspective_api_labels_for_post(post=post, source_feed=source_feed)
        for post in posts
    ]


def export_perspective_api_labels(
    labels: list[dict],
    source_feed: str,
    store_local: bool = True,
    store_remote: bool = True
):
    num_labels = len(labels)
    if store_local:
        print(f"Exporting {num_labels} perspective API labels to local storage...") # noqa
        export_filename = f"perspective_api_labels_{source_feed}_{current_datetime_str}.jsonl"
        with open(export_filename, "w") as f:
            for label in labels:
                f.write(json.dumps(label) + "\n")
        print(f"Exported {num_labels} perspective API labels to local storage.") # noqa
    if store_remote:
        print("Inserting into MongoDB in bulk...")
        total_successful_inserts, duplicate_key_count = chunk_insert_posts(
            posts=labels,
            mongo_collection=mongo_collection
        )
        print(f"Inserted {total_successful_inserts} perspective API labels into MongoDB.") # noqa
        print(f"Duplicate key count: {duplicate_key_count}")
        print("Finished bulk inserting into MongoDB.")


def plot_perspective_api_labels_histogram(
    perspective_api_labels: list[dict], source_feed: str
) -> None:
    """Plot a histogram of the perspective API labels."""
    # get labels for each of toxicity and constructiveness
    prob_toxic_lst: list[float] = [
        label["classifications"]["prob_toxic"]
        for label in perspective_api_labels
        if label["classifications"] is not None
    ]
    prob_constructiveness_lst: list[float] = [
        label["classifications"]["prob_constructiveness"]
        for label in perspective_api_labels
        if label["classifications"] is not None
    ]
    # create histograms of each
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    axs[0].hist(prob_toxic_lst, bins=20)
    axs[0].set_title("Toxicity")
    axs[0].set_xlabel("Probability")
    axs[0].set_ylabel("Count")
    axs[1].hist(prob_constructiveness_lst, bins=20)
    axs[1].set_title("Constructiveness")
    axs[1].set_xlabel("Probability")
    axs[1].set_ylabel("Count")
    plt.show()

    # export as image
    fig.savefig(f"{source_feed}_perspective_api_labels_histogram.png")


def get_perspective_api_labels_for_source(
    posts: list[FilteredPreprocessedPostModel], source_feed: str
):
    # load existing perspective API labels, if any, and only get perspective
    # API labels for data that we don't have labels for yet.

    # get perspective API labels
    perspective_api_labels: list[dict] = get_perspective_api_labels_for_posts(
        posts=posts, source_feed=source_feed
    )

    # write perspective API labels to local and remote storage
    export_perspective_api_labels(
        labels=perspective_api_labels,
        source_feed=source_feed,
        store_local=True,
        store_remote=True
    )

    # plot perspective API labels as a histogram
    plot_perspective_api_labels_histogram(
        perspective_api_labels=perspective_api_labels,
        source_feed=source_feed
    )



def main():
    # load data
    data_by_source: dict = load_data()
    firehose_posts: list[FilteredPreprocessedPostModel] = data_by_source["firehose"] # noqa
    most_liked_posts: list[FilteredPreprocessedPostModel] = data_by_source["most_liked"] # noqa

    print(f"Number of firehose posts: {len(firehose_posts)}")
    print(f"Number of most liked posts: {len(most_liked_posts)}")

    # NOTE: can comment out any of these to test the functions for a given
    # source separately.
    get_perspective_api_labels_for_source(
        posts=firehose_posts, source_feed="firehose"
    )

    get_perspective_api_labels_for_source(
        posts=most_liked_posts, source_feed="most_liked"
    )

if __name__ == "__main__":
    main()
