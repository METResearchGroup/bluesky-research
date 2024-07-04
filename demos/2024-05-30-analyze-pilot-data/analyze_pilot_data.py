"""Analyze pilot data."""
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lib.db.sql.ml_inference_database import (get_metadata,
                                              get_perspective_api_labels)
from services.ml_inference.models import (PerspectiveApiLabelsModel,
                                          RecordClassificationMetadataModel)

n_feed_posts = 250


def load_data():
    metadata: list[RecordClassificationMetadataModel] = get_metadata()
    perspective_api_labels: list[PerspectiveApiLabelsModel] = (
        get_perspective_api_labels()
    )
    return (metadata, perspective_api_labels)


def join_metadata_to_labels(metadata, perspective_api_labels):
    metadata_dicts = [m.dict() for m in metadata]
    perspective_api_labels_dicts = [p.dict() for p in perspective_api_labels]
    metadata_df = pd.DataFrame(metadata_dicts)
    perspective_api_labels_df = pd.DataFrame(perspective_api_labels_dicts)
    merged_df = metadata_df.merge(
        perspective_api_labels_df, how="inner", on="uri"
    )
    return merged_df


def plot_probs(
    firehose_probs,
    most_liked_feed_probs,
    plot_title,
    image_filepath,
    treatment_probs=None
):
    """Plot the probabilities as a pair of side-by-side histograms.

    Export as .png files.
    """
    firehose_plot = sns.kdeplot(
        firehose_probs,
        label="Firehose feed",
        cumulative=False,
        bw_adjust=0.25
    )
    most_liked_plot = sns.kdeplot(
        most_liked_feed_probs,
        label="Engagement feed",
        cumulative=False,
        bw_adjust=0.25
    )
    if treatment_probs is not None:
        treatment_plot = sns.kdeplot(
            treatment_probs,
            label="Treatment feed",
            cumulative=False,
            bw_adjust=0.25
        )

    plt.draw()
    plt.hist(
        firehose_probs, bins=20, alpha=0.5, density=True,
        label="Firehose feed"
    )
    plt.hist(
        most_liked_feed_probs, bins=20, alpha=0.5, density=True,
        label="Engagement feed"
    )
    firehose_color = firehose_plot.get_lines()[-1].get_c()
    most_liked_color = most_liked_plot.get_lines()[-1].get_c()
    firehose_color = 'blue'
    most_liked_color = 'orange'

    plt.axvline(
        np.mean(firehose_probs),
        color=firehose_color,
        linestyle='dashed',
        linewidth=1
    )
    plt.text(
        np.mean(firehose_probs),
        plt.gca().get_ylim()[1]/2,
        'Mean (Firehose)',
        rotation=90,
        fontsize=0.75*plt.rcParams['font.size']
    )

    plt.axvline(
        np.mean(most_liked_feed_probs),
        color=most_liked_color,
        linestyle='dashed',
        linewidth=1
    )
    plt.text(
        np.mean(most_liked_feed_probs),
        plt.gca().get_ylim()[1]/2,
        'Mean (Engagement)',
        rotation=90,
        fontsize=0.75*plt.rcParams['font.size']
    )

    if treatment_probs is not None:
        plt.axvline(
            np.mean(treatment_probs),
            color='green',
            linestyle='dashed',
            linewidth=1
        )
        plt.text(
            np.mean(treatment_probs),
            plt.gca().get_ylim()[1]/2,
            'Mean (Treatment)',
            rotation=90,
            fontsize=0.75*plt.rcParams['font.size']
        )

    plt.legend(loc="upper right")
    plt.xlabel("Probability")
    plt.ylabel("Frequency")
    plt.title(plot_title)
    plt.show()
    plt.savefig(image_filepath)
    plt.clf()


def get_labels(probs, threshold: float = 0.8):
    """Return the labels based on the threshold."""
    labels = []
    for prob in probs:
        if prob >= threshold:
            labels.append(1)
        else:
            labels.append(0)
    return labels


def report_class_probability(
    probs, title: str, task_name: str, threshold: float = 0.8
) -> float:
    """Print the class probability"""
    labels = get_labels(probs, threshold)
    num_positive = sum(labels)
    print('-' * 10)
    print(title)
    percentage_positive = num_positive / len(labels) * 100
    rounded_percentage_positive = round(percentage_positive, 8)
    print(f"Percentage of {task_name} labels: {rounded_percentage_positive}%")
    print(f"Threshold: {threshold}")
    print('-' * 10)
    return rounded_percentage_positive


def calculate_and_report_class_probs(
    firehose_toxicity_probs,
    most_liked_toxicity_probs,
    firehose_constructiveness_probs,
    most_liked_constructiveness_probs
):
    # think 0.8 is what's recommended by Jigsaw, need to find reference.
    for threshold in [0.5, 0.7, 0.8, 0.9, 0.95]:
        print('-' * 20)
        try:
            firehose_toxicity_class_prob = report_class_probability(
                probs=firehose_toxicity_probs,
                title="Toxicity probabilities for firehose",
                task_name="toxicity",
                threshold=threshold
            )

            most_liked_toxicity_class_prob = report_class_probability(
                probs=most_liked_toxicity_probs,
                title="Toxicity probabilities for most liked feed",
                task_name="toxicity",
                threshold=threshold
            )

            percent_diff_toxicity = round(
                (most_liked_toxicity_class_prob - firehose_toxicity_class_prob)
                / firehose_toxicity_class_prob * 100, 4
            )
            print(f"Percent difference in toxicity in engagement condition: {percent_diff_toxicity}%")  # noqa

            firehose_constructiveness_class_prob = report_class_probability(
                probs=firehose_constructiveness_probs,
                title="Constructiveness probabilities for firehose",
                task_name="constructiveness",
                threshold=threshold
            )

            most_liked_constructiveness_class_prob = report_class_probability(
                probs=most_liked_constructiveness_probs,
                title="Constructiveness probabilities for most liked feed",
                task_name="constructiveness",
                threshold=threshold
            )

            percent_diff_constructiveness = round(
                (most_liked_constructiveness_class_prob -
                 firehose_constructiveness_class_prob)
                / firehose_constructiveness_class_prob * 100, 4
            )
            print(
                f"Percent difference in constructiveness in engagement condition: {percent_diff_constructiveness}%")
        except ZeroDivisionError:
            print("No samples with probability above threshold.")
            continue


def calculate_treatment_score(
    likes: int,
    prob_toxicity: float,
    prob_constructiveness: float,
    coef_toxicity: float,
    coef_constructiveness: float
):
    """Calculate the treatment score.

    Takes the original engagement score (here, likes) and applies a
    treatment score based on the toxicity and constructiveness.

    We use like_count for now, but we can use a weighted engagement score
    at some point where we differentially weigh things like likes, replies,
    and reposts, into a total "engagement" score.

    We divide by the sum of the probabilities to normalize the coefficients.
    Particularly important given how small some of the probabilities can be.
    """
    coef = (
        prob_toxicity * coef_toxicity
        + prob_constructiveness * coef_constructiveness
    ) / (prob_toxicity + prob_constructiveness)
    # preferentially outweighs posts with lots of likes, which isn't ideal.
    # return likes + (coef * likes)
    # return (coef * likes)

    # np.log to reduce the impact of the likes on the treatment score.
    return (coef * np.log(likes + 1))


def main():
    # metadata, perspective_api_labels = load_data()
    # merged_df = join_metadata_to_labels(metadata, perspective_api_labels)
    # firehose_df = merged_df[merged_df["source"] == "firehose"]
    # most_liked_feed_df = merged_df[merged_df["source"] == "most_liked"]
    # firehose_df.to_csv("perspective_labels_firehose.csv")
    # most_liked_feed_df.to_csv("perspective_labels_most_liked_feed.csv")
    firehose_df = pd.read_csv("perspective_labels_firehose.csv")
    most_liked_feed_df = pd.read_csv("perspective_labels_most_liked_feed.csv")

    # sort most_liked_feed_df by like_count, take the top `n_feed_posts` rows.
    most_liked_feed_df = most_liked_feed_df.sort_values(
        by="like_count", ascending=False
    )
    most_liked_feed_df = most_liked_feed_df.head(n_feed_posts)

    firehose_toxicity_probs = firehose_df["prob_toxic"].dropna()
    most_liked_toxicity_probs = most_liked_feed_df["prob_toxic"].dropna()
    average_firehose_toxicity_probs = np.mean(firehose_toxicity_probs)
    average_most_liked_toxicity_probs = np.mean(most_liked_toxicity_probs)
    print(f"Average toxicity probability for firehose: {average_firehose_toxicity_probs}")  # noqa
    print(f"Average toxicity probability for most liked feed: {average_most_liked_toxicity_probs}")  # noqa

    firehose_constructiveness_probs = firehose_df["prob_constructive"].dropna()
    most_liked_constructiveness_probs = most_liked_feed_df["prob_constructive"].dropna()  # noqa
    average_firehose_constructiveness_probs = np.mean(firehose_constructiveness_probs)  # noqa
    average_most_liked_constructiveness_probs = np.mean(most_liked_constructiveness_probs)  # noqa
    print(f"Average constructiveness probability for firehose: {average_firehose_constructiveness_probs}")  # noqa
    print(f"Average constructiveness probability for most liked feed: {average_most_liked_constructiveness_probs}")  # noqa

    min_timestamp_firehose = firehose_df['synctimestamp'].dropna().min()
    max_timestamp_firehose = firehose_df['synctimestamp'].dropna().max()

    min_timestamp_most_liked = most_liked_feed_df['synctimestamp'].dropna(
    ).min()
    max_timestamp_most_liked = most_liked_feed_df['synctimestamp'].dropna(
    ).max()

    print(f"Firehose feed: {min_timestamp_firehose} to {max_timestamp_firehose}")  # noqa
    print(f"Most liked feed: {min_timestamp_most_liked} to {max_timestamp_most_liked}")  # noqa

    # plot_probs(
    #     firehose_probs=firehose_toxicity_probs,
    #     most_liked_feed_probs=most_liked_toxicity_probs,
    #     plot_title="Density plot of toxicity probabilities by condition",
    #     image_filepath="toxicity_probs.png"
    # )

    # plot_probs(
    #     firehose_probs=firehose_constructiveness_probs,
    #     most_liked_feed_probs=most_liked_constructiveness_probs,
    #     plot_title="Density plot of constructiveness probabilities by condition",
    #     image_filepath="constructiveness_probs.png"
    # )

    calculate_and_report_class_probs(
        firehose_toxicity_probs,
        most_liked_toxicity_probs,
        firehose_constructiveness_probs,
        most_liked_constructiveness_probs
    )

    breakpoint()

    # now, need to calculate scores for each treatment and apply
    # to most-liked feed to re-sort it.
    coef_toxicity = 0.94
    coef_constructiveness = 1

    # re-load most_liked_feed_df since upstream, we filtered the original df
    # down to the top `n_feed_posts`. We want the original pool of most liked
    # posts, but we want to be able to sort them by treatment score and then
    # take the top `n_feed_posts`.
    most_liked_feed_df = pd.read_csv("perspective_labels_most_liked_feed.csv")
    most_liked_feed_subset_probs_df = most_liked_feed_df[
        ["prob_toxic", "prob_constructive", "like_count"]
    ]  # noqa

    most_liked_feed_subset_probs_df = most_liked_feed_subset_probs_df.dropna()  # noqa

    most_liked_feed_subset_probs_df["treatment_score"] = most_liked_feed_subset_probs_df.apply(  # noqa
        lambda x: calculate_treatment_score(
            likes=x["like_count"],
            prob_toxicity=x["prob_toxic"],
            prob_constructiveness=x["prob_constructive"],
            coef_toxicity=coef_toxicity,
            coef_constructiveness=coef_constructiveness
        ),
        axis=1
    )

    treatment_posts = most_liked_feed_subset_probs_df.nlargest(n_feed_posts, "treatment_score")  # noqa
    treatment_posts = treatment_posts.sort_values(by="treatment_score", ascending=False)  # noqa

    treatment_average_toxicity = np.mean(treatment_posts["prob_toxic"])  # noqa
    treatment_average_constructiveness = np.mean(treatment_posts["prob_constructive"])  # noqa

    print(f"(Treatment): Average toxicity for top {n_feed_posts} posts: {treatment_average_toxicity}")  # noqa
    print(f"(Treatment): Average constructiveness for top {n_feed_posts} posts in firehose: {treatment_average_constructiveness}")  # noqa

    treatment_toxicity_probs = treatment_posts["prob_toxic"]
    treatment_constructiveness_probs = treatment_posts["prob_constructive"]

    for threshold in [0.5, 0.7, 0.8, 0.9, 0.95]:
        print('-' * 20)
        try:
            treatment_toxicity_class_prob = report_class_probability(
                probs=treatment_toxicity_probs,
                title="Toxicity probabilities for treatment",
                task_name="toxicity",
                threshold=threshold
            )

            treatment_constructiveness_class_prob = report_class_probability(
                probs=treatment_constructiveness_probs,
                title="Constructiveness probabilities for treatment",
                task_name="constructiveness",
                threshold=threshold
            )
        except ZeroDivisionError:
            print("No samples with probability above threshold.")
            continue

    # TODO: export results.
    treatment_posts.to_csv("perspective_api_treatment_posts.csv")

    plot_probs(
        firehose_probs=firehose_toxicity_probs,
        most_liked_feed_probs=most_liked_toxicity_probs,
        treatment_probs=treatment_toxicity_probs,
        plot_title="Density plot of toxicity probabilities by condition",
        image_filepath="toxicity_probs.png"
    )

    plot_probs(
        firehose_probs=firehose_constructiveness_probs,
        most_liked_feed_probs=most_liked_constructiveness_probs,
        treatment_probs=treatment_constructiveness_probs,
        plot_title="Density plot of constructiveness probabilities by condition",
        image_filepath="constructiveness_probs.png"
    )


if __name__ == "__main__":
    main()
