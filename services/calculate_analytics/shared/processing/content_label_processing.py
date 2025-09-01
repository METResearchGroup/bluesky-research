"""Processes post labels for downstream processing."""

from typing import Iterable, Literal, Optional

import numpy as np

from services.calculate_analytics.shared.processing.constants import LABELS_CONFIG


def _init_labels_collection() -> dict[str, list]:
    """Returns a dict of labels that we're aggregating across posts.

    We have a bunch of posts, and we want to collect all of their labels, so
    we can aggregate across them. For example, if we want to calculate the
    average toxicity score of all posts liked by a user, we first need to
    collect all of the toxicity scores of all the posts liked by the user.

    This function returns a dict where the key is the label name (e.g., prob_toxicity)
    and the value is an empty list.
    """
    return {key: [] for key in LABELS_CONFIG.keys()}


def collect_labels_for_post_uris(
    post_uris: list[str], labels_for_content: dict[str, dict]
) -> dict[str, list]:
    """For the post URIs, collect their labels.

    We have a bunch of posts, and we want to collect all of their labels, so
    we can aggregate across them. For example, if we want to calculate the
    average toxicity score of all posts liked by a user, we first need to
    collect all of the toxicity scores of all the posts liked by the user.

    Here, we go through each post URI and collect its labels, and store them
    in a dictionary whose key is the label name (e.g., prob_toxicity) and
    the value is a list of the values of that label for all the posts.

    Now that we have this, we can aggregate across each label to calculate,
    for example, the average toxicity score of all posts liked by a user, since
    we'll have a list of all the toxicity scores of all the posts liked by the user.
    """
    aggregated_label_to_label_values: dict[str, list] = _init_labels_collection()

    for post_uri in post_uris:
        labels_for_post_uri = labels_for_content[post_uri]
        for label, value in labels_for_post_uri.items():
            if value is not None:
                aggregated_label_to_label_values[label].append(value)

    return aggregated_label_to_label_values


# NOTE: yes, _calculate_average_for_{type} is the same in practice for all
# three cases, but I'm leaving it here for readability and to note that making
# it the same for all 3 was an intentional design choice.
def _calculate_average_for_probability_label(label_values: list[float]):
    """Calculates the average for a label that is of type 'probability'.

    For these, we just take the mean of the label values.
    """
    return round(np.mean(label_values), 3) if len(label_values) > 0 else None


def _calculate_average_for_score_label(label_values: list[float]):
    """Calculates the average for a label that is of type 'score'.

    For these, we just take the mean of the label values.
    """
    return round(np.mean(label_values), 3) if len(label_values) > 0 else None


def _calculate_average_for_boolean_label(label_values: list[bool]):
    """Calculates the average for a label that is of type 'boolean'.

    For these, we just take the mean of the label values, setting True = 1
    and False = 0.
    """
    return round(np.mean(label_values), 3) if len(label_values) > 0 else None


def calculate_average_for_label(label: str, label_values: list):
    """Calculates the average for a label that is of type 'probability' or 'score'.

    For these, we just take the mean of the label values.
    """
    config = LABELS_CONFIG[label]
    if config["type"] == "probability":
        return _calculate_average_for_probability_label(label_values=label_values)
    elif config["type"] == "score":
        return _calculate_average_for_score_label(label_values=label_values)
    elif config["type"] == "boolean":
        return _calculate_average_for_boolean_label(label_values=label_values)


def calculate_averages_for_content_labels(
    aggregated_label_to_label_values: dict[str, list],
):
    return {
        label: calculate_average_for_label(label, label_values=label_values)
        for label, label_values in aggregated_label_to_label_values.items()
    }


def _calculate_proportion_for_probability_label(label_values: list, threshold: float):
    """Calculates the proportions for a label that is of type 'probability'.

    For these, we figure out the proportion of the labels that are above a certain
    threshold.
    """
    if len(label_values) == 0:
        return None

    proportion = np.mean(np.array(label_values) >= threshold)
    return round(proportion, 3)


# NOTE: do I really need this? Not really...? Will have to think about if I want to include.
# NOTE: just keeping this here for a consistent interface and so that I don't have
# to add filtering elsewhere. But I'll note that this will always be None.
def _calculate_proportion_for_score_label(label_values: list):
    """Calculates the proportions for a label that is of type 'score'.

    For this, currently there is no "proportion" to calculate, since this is just
    a raw score and the actual label we care about is the label provided
    (e.g., in this case, we can't interpret the valence_clf_score directly
    but we can interpret the valence_label).

    We just have this here for a consistent interface, as we will need an
    average score for valence_clf_score.
    """
    return None


def _calculate_proportion_for_boolean_label(label_values: list):
    """Calculates the proportions for a label that is of type 'boolean'.

    For these, we figure out the proportion of the labels that are True.
    """
    return round(np.mean(label_values), 3) if len(label_values) > 0 else None


def calculate_proportion_for_label(label: str, label_values: list):
    config = LABELS_CONFIG[label]
    if config["type"] == "probability":
        return _calculate_proportion_for_probability_label(
            label_values=label_values, threshold=config["threshold"]
        )
    elif config["type"] == "score":
        return _calculate_proportion_for_score_label(label_values=label_values)
    elif config["type"] == "boolean":
        return _calculate_proportion_for_boolean_label(label_values=label_values)
    else:
        raise ValueError(f"Unknown label type: {config['type']}")


def calculate_proportions_for_content_labels(
    aggregated_label_to_label_values: dict[str, list],
):
    return {
        label: calculate_proportion_for_label(label, label_values)
        for label, label_values in aggregated_label_to_label_values.items()
    }


def transform_metric_field_names(
    metrics: dict[str, Optional[float]],
    metric_type: Literal["average", "proportion"],
    interaction_type: Literal["engagement", "feed"],
    args: dict,
) -> dict[str, Optional[float]]:
    """Transforms the metric field names to be more readable, by adding the
    correct prefixes for the metric type and interaction type.

    For example, if we want to calculate the metrics for posts that users
    engaged with, we would use the interaction type "engagement".

    Goes through the metrics dict and adds new keys with the correct names
    and then removes the old keys.

    For interaction type "feed":
    e.g.,
        - feed_average_toxic
        - feed_proportion_is_sociopolitical
        - feed_average_is_valence_positive
        - feed_proportion_is_valence_positive
        etc.

    For interaction type "engagement":
    e.g.,
        - average_liked_posts_toxic
        - proportion_replied_posts_is_sociopolitical
        - average_reposted_posts_severe_toxic
        - proportion_liked_posts_is_valence_positive
    etc.
    """
    result: dict[str, Optional[float]] = {}
    if interaction_type == "feed":
        for label, value in metrics.items():
            label = label.replace("prob_", "")
            new_label = f"feed_{metric_type}_{label}"
            result[new_label] = value

    elif interaction_type == "engagement":
        record_type = args["record_type"]
        record_type_suffix = (
            "liked_posts"
            if record_type == "like"
            else "posted_posts"
            if record_type == "post"
            else "reposted_posts"
            if record_type == "repost"
            else "replied_posts"
            if record_type == "reply"
            else None
        )
        if record_type_suffix is None:
            raise ValueError(f"Invalid record type: {record_type}")
        prefix = f"engagement_{metric_type}_{record_type_suffix}"  # e.g., average_liked_posts, proportion_replied_posts, etc.
        for label, value in metrics.items():
            label = label.replace("prob_", "")
            new_label = f"{prefix}_{label}"  # e.g., average_liked_posts_prob_toxic, proportion_replied_posts_prob_sociopolitical, etc.
            result[new_label] = value
    else:
        raise ValueError(f"Invalid interaction type: {interaction_type}")

    return result


def calculate_metrics_for_content_labels(
    record_type: Optional[str],
    interaction_type: Literal["engagement", "feed"],
    aggregated_label_to_label_values: dict[str, list],
):
    """Calculate the metrics for the content labels.

    Returns a dict of metrics, with fields like:
        - average_liked_posts_prob_toxic
        - proportion_replied_posts_prob_sociopolitical
        - average_reposted_posts_prob_severe_toxic
        - proportion_liked_posts_is_valence_positive
        etc.
    Where the prefix is the record type (e.g., liked_posts, reposted_posts, etc.)
    and the metric type (average, proportion) and the label is the label name
    (toxicity, sociopolitical, etc.).
    """
    average_metrics: dict[str, Optional[float]] = calculate_averages_for_content_labels(
        aggregated_label_to_label_values=aggregated_label_to_label_values
    )
    transformed_average_metrics: dict[str, Optional[float]] = (
        transform_metric_field_names(
            metrics=average_metrics,
            metric_type="average",
            interaction_type=interaction_type,
            args={"record_type": record_type},
        )
    )
    proportion_metrics: dict[str, Optional[float]] = (
        calculate_proportions_for_content_labels(
            aggregated_label_to_label_values=aggregated_label_to_label_values
        )
    )
    transformed_proportion_metrics: dict[str, Optional[float]] = (
        transform_metric_field_names(
            metrics=proportion_metrics,
            metric_type="proportion",
            interaction_type=interaction_type,
            args={"record_type": record_type},
        )
    )
    return {**transformed_average_metrics, **transformed_proportion_metrics}


def get_metrics_for_user_feeds_from_partition_date(
    post_uris: Iterable[str],
    labels_for_feed_content: dict[str, dict],
):
    """Get the metrics for the feed content shown to a given user on a given date."""
    # get aggregated list of labels for the posts, keyed on the label name.
    # for example, we have a key for "prob_toxic" and a value that is a list of
    # all the "prob_toxic" scores for the posts.
    aggregated_label_to_label_values: dict[str, list] = collect_labels_for_post_uris(
        post_uris=post_uris, labels_for_content=labels_for_feed_content
    )

    # given the aggregated list of labels, we can now calculate the metrics
    # for the feed content.
    metrics: dict[str, Optional[float]] = calculate_metrics_for_content_labels(
        record_type=None,
        interaction_type="feed",
        aggregated_label_to_label_values=aggregated_label_to_label_values,
    )

    return metrics


def get_metrics_for_record_type(
    record_type: str,
    user_to_content_engaged_with,
    labels_for_engaged_content,
    did: str,
    partition_date: str,
) -> dict[str, Optional[float]]:
    post_uris: list[str] = user_to_content_engaged_with[did][partition_date][
        record_type
    ]

    # get aggregated list of labels for the posts, keyed on the label name.
    # for example, we have a key for "prob_toxic" and a value that is a list of
    # all the "prob_toxic" scores for the posts.
    aggregated_label_to_label_values: dict[str, list] = collect_labels_for_post_uris(
        post_uris=post_uris, labels_for_content=labels_for_engaged_content
    )

    # given the aggregated list of labels, we can now calculate the metrics
    # for the record type.
    metrics: dict[str, Optional[float]] = calculate_metrics_for_content_labels(
        record_type=record_type,
        interaction_type="engagement",
        aggregated_label_to_label_values=aggregated_label_to_label_values,
    )
    return metrics


def get_metrics_for_record_types(
    record_types: list[str],
    user_to_content_engaged_with,
    labels_for_engaged_content,
    did: str,
    partition_date: str,
) -> dict[str, dict[str, Optional[float]]]:
    return {
        record_type: get_metrics_for_record_type(
            record_type,
            user_to_content_engaged_with,
            labels_for_engaged_content,
            did,
            partition_date,
        )
        for record_type in record_types
    }


def flatten_content_metrics_across_record_types(
    record_type_to_metrics_map: dict[str, dict[str, Optional[float]]],
) -> dict[str, Optional[float]]:
    """Flattens the metrics across record types, to build a single dictionary
    of metrics for this user and for this date.
    """
    full_metrics = {}
    for metrics in record_type_to_metrics_map.values():
        full_metrics.update(metrics)
    return full_metrics
