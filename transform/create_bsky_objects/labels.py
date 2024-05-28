"""Create Bluesky classes from dict."""
from atproto_client.models.com.atproto.label.defs import SelfLabel, SelfLabels


def create_label(label_dict: dict) -> SelfLabel:
    return SelfLabel(val=label_dict["val"])


def create_labels(labels_dict: dict) -> SelfLabels:
    labels = [
        create_label(label_dict) for label_dict in labels_dict["values"]
    ]
    return SelfLabels(values=labels)
