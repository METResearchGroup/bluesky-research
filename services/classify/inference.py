"""Inference module for classifying text.

For Google Perspective API, the list of possible attributes to classify is
found at https://developers.perspectiveapi.com/s/about-the-api-attributes-and-languages?language=en_US
(the default is TOXICITY).
""" # noqa
import json
import time

from googleapiclient import discovery

from lib.helper import GOOGLE_API_KEY, logger
from services.transform.transform_raw_data import FlattenedFeedViewPost

google_client = discovery.build(
  "commentanalyzer",
  "v1alpha1",
  developerKey=GOOGLE_API_KEY,
  discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
  static_discovery=False,
)

default_requested_attributes = {
    "TOXICITY": {},
    # constructive attributes, from Perspective API
    "AFFINITY_EXPERIMENTAL": {},
    "COMPASSION_EXPERIMENTAL": {},
    "CONSTRUCTIVE_EXPERIMENTAL": {},
    "CURIOSITY_EXPERIMENTAL": {},
    "NUANCE_EXPERIMENTAL": {},
    "PERSONAL_STORY_EXPERIMENTAL": {},
    # persuasion attributes, from Perspective API
    "ALIENATION_EXPERIMENTAL": {},
    "FEARMONGERING_EXPERIMENTAL": {},
    "GENERALIZATION_EXPERIMENTAL": {},
    "MORAL_OUTRAGE_EXPERIMENTAL": {},
    "POWER_APPEAL_EXPERIMENTAL": {},
    "SCAPEGOATING_EXPERIMENTAL": {}
}

attribute_to_labels_map = {
    "TOXICITY": {
        "prob": "prob_toxic",
        "label": "label_toxic"
    },
    "AFFINITY_EXPERIMENTAL": {
        "prob": "prob_affinity",
        "label": "label_affinity"
    },
    "COMPASSION_EXPERIMENTAL": {
        "prob": "prob_compassion",
        "label": "label_compassion"
    },
    "CONSTRUCTIVE_EXPERIMENTAL": {
        "prob": "prob_constructive",
        "label": "label_constructive"
    },
    "CURIOSITY_EXPERIMENTAL": {
        "prob": "prob_curiosity",
        "label": "label_curiosity"
    },
    "NUANCE_EXPERIMENTAL": {
        "prob": "prob_nuance",
        "label": "label_nuance"
    },
    "PERSONAL_STORY_EXPERIMENTAL": {
        "prob": "prob_personal_story",
        "label": "label_personal_story"
    },
    "ALIENATION_EXPERIMENTAL": {
        "prob": "prob_alienation",
        "label": "label_alienation"
    },
    "FEARMONGERING_EXPERIMENTAL": {
        "prob": "prob_fearmongering",
        "label": "label_fearmongering"
    },
    "GENERALIZATION_EXPERIMENTAL": {
        "prob": "prob_generalization",
        "label": "label_generalization"
    },
    "MORAL_OUTRAGE_EXPERIMENTAL": {
        "prob": "prob_moral_outrage",
        "label": "label_moral_outrage"
    },
    "POWER_APPEAL_EXPERIMENTAL": {
        "prob": "prob_power_appeal",
        "label": "label_power_appeal"
    },
    "SCAPEGOATING_EXPERIMENTAL": {
        "prob": "prob_scapegoating",
        "label": "label_scapegoating"
    }
}



def request_comment_analyzer(
    text: str, requested_attributes: dict = None
) -> dict:
    """Sends request to commentanalyzer endpoint.
    
    Docs at https://developers.perspectiveapi.com/s/docs-sample-requests?language=en_US

    Example request:

    analyze_request = {
    'comment': { 'text': 'friendly greetings from python' },
    'requestedAttributes': {'TOXICITY': {}}
    }

    response = client.comments().analyze(body=analyze_request).execute()
    print(json.dumps(response, indent=2))
    """
    if not requested_attributes:
        requested_attributes = default_requested_attributes
    analyze_request = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": requested_attributes,
    }
    logger.info(
        f"Sending request to commentanalyzer endpoint with request={analyze_request}...", # noqa
    )
    response = google_client.comments().analyze(body=analyze_request).execute()
    return json.dumps(response)


def classify_text_toxicity(text: str) -> dict:
    """Classify text toxicity."""
    response = request_comment_analyzer(
        text=text, requested_attributes={"TOXICITY": {}}
    )
    response_obj = json.loads(response)
    toxicity_prob_score = (
        response_obj["attributeScores"]["TOXICITY"]["summaryScore"]["value"]
    )

    return {
        "prob_toxic": toxicity_prob_score,
        "label_toxic": 0 if toxicity_prob_score < 0.5 else 1
    }


def classify_text(text: str) -> dict:
    """Classify text using all the attributes from the Google Perspectives API.""" # noqa
    response = request_comment_analyzer(
        text=text, requested_attributes=default_requested_attributes
    )
    response_obj = json.loads(response)
    classification_probs_and_labels = {}
    for attribute, labels in attribute_to_labels_map.items():
        prob_score = (
            response_obj["attributeScores"][attribute]["summaryScore"]["value"]
        )
        classification_probs_and_labels[labels["prob"]] = prob_score
        classification_probs_and_labels[labels["label"]] = 0 if prob_score < 0.5 else 1
    return classification_probs_and_labels


def perform_classification(text: str) -> dict:
    """Perform classifications on text.
    
    Able to eventually support different inference functions.
    """
    classification_functions = [classify_text]
    classification_probs_and_labels = {}
    for inference_func in classification_functions:
        res = inference_func(text)
        classification_probs_and_labels.update(res)

    return classification_probs_and_labels


def perform_inference(flattened_posts: list[FlattenedFeedViewPost]) -> list[dict]:
    print(f"Performing inference on {len(flattened_posts)} posts...")
    processed_posts = []
    for idx, post in enumerate(flattened_posts):
        if idx % 30 == 0:
            print(f"Processed {idx} posts...")
        processed_post = {**post, **perform_classification(post["text"])}
        processed_posts.append(processed_post)
        # Wait for 1.1 seconds before making the next request, due to rate
        # limit of 60 queries per minute for Perspective API
        time.sleep(1.2)
    return processed_posts
