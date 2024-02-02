"""Inference module for classifying text.

For Google Perspective API, the list of possible attributes to classify is
found at https://developers.perspectiveapi.com/s/about-the-api-attributes-and-languages?language=en_US
(the default is TOXICITY).
""" # noqa
import json

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
    "TOXICITY": {}
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


def perform_classification(text: str) -> dict:
    """Perform classifications on text.
    
    Able to eventually support different inference functions.
    """
    classification_functions = [classify_text_toxicity]
    classification_probs_and_labels = {}
    for inference_func in classification_functions:
        res = inference_func(text)
        classification_probs_and_labels.update(res)

    return classification_probs_and_labels


def perform_inference(flattened_posts: list[FlattenedFeedViewPost]) -> list[dict]:
    print(f"Performing inference on {len(flattened_posts)} posts...")
    return [
        {**post, **perform_classification(post["text"])}
        for post in flattened_posts
    ]
