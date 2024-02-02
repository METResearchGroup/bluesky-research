"""Inference module for classifying text.

For Google Perspective API, the list of possible attributes to classify is
found at https://developers.perspectiveapi.com/s/about-the-api-attributes-and-languages?language=en_US
(the default is TOXICITY).
""" # noqa
import json

from googleapiclient import discovery

from lib.helper import GOOGLE_API_KEY, logger

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
    try:
        toxicity_prob_score = (
            response["attributeScores"]["TOXICITY"]["summaryScore"]["value"]
        )
    except Exception as e:
        print(f"Error in trying to get toxicity score: {e}")
        breakpoint()

    return {
        "prob": toxicity_prob_score,
        "label": 0 if toxicity_prob_score < 0.5 else 1
    }


def classify_texts_toxicity(texts: list[str]) -> list[dict]:
    """Classify texts toxicity."""
    classified_texts: list[dict] = []
    logger.info(
        f"Classifying {len(texts)} texts for toxicity..."
    )
    for text in texts:
        label_dict = classify_text_toxicity(text)
        classified_texts.append(
            {
                "text": text,
                **label_dict
            }
        )
    return classified_texts
