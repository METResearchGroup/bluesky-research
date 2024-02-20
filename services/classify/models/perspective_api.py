import json
import time
from typing import Optional

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
    #"POWER_APPEAL_EXPERIMENTAL": {},
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


def classify_text(
    text: str,
    attributes: Optional[dict] = default_requested_attributes
  ) -> dict:
    """Classify text using all the attributes from the Google Perspectives API.""" # noqa
    response = request_comment_analyzer(
        text=text, requested_attributes=attributes
    )
    response_obj = json.loads(response)
    classification_probs_and_labels = {}
    for attribute, labels in attribute_to_labels_map.items():
        if attribute in response_obj["attributeScores"]:
          prob_score = (
              response_obj["attributeScores"][attribute]["summaryScore"]["value"]
          )
          classification_probs_and_labels[labels["prob"]] = prob_score
          classification_probs_and_labels[labels["label"]] = 0 if prob_score < 0.5 else 1
    return classification_probs_and_labels


def perform_batch_inference(batch: list[str]) -> list[dict]:
    """Performs inference using the Google Perspectives API.

    Returns a JSON of the following format:
    {
        "text": <text>,
        "TOXICITY": <binary 0 or 1>,
        "AFFINITY_EXPERIMENTAL": <binary 0 or 1>,
        "COMPASSION_EXPERIMENTAL": <binary 0 or 1>,
        "CONSTRUCTIVE_EXPERIMENTAL": <binary 0 or 1>,
        "CURIOSITY_EXPERIMENTAL": <binary 0 or 1>,
        "NUANCE_EXPERIMENTAL": <binary 0 or 1>,
        "PERSONAL_STORY_EXPERIMENTAL": <binary 0 or 1>,
        "ALIENATION_EXPERIMENTAL": <binary 0 or 1>,
        "FEARMONGERING_EXPERIMENTAL": <binary 0 or 1>,
        "GENERALIZATION_EXPERIMENTAL": <binary 0 or 1>,
        "MORAL_OUTRAGE_EXPERIMENTAL": <binary 0 or 1>,
        "POWER_APPEAL_EXPERIMENTAL": <binary 0 or 1>,
        "SCAPEGOATING_EXPERIMENTAL": <binary 0 or 1>
    }
    """
    processed_posts = []
    for idx, text in enumerate(batch):
        if idx % 30 == 0:
            logger.info(f"Processed {idx} posts...")
        classification_probs_and_labels = classify_text(text)
        processed_post = {
            "text": text,
            **classification_probs_and_labels
        }
        processed_posts.append(processed_post)
        time.sleep(1.2)
    return processed_posts


if __name__ == "__main__":
    texts = [
        "Restricting abortion access in all cases is morally wrong.",
        "The situation in Gaza is a horrible humanitarian crisis and we shouldn't ignore it.",
        "I can't believe the refs are so biased in the Super Bowl!"
    ]
    outputs: list[dict] = perform_batch_inference(texts)
