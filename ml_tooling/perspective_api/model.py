"""Model for the classify_perspective_api service."""
import aiohttp
import asyncio
import json
from typing import Optional

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

from lib.helper import GOOGLE_API_KEY, create_batches, logger

DEFAULT_BATCH_SIZE = 50

google_client = discovery.build(
    "commentanalyzer",
    "v1alpha1",
    developerKey=GOOGLE_API_KEY,
    discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",  # noqa
    static_discovery=False,
)


attribute_to_labels_map = {
    # production-ready attributes
    "TOXICITY": {
        "prob": "prob_toxic",
        "label": "label_toxic"
    },
    "SEVERE_TOXICITY": {
        "prob": "prob_severe_toxic",
        "label": "label_severe_toxic"
    },
    "IDENTITY_ATTACK": {
        "prob": "prob_identity_attack",
        "label": "label_identity_attack"
    },
    "INSULT": {

        "prob": "prob_insult",
        "label": "label_insult"
    },
    "PROFANITY": {
        "prob": "prob_profanity",
        "label": "label_profanity"
    },
    "THREAT": {
        "prob": "prob_threat",
        "label": "label_threat"
    },
    # constructive attributes, from Perspective API
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
    "REASONING_EXPERIMENTAL": {
        "prob": "prob_reasoning",
        "label": "label_reasoning"
    },
    "RESPECT_EXPERIMENTAL": {
        "prob": "prob_respect",
        "label": "label_respect"
    },
    # persuasion attributes
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
    "SCAPEGOATING_EXPERIMENTAL": {
        "prob": "prob_scapegoating",
        "label": "label_scapegoating"
    },
    # moderation attributes
    "SEXUALLY_EXPLICIT": {
        "prob": "prob_sexually_explicit",
        "label": "label_sexually_explicit"
    },
    "FLIRTATION": {
        "prob": "prob_flirtation",
        "label": "label_flirtation"
    },
    "SPAM": {
        "prob": "prob_spam",
        "label": "label_spam"
    },
}


default_requested_attribute_keys = list(attribute_to_labels_map.keys())
default_requested_attributes = {
    attribute: {} for attribute in default_requested_attribute_keys
}


def request_comment_analyzer(
    text: str,
    requested_attributes: dict=default_requested_attributes
) -> str:
    """Sends request to commentanalyzer endpoint.

    Docs at https://developers.perspectiveapi.com/s/docs-sample-requests?language=en_US

    Example request:

    analyze_request = {
    'comment': { 'text': 'friendly greetings from python' },
    'requestedAttributes': {'TOXICITY': {}}
    }

    response = client.comments().analyze(body=analyze_request).execute()
    print(json.dumps(response, indent=2))
    """  # noqa
    if not requested_attributes:
        requested_attributes = default_requested_attributes
    analyze_request = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": requested_attributes,
    }
    logger.info(
        f"Sending request to commentanalyzer endpoint with request={analyze_request}...",  # noqa
    )
    try:
        response = google_client.comments().analyze(body=analyze_request).execute()
    except HttpError as e:
        logger.error(f"Error sending request to commentanalyzer: {e}")
        response = {"error": str(e)}
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



def process_response(response_str: str) -> dict:
    response_obj = json.loads(response_str)
    if "error" in response_obj:
        return {"error": response_obj["error"]}
    classification_probs_and_labels = {}
    for attribute, labels in attribute_to_labels_map.items():
        if attribute in response_obj["attributeScores"]:
            prob_score = (
                response_obj["attributeScores"][attribute]["summaryScore"]["value"]  # noqa
            )
            classification_probs_and_labels[labels["prob"]] = prob_score
            classification_probs_and_labels[labels["label"]] = 0 if prob_score < 0.5 else 1 # noqa
    return classification_probs_and_labels


def classify(
    text: str, attributes: Optional[dict] = default_requested_attributes
) -> dict:
    """Classify text using all the attributes from the Google Perspectives API."""  # noqa
    response: str = request_comment_analyzer(
        text=text, requested_attributes=attributes
    )
    return process_response(response)


def create_perspective_request(text):
    return {
        'comment': {'text': text},
        'languages': ['en'],
        'requestedAttributes': default_requested_attributes
    }


async def process_perspective_batch(requests, responses):
    """Process a batch of requests in a single query.

    See https://googleapis.github.io/google-api-python-client/docs/batch.html
    for more details
    """
    batch = BatchHttpRequest()
    
    def callback(request_id, response, exception):
        if exception is not None:
            print(f"Request {request_id} failed: {exception}")
            responses.append(None)
        else:
            response_str = json.dumps(response)
            response_obj = json.loads(response_str)
            if "error" in response_obj:
                print(f"Request {request_id} failed: {response_obj['error']}")
                responses.append(None)
            classification_probs_and_labels = {}
            for attribute, labels in attribute_to_labels_map.items():
                if attribute in response_obj["attributeScores"]:
                    prob_score = (
                        response_obj["attributeScores"][attribute]["summaryScore"]["value"]  # noqa
                    )
                    classification_probs_and_labels[labels["prob"]] = prob_score # noqa
                    classification_probs_and_labels[labels["label"]] = 0 if prob_score < 0.5 else 1
            responses.append(classification_probs_and_labels)

    for _, request in enumerate(requests):
        batch.add(
            google_client.comments().analyze(body=request), callback=callback
        )

    batch.execute()


async def batch_classify_texts(
    texts: list[str],
    batch_size: Optional[int]=DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float]=1.0
):
    request_payloads: list[dict] = [
        create_perspective_request(text) for text in texts
    ]
    request_payload_batches: list[list[dict]] = create_batches(
        iterable=request_payloads, batch_size=batch_size
    )
    responses: list[dict] = []
    for batch in request_payload_batches:
        await process_perspective_batch(batch, responses)
        await asyncio.sleep(seconds_delay_per_batch)
    return responses


def run_batch_classification(
    texts: list[str],
    batch_size: Optional[int]=DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float]=1.0
):
    loop = asyncio.get_event_loop()
    responses = loop.run_until_complete(
        batch_classify_texts(
            texts=texts,
            batch_size=batch_size,
            seconds_delay_per_batch=seconds_delay_per_batch
        )
    )
    return responses
