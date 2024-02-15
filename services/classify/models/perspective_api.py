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
