"""Pipeline for classifying posts.

Fetches preprocessed posts that are more recent than the most recently
classified post for a given classification type.

Run via `typer`: https://pypi.org/project/typer/

Example usage:
>>> python main.py --inference-type perspective
>>> python main.py --inference-type sociopolitical
"""

import sys
import typer

from lib.log.logger import Logger
from .perspective_api.perspective_api import classify_latest_posts as classify_perspective_posts  # noqa
from .sociopolitical import classify_latest_posts as classify_sociopolitical_posts  # noqa

from enum import Enum

logger = Logger(__name__)


class InferenceType(str, Enum):
    perspective = "perspective"
    sociopolitical = "sociopolitical"


def main(
    inference_type: InferenceType = typer.Option(
        ..., help="Type of inference to run"
    )
):
    try:
        logger.info(f"Starting classification with type: {inference_type}")
        if inference_type == "perspective":
            classify_perspective_posts()
        elif inference_type == "sociopolitical":
            classify_sociopolitical_posts()
        else:
            print("Invalid inference type.")
            logger.error("Invalid inference type provided.")
            sys.exit(1)
        logger.info(f"Completed classification with type: {inference_type}")
    except Exception as e:
        logger.error(f"Error in classification pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
