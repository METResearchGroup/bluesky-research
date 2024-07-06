"""Pipeline for running preprocessing steps.

Loads the latest data for preprocessing and runs the preprocessing steps.

Run via `typer`: https://pypi.org/project/typer/

Example usage:
>>> python main.py --sync-type firehose
"""
import sys
import traceback

import typer

from lib.log.logger import Logger
from services.preprocess_raw_data.helper import preprocess_latest_raw_data

logger = Logger(__name__)


def main():
    try:
        logger.info("Starting preprocessing pipeline.")
        preprocess_latest_raw_data()
        logger.info("Completed preprocessing pipeline.")
    except Exception as e:
        logger.error(f"Error in preprocessing pipeline: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
