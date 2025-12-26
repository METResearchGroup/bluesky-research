"""Load superposter data."""

import json
import re
from typing import Optional

import pandas as pd

from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

from services.calculate_superposters.models import (
    CalculateSuperposterSource,
    SuperposterCalculationModel,
)

logger = get_logger(__name__)

# Constants for remote loading (lazy loaded)
_DB_NAME = None
_ATHENA_TABLE_NAME = "daily_superposters"
_athena = None


def _get_athena():
    """Lazy load Athena instance to avoid import dependency."""
    global _athena, _DB_NAME
    if _athena is None:
        from lib.aws.athena import Athena, DEFAULT_DB_NAME

        _athena = Athena()
        _DB_NAME = DEFAULT_DB_NAME
    return _athena, _DB_NAME


def _transform_string(input_str: str) -> str:
    """Transforms the superposter string from Athena format to JSON.

    e.g.,
    >> _transform_string('{author_did=did:plc:jhfzhcn4lgr5bapem2lyodwm, count=5}')
    '{"author_did":"did:plc:jhfzhcn4lgr5bapem2lyodwm", "count":5}'
    """
    # Step 1: Surround keys with quotes
    input_str = re.sub(r"(\w+)=", r'"\1":', input_str)

    # Step 2: Surround the did:plc:<some string> with quotes
    input_str = re.sub(r"(did:plc:[\w]+)", r'"\1"', input_str)

    return input_str


def _load_from_local_storage(
    latest_timestamp: Optional[str] = None,
) -> list[dict]:
    """Load superposters from local storage.

    Args:
        latest_timestamp: Optional timestamp string to filter superposters.
            If None, loads all available data.

    Returns:
        List of superposter dictionaries with 'author_did' and 'count' keys.
    """
    superposters_df: pd.DataFrame = load_data_from_local_storage(
        service="daily_superposters", latest_timestamp=latest_timestamp
    )

    if len(superposters_df) == 0:
        logger.warning("No superposters found in latest batch.")
        return []

    # The superposters column contains a JSON string
    superposters_json_str: str = superposters_df["superposters"].iloc[0]
    superposters_list: list[dict] = json.loads(superposters_json_str)
    return superposters_list


def _load_from_remote() -> list[dict]:
    """Load superposters from Athena (remote).

    Returns:
        List of superposter dictionaries with 'author_did' and 'count' keys.
    """
    athena, db_name = _get_athena()

    query = f"""
    SELECT * FROM {db_name}.{_ATHENA_TABLE_NAME}
    ORDER BY insert_date_timestamp DESC
    LIMIT 1
    """
    superposters_df = athena.query_results_as_df(query)
    superposter_dicts = superposters_df.to_dict(orient="records")
    superposter_dicts = parse_converted_pandas_dicts(superposter_dicts)

    if len(superposter_dicts) == 0:
        logger.warning("No superposters found in Athena.")
        return []

    superposter_dict = superposter_dicts[0]
    superposters: str = superposter_dict["superposters"]

    # Transform the Athena format string to valid JSON
    superposters_json_str = _transform_string(superposters)
    superposter_list: list[dict] = json.loads(superposters_json_str)

    # Validate with Pydantic model (optional but good for type safety)
    superposter_dict["superposters"] = superposter_list
    superposter_model = SuperposterCalculationModel(**superposter_dict)

    # Convert to list of dicts for consistency
    return [superposter.dict() for superposter in superposter_model.superposters]


def load_latest_superposters(
    source: CalculateSuperposterSource = CalculateSuperposterSource.LOCAL,
    latest_timestamp: Optional[str] = None,
) -> set[str]:
    """Load the latest superposter DIDs.

    Loads superposter data from either local storage or remote (Athena),
    then extracts and returns the set of author DIDs.

    Args:
        source: Source to load from - "local" (local storage) or "remote" (Athena).
        latest_timestamp: Optional timestamp string to filter superposters when
            loading from local storage. Ignored when source is "remote".

    Returns:
        Set of superposter author DIDs.
    """
    if source == CalculateSuperposterSource.LOCAL:
        superposters_list = _load_from_local_storage(latest_timestamp=latest_timestamp)
    elif source == CalculateSuperposterSource.REMOTE:
        superposters_list = _load_from_remote()
    else:
        raise ValueError(f"Unknown source: {source}")

    # Extract author_dids into a set
    author_dids = {superposter["author_did"] for superposter in superposters_list}
    return author_dids
