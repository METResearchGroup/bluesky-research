"""Calculate superposters."""

import json
import os
import re
from typing import Optional

from boto3.dynamodb.types import TypeSerializer

from lib.aws.athena import Athena, DEFAULT_DB_NAME
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime, current_datetime_str
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.calculate_superposters.models import (
    SuperposterModel,
    SuperposterCalculationModel,
)

DB_NAME = DEFAULT_DB_NAME
DAILY_POSTS_GLUE_TABLE_NAME = "daily_posts"
athena_table_name = "daily_superposters"
dynamodb_table_name = "superposter_calculation_sessions"

athena = Athena()
dynamodb = DynamoDB()
s3 = S3()
s3_root_key = "daily_superposters"
logger = get_logger(__name__)

serializer = TypeSerializer()


def insert_superposter_session(superposter_calculation_session: dict):
    """Insert superposter session."""
    try:
        dynamodb.insert_item_into_table(
            item=superposter_calculation_session, table_name=dynamodb_table_name
        )
        logger.info(
            f"Successfully inserted superposter calculation session: {superposter_calculation_session}"  # noqa
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to insert enrichment consolidation session: {e}")  # noqa
        raise


def calculate_latest_superposters(
    top_n_percent: Optional[float] = None, threshold: Optional[float] = None
):
    """Get latest superposters.

    Calculates either based on percentile (e.g., top 5% of posters)
    or threshold (e.g., >=5 posts).

    Prioritizes percentile over threshold if both are given.
    """
    if top_n_percent is not None:
        query = f"""
        WITH ranked_users AS (
            SELECT author_did, COUNT(*) as count,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as row_num, # returns row number for the resulting grouped output
                COUNT(*) OVER () as total_count # calculates the total number of distinct author_did values.
            FROM {DB_NAME}.{DAILY_POSTS_GLUE_TABLE_NAME}
            GROUP BY author_did
        )
        SELECT author_did, count
        FROM ranked_users
        WHERE row_num <= total_count * {top_n_percent}
        ORDER BY count DESC
        """  # noqa
    elif threshold is not None:
        query = f"""
        SELECT author_did, COUNT(*) as count
        FROM {DB_NAME}.{DAILY_POSTS_GLUE_TABLE_NAME}
        GROUP BY author_did
        HAVING COUNT(*) >= {threshold}
        ORDER BY count DESC
        """
    else:
        raise ValueError("Either percentile or threshold must be provided.")

    superposters_df = athena.query_results_as_df(query)
    logger.info(f"Fetched {len(superposters_df)} superposters from Athena.")

    superposters = [
        SuperposterModel(author_did=row["author_did"], count=row["count"])
        for _, row in superposters_df.iterrows()
    ]

    timestamp = generate_current_datetime_str()
    s3_full_key = None

    if len(superposters) > 0:
        output = SuperposterCalculationModel(
            insert_date_timestamp=current_datetime_str,
            insert_date=current_datetime.strftime("%Y-%m-%d"),
            superposters=superposters,
            method="top_n_percent" if top_n_percent is not None else "threshold",
            top_n_percent=top_n_percent,
            threshold=threshold,
        )
        output_dict = output.dict()
        s3_full_key = os.path.join(s3_root_key, f"superposters_{timestamp}.json")
        s3.write_dict_json_to_s3(data=output_dict, key=s3_full_key)
    else:
        logger.info("No superposters found. Not exporting file to S3.")
    superposter_calculation_session = {
        "insert_date_timestamp": timestamp,
        "insert_date": current_datetime.strftime("%Y-%m-%d"),
        "method": "top_n_percent" if top_n_percent is not None else "threshold",
        "top_n_percent": top_n_percent,
        "threshold": threshold,
        "s3_full_key": s3_full_key,
    }
    insert_superposter_session(superposter_calculation_session)

    logger.info(f"Wrote {len(superposters)} superposters to S3.")


def transform_string(input_str: str) -> str:
    """Transforms the superposter string.

    e.g.,
    >> transform_string('{author_did=did:plc:jhfzhcn4lgr5bapem2lyodwm, count=5}')
    '{"author_did":"did:plc:jhfzhcn4lgr5bapem2lyodwm", "count":5}'
    """
    # Step 1: Surround keys with quotes
    input_str = re.sub(r"(\w+)=", r'"\1":', input_str)

    # Step 2: Surround the did:plc:<some string> with quotes
    input_str = re.sub(r"(did:plc:[\w]+)", r'"\1"', input_str)

    return input_str


def load_latest_superposters() -> set[str]:
    """Loads the latest superposter DIDs."""
    query = f"""
    SELECT * FROM {DB_NAME}.{athena_table_name}
    ORDER BY insert_date_timestamp DESC
    LIMIT 1
    """
    superposters_df = athena.query_results_as_df(query)
    superposter_dicts = superposters_df.to_dict(orient="records")
    superposter_dicts = athena.parse_converted_pandas_dicts(superposter_dicts)
    superposter_dict = superposter_dicts[0]
    superposters: str = superposter_dict["superposters"]
    superposter_list: list[dict] = json.loads(transform_string(superposters))
    superposter_dict["superposters"] = superposter_list
    superposter_model = SuperposterCalculationModel(**superposter_dict)
    author_dids = [
        superposter.author_did for superposter in superposter_model.superposters
    ]
    return set(author_dids)


if __name__ == "__main__":
    calculate_latest_superposters(top_n_percent=None, threshold=5)
    # load_latest_superposters()
