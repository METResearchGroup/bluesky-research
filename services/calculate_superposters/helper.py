"""Calculate superposters."""
from typing import Optional

from lib.aws.athena import Athena, DEFAULT_DB_NAME
from lib.aws.dynamodb import DynamoDB
from lib.constants import current_datetime, current_datetime_str
from lib.log.logger import get_logger

DB_NAME = DEFAULT_DB_NAME
GLUE_TABLE_NAME = "daily_posts"
DYNAMODB_TABLE_NAME = "superposters"

athena = Athena()
dynamodb = DynamoDB()
logger = get_logger(__name__)


def calculate_latest_superposters(
    percentile: Optional[float] = None,
    threshold: Optional[float] = None
):
    """Get latest superposters.

    Calculates either based on percentile (e.g., top 5% of posters)
    or threshold (e.g., >=5 posts).

    Prioritizes percentile over threshold if both are given.
    """
    if percentile is not None:
        query = f"""
        WITH ranked_users AS (
            SELECT id, COUNT(*) as count,
                   PERCENT_RANK() OVER (ORDER BY COUNT(*) DESC) as percentile_rank
            FROM {DB_NAME}.{GLUE_TABLE_NAME}
            GROUP BY id
        )
        SELECT id, count
        FROM ranked_users
        WHERE percentile_rank <= {percentile}
        ORDER BY count DESC
        """  # noqa
    elif threshold is not None:
        query = f"""
        SELECT id, COUNT(*) as count
        FROM {DB_NAME}.{GLUE_TABLE_NAME}
        GROUP BY id
        HAVING COUNT(*) >= {threshold}
        ORDER BY count DESC
        """
    else:
        raise ValueError("Either percentile or threshold must be provided.")

    # fetch results from Athena.
    superposters_df = athena.query_results_as_df(query)
    logger.info(f"Fetched {len(superposters_df)} superposters from Athena.")

    # transform results, get as a list of dicts.
    superposter_dicts = superposters_df.to_dict(orient="records")
    output = {
        "insert_date_timestamp": current_datetime_str,
        "insert_date": current_datetime.strftime("%Y-%m-%d"),
        "superposters": superposter_dicts,
        "method": "percentile" if percentile is not None else "threshold",
        "percentile": percentile,
        "threshold": threshold
    }

    # write to DynamoDB.
    dynamodb.insert_item_into_table(output, DYNAMODB_TABLE_NAME)
