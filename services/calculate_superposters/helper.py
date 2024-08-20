"""Calculate superposters."""
from typing import Optional

from boto3.dynamodb.types import TypeSerializer

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

serializer = TypeSerializer()


def calculate_latest_superposters(
    top_n_percent: Optional[float] = None,
    threshold: Optional[float] = None
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
            FROM {DB_NAME}.{GLUE_TABLE_NAME}
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
        FROM {DB_NAME}.{GLUE_TABLE_NAME}
        GROUP BY author_did
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
        "method": "top_n_percent" if top_n_percent is not None else "threshold",
        "top_n_percent": top_n_percent,
        "threshold": threshold
    }

    serialized_output = {k: serializer.serialize(v) for k, v in output.items()}

    # write to DynamoDB.
    dynamodb.insert_item_into_table(serialized_output, DYNAMODB_TABLE_NAME)

    logger.info(f"Wrote {len(serialized_output)} superposters to DynamoDB.")


if __name__ == "__main__":
    calculate_latest_superposters(top_n_percent=None, threshold=5)
