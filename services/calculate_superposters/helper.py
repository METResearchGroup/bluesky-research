"""Calculate superposters."""

from datetime import timedelta
import json
from typing import Optional

from boto3.dynamodb.types import TypeSerializer
import pandas as pd

from lib.aws.athena import Athena, DEFAULT_DB_NAME
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime, current_datetime_str, timestamp_format  # noqa
from lib.db.manage_local_data import load_latest_data, export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
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
    top_n_percent: Optional[float] = None,
    threshold: Optional[float] = None,
    use_athena: bool = False,
):
    """Get latest superposters.

    Calculates either based on percentile (e.g., top 5% of posters)
    or threshold (e.g., >=5 posts).

    Prioritizes percentile over threshold if both are given.
    """
    lookback_days = 1
    lookback_datetime = current_datetime - timedelta(days=lookback_days)
    lookback_datetime_str = lookback_datetime.strftime(timestamp_format)

    posts_df: pd.DataFrame = load_latest_data(
        service="daily_superposters",
        latest_timestamp=lookback_datetime_str,
    )
    if len(posts_df) == 0:
        logger.info("No posts to calculate superposters for.")
        return
    if top_n_percent is not None:
        query = f"""
        WITH ranked_users AS (
            SELECT author_did, COUNT(*) as count,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as row_num, # returns row number for the resulting grouped output
                COUNT(*) OVER () as total_count # calculates the total number of distinct author_did values.
            FROM preprocessed_posts
            GROUP BY author_did
        )
        SELECT author_did, count
        FROM ranked_users
        WHERE synctimestamp >= '{lookback_datetime_str}'
        AND row_num <= total_count * {top_n_percent}
        ORDER BY count DESC
        """  # noqa
        ranked_users = posts_df.groupby("author_did").size().reset_index(name="count")
        ranked_users["row_num"] = ranked_users["count"].rank(
            method="first", ascending=False
        )
        total_count = ranked_users["count"].count()
        output_df = ranked_users[
            (posts_df["synctimestamp"] >= lookback_datetime_str)
            & (ranked_users["row_num"] <= total_count * top_n_percent)
        ].sort_values(by="count", ascending=False)[["author_did", "count"]]
    elif threshold is not None:
        query = f"""
        SELECT author_did, COUNT(*) as count
        FROM preprocessed_posts
        WHERE synctimestamp >= '{lookback_datetime_str}'
        GROUP BY author_did
        HAVING COUNT(*) >= {threshold}
        ORDER BY count DESC
        """
        output_df = (
            posts_df[posts_df["synctimestamp"] >= lookback_datetime_str]
            .groupby("author_did")
            .size()
            .reset_index(name="count")
        )
        output_df = output_df[output_df["count"] >= threshold].sort_values(
            by="count", ascending=False
        )
    else:
        raise ValueError("Either percentile or threshold must be provided.")

    if use_athena:
        superposters_df = athena.query_results_as_df(query)
        logger.info(f"Fetched {len(superposters_df)} superposters from Athena.")
    else:
        logger.info("Using local data to calculate superposters.")
        superposters_df = output_df

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
        dtype_map = MAP_SERVICE_TO_METADATA["daily_superposters"]["dtypes_map"]
        output_dict = output.dict()
        output_dict["superposters"] = json.dumps(output_dict["superposters"])
        df = pd.DataFrame([output_dict])
        df = df.astype(dtype_map)
        export_data_to_local_storage(service="daily_superposters", df=df)
    else:
        logger.info("No superposters found. Not exporting file.")
    superposter_calculation_session = {
        "insert_date_timestamp": timestamp,
        "insert_date": current_datetime.strftime("%Y-%m-%d"),
        "method": "top_n_percent" if top_n_percent is not None else "threshold",
        "top_n_percent": top_n_percent,
        "threshold": threshold,
        "s3_full_key": s3_full_key,
    }
    insert_superposter_session(superposter_calculation_session)

    logger.info(f"Wrote {len(superposters)} superposters.")


if __name__ == "__main__":
    calculate_latest_superposters(top_n_percent=None, threshold=5)
    # load_latest_superposters()
