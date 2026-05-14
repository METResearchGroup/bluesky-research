"""Migrate service data from S3 (via Athena) into local partitioned storage."""

from typing import Literal, Optional

import pandas as pd

from lib.constants import default_lookback_days
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger

from services.compact_all_services.constants import default_export_format
from services.compact_all_services.s3_compaction import (
    athena,
    generate_service_sql_query,
    get_service_compaction_session,
)

logger = get_logger(__name__)


def compact_migrate_s3_data_to_local_storage(
    service: str,
    timestamp: Optional[str] = None,
    export_format: Literal["json", "parquet"] = default_export_format,
    lookback_days: int = default_lookback_days,
) -> None:
    """Migrates data from S3 to local storage and compacts it by date.

    Steps:
    1. Load data from S3 (via Athena) as a pandas dataframe.
    2. Divide the dataframe into chunks, by day.
    2. Write data to local storage. Each day's data is its own file. Determines
    if the data is older than "lookback_days" and writes it to the "/cache"
    path or the "/active" path.

    This uses `export_data_to_local_storage` and just provides the related df
    to export.
    """
    logger.info(f"Migrating service={service} from S3 to local storage")
    latest_service_compaction_session: dict = get_service_compaction_session(service)
    timestamp = latest_service_compaction_session.get("compaction_timestamp", None)
    if timestamp:
        logger.info(f"Compacting data from {service} after {timestamp}")
    query = generate_service_sql_query(service, timestamp)
    dtypes_map = MAP_SERVICE_TO_METADATA[service].get("dtypes_map", None)
    df: pd.DataFrame = athena.query_results_as_df(query, dtypes_map=dtypes_map)
    export_data_to_local_storage(
        service=service, df=df, export_format=export_format, lookback_days=lookback_days
    )
    logger.info(f"Successfully migrated service={service} from S3 to local storage")
