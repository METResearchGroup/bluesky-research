"""Compacts user session logs, partitioned by date."""

# ruff: noqa: E402

import os
from uuid import uuid4

import pandas as pd

_athena = None
_glue = None
_s3 = None
try:
    from lib.helper import track_performance
except ModuleNotFoundError:  # pragma: no cover - local lightweight fallback

    def track_performance(func=None, *args, **kwargs):
        if func is None:
            return lambda inner: inner
        return func


from lib.log.logger import get_logger

from services.compact_user_session_logs.constants import (
    COMPACTED_FILENAME_PREFIX,
    COMPACTED_FILENAME_SUFFIX,
    PARTITION_DATE_COLUMN,
    USER_SESSION_LOGS_ATHENA_QUERY,
    USER_SESSION_LOGS_DEDUPE_COLUMNS,
    USER_SESSION_LOGS_GLUE_CRAWLER_NAME,
    USER_SESSION_LOGS_S3_ROOT_PREFIX,
)

try:
    from services.compact_user_session_logs.get_missing_user_session_logs_cloudwatch import (
        main as get_missing_user_session_logs_cloudwatch,
    )
except ModuleNotFoundError:  # pragma: no cover - local lightweight fallback

    def get_missing_user_session_logs_cloudwatch():
        return None


logger = get_logger(__name__)
athena = None
glue = None
s3 = None


def _get_athena():
    global _athena, athena
    if athena is not None:
        return athena
    if _athena is None:
        from lib.aws.athena import Athena

        _athena = Athena()
        athena = _athena
    return _athena


def _get_glue():
    global _glue, glue
    if glue is not None:
        return glue
    if _glue is None:
        from lib.aws.glue import Glue

        _glue = Glue()
        glue = _glue
    return _glue


def _get_s3():
    global _s3, s3
    if s3 is not None:
        return s3
    if _s3 is None:
        from lib.aws.s3 import S3

        _s3 = S3()
        s3 = _s3
    return _s3


def _partition_date_from_prefix(partition_date_prefix: str) -> str:
    return partition_date_prefix.split("=")[-1].rstrip("/")


def _build_compaction_export_key(full_prefix: str, partition_date_prefix: str) -> str:
    uuid = str(uuid4())[:8]
    return os.path.join(
        full_prefix,
        f"{COMPACTED_FILENAME_PREFIX}{partition_date_prefix}_{uuid}{COMPACTED_FILENAME_SUFFIX}",
    )


def load_all_user_session_logs_to_df() -> pd.DataFrame:
    glue = _get_glue()
    athena = _get_athena()
    glue.start_crawler(crawler_name=USER_SESSION_LOGS_GLUE_CRAWLER_NAME)
    glue.wait_for_crawler_completion(crawler_name=USER_SESSION_LOGS_GLUE_CRAWLER_NAME)
    df = athena.query_results_as_df(query=USER_SESSION_LOGS_ATHENA_QUERY)
    return df


@track_performance
def main():
    logger.info("Starting to compact user session logs.")
    logger.info("Checking for missing user session logs in CloudWatch.")
    get_missing_user_session_logs_cloudwatch()
    logger.info(
        "Finished checking for (and inserting, if relevant) missing user session logs in CloudWatch."
    )
    s3_obj = _get_s3()
    glue_obj = _get_glue()
    partition_date_prefixes: list[str] = s3_obj.list_immediate_subfolders(
        prefix=USER_SESSION_LOGS_S3_ROOT_PREFIX
    )
    partition_date_prefixes.sort()  # process earlier ones first.
    logger.info(f"Found {len(partition_date_prefixes)} partition date prefixes.")
    df: pd.DataFrame = load_all_user_session_logs_to_df()
    for partition_date_prefix in partition_date_prefixes:
        logger.info(f"Processing partition date prefix: {partition_date_prefix}")
        full_prefix = os.path.join(
            USER_SESSION_LOGS_S3_ROOT_PREFIX, partition_date_prefix
        )
        keys: list[str] = s3_obj.list_keys_given_prefix(prefix=full_prefix)
        # partition dates with only one file are already compacted.
        if len(keys) > 1:
            logger.info(
                f"Found {len(keys)} files for partition date prefix: {partition_date_prefix}"
            )
            partition_date = _partition_date_from_prefix(partition_date_prefix)
            subset_df = df[df[PARTITION_DATE_COLUMN] == partition_date]
            if len(subset_df) == 0:
                raise ValueError(
                    f"No records found for partition date: {partition_date_prefix}. That shouldn't be the case if there were files found..."
                )
            subset_df = subset_df.drop_duplicates(
                subset=USER_SESSION_LOGS_DEDUPE_COLUMNS
            )  # type: ignore
            # drop extra partition_date column that persists from fetching
            # partitioned data.
            if PARTITION_DATE_COLUMN in subset_df.columns:
                subset_df = subset_df.drop(columns=[PARTITION_DATE_COLUMN])
            export_key = _build_compaction_export_key(
                full_prefix, partition_date_prefix
            )
            df_dicts = subset_df.to_dict(orient="records")
            if len(df_dicts) == 0:
                raise ValueError(
                    f"No records found for partition date: {partition_date_prefix}. That shouldn't be the case if there were files found..."
                )
            logger.info(
                f"(Partition date: {partition_date_prefix}): Writing {len(df_dicts)} compacted records to {export_key}."
            )
            s3_obj.write_dicts_jsonl_to_s3(data=df_dicts, key=export_key)
            logger.info(
                f"(Partition date: {partition_date_prefix}): Deleting {len(keys)} files."
            )
            for key in keys:
                s3_obj.delete_from_s3(key=key)
            logger.info(
                f"(Partition date: {partition_date_prefix}): Finished deleting {len(keys)} files."
            )
            logger.info(
                f"Finished processing partition date prefix: {partition_date_prefix}"
            )
        else:
            logger.info(
                f"Partition date prefix: {partition_date_prefix} already compacted, skipping..."
            )
        logger.info("-" * 10)
    logger.info("Finished processing all partition date prefixes.")
    logger.info(
        f"Triggering Glue crawler: {USER_SESSION_LOGS_GLUE_CRAWLER_NAME} to update the Glue catalog."
    )
    glue_obj.start_crawler(crawler_name=USER_SESSION_LOGS_GLUE_CRAWLER_NAME)
    glue_obj.wait_for_crawler_completion(
        crawler_name=USER_SESSION_LOGS_GLUE_CRAWLER_NAME
    )
    logger.info(
        f"Triggered Glue crawler: {USER_SESSION_LOGS_GLUE_CRAWLER_NAME} to update the Glue catalog."
    )
    logger.info("Completed compacting user session logs.")


if __name__ == "__main__":
    main()
