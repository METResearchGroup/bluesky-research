"""Compacts user session logs, partitioned by date."""

import os
from uuid import uuid4

import pandas as pd

from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.helper import track_performance
from lib.log.logger import get_logger

from services.compact_user_session_logs.get_missing_user_session_logs_cloudwatch import (
    main as get_missing_user_session_logs_cloudwatch,
)  # noqa

logger = get_logger(__name__)
glue = Glue()
s3 = S3()
root_prefix = "user_session_logs/"
glue_crawler_name = "user_session_logs_glue_crawler"


@track_performance
def main():
    logger.info("Starting to compact user session logs.")
    partition_date_prefixes: list[str] = s3.list_immediate_subfolders(
        prefix=root_prefix
    )
    partition_date_prefixes.sort()  # process earlier ones first.
    logger.info(f"Found {len(partition_date_prefixes)} partition date prefixes.")
    for partition_date_prefix in partition_date_prefixes:
        logger.info(f"Processing partition date prefix: {partition_date_prefix}")
        full_prefix = os.path.join(root_prefix, partition_date_prefix)
        keys: list[str] = s3.list_keys_given_prefix(prefix=full_prefix)
        # partition dates with only one file are already compacted.
        if len(keys) > 1:
            logger.info(
                f"Found {len(keys)} files for partition date prefix: {partition_date_prefix}"
            )
            df: pd.DataFrame = s3.load_keys_to_df(keys=keys)
            df = df.drop_duplicates(subset=["user_did", "timestamp", "cursor"])
            uuid = str(uuid4())[:8]
            export_key = os.path.join(
                full_prefix, f"compacted_{partition_date_prefix}_{uuid}.jsonl"
            )
            df_dicts = df.to_dict(orient="records")
            logger.info(
                f"(Partition date: {partition_date_prefix}): Writing {len(df_dicts)} compacted records to {export_key}."
            )
            s3.write_dicts_jsonl_to_s3(data=df_dicts, key=export_key)
            logger.info(
                f"(Partition date: {partition_date_prefix}): Deleting {len(keys)} files."
            )
            for key in keys:
                s3.delete_from_s3(key=key)
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
    logger.info("Checking for missing user session logs in CloudWatch.")
    get_missing_user_session_logs_cloudwatch()
    logger.info("Finished checking for missing user session logs in CloudWatch.")
    logger.info(
        f"Triggering Glue crawler: {glue_crawler_name} to update the Glue catalog."
    )
    glue.start_crawler(crawler_name=glue_crawler_name)
    logger.info(
        f"Triggered Glue crawler: {glue_crawler_name} to update the Glue catalog."
    )
    logger.info("Completed compacting user session logs.")


if __name__ == "__main__":
    main()
