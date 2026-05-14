"""Queries CloudWatch logs to get logs of users whose logins weren't correctly
captured in the user_session_logs table.

Runs daily to fetch logs and consolidates with the users who are in the study
in order to, with the data that is already captured in the user_session_logs,
build a complete picture of user sessions.
"""

from datetime import datetime, timedelta
import os
import re
import time
from uuid import uuid4

import boto3
import pandas as pd

from lib.aws.s3 import S3
from lib.constants import timestamp_format
from lib.log.logger import get_logger
from services.compact_user_session_logs.constants import (
    BACKFILL_CURSOR,
    BACKFILL_FEED_LABEL,
    BACKFILL_FEED_LENGTH,
    BACKFILL_FILENAME_PREFIX,
    BACKFILL_FILENAME_SUFFIX,
    BACKFILL_LIMIT,
    BACKFILL_LOOKBACK_DAYS,
    CLOUDWATCH_INSIGHTS_QUERY_TEMPLATE,
    CLOUDWATCH_LOG_GROUP_NAME,
    CLOUDWATCH_LOG_STREAM_NAME,
    CLOUDWATCH_TIMESTAMP_PARSE_FORMAT,
    DID_PLC_REGEX,
    INTERNAL_BACKFILL_DATE_COLUMN,
    USER_SESSION_LOGS_S3_SEGMENT,
)
from services.participant_data.helper import get_all_users

logs_client = boto3.client("logs")
s3 = S3()
logger = get_logger(__name__)


def extract_did(message: str) -> str | None:
    match = re.search(DID_PLC_REGEX, message)
    return match.group(0) if match else None


def main() -> None:
    valid_dids = {user.bluesky_user_did for user in get_all_users()}
    diff = timedelta(days=BACKFILL_LOOKBACK_DAYS)
    now = datetime.now()
    start_time_ms = int((now - diff).timestamp()) * 1000
    end_time_ms = int(now.timestamp()) * 1000

    query = CLOUDWATCH_INSIGHTS_QUERY_TEMPLATE.format(stream=CLOUDWATCH_LOG_STREAM_NAME)

    response = logs_client.start_query(
        logGroupName=CLOUDWATCH_LOG_GROUP_NAME,
        startTime=start_time_ms,
        endTime=end_time_ms,
        queryString=query,
    )

    query_id = response["queryId"]

    while True:
        response = logs_client.get_query_results(queryId=query_id)
        if response["status"] != "Running":
            break
        time.sleep(1)

    all_results = []
    next_token = None

    while True:
        if next_token:
            response = logs_client.get_query_results(
                queryId=query_id, nextToken=next_token
            )
        else:
            response = logs_client.get_query_results(queryId=query_id)

        all_results.extend(response["results"])

        if "nextToken" in response:
            next_token = response["nextToken"]
            logger.info(
                f"Fetched {len(response['results'])} results. Fetching next page..."
            )
        else:
            break

    logger.info(f"Total results fetched: {len(all_results)}")

    user_session_logs: list[dict] = []

    total_results = len(all_results)
    for idx, result in enumerate(all_results):
        if idx % 100 == 0:
            print(f"Processed {idx}/{total_results} results...")
        timestamp = next(
            field["value"] for field in result if field["field"] == "@timestamp"
        )
        message = next(
            field["value"] for field in result if field["field"] == "@message"
        )
        did = extract_did(message)
        if did and did in valid_dids:
            ts = datetime.strptime(timestamp, CLOUDWATCH_TIMESTAMP_PARSE_FORMAT)
            formatted_timestamp = ts.strftime(timestamp_format)
            date = ts.date().strftime("%Y-%m-%d")
            user_session_logs.append(
                {
                    "timestamp": formatted_timestamp,
                    INTERNAL_BACKFILL_DATE_COLUMN: date,
                    "user_did": did,
                }
            )

    if len(user_session_logs) == 0:
        logger.info(
            f"No backfill user session logs to write for the past {BACKFILL_LOOKBACK_DAYS} days. "
            "Any missing log accesses are from non-study participants.Exiting..."
        )
        return

    df = pd.DataFrame(user_session_logs)

    for date, group in df.groupby(INTERNAL_BACKFILL_DATE_COLUMN):
        short_uuid = str(uuid4())[:8]
        filename = (
            f"{BACKFILL_FILENAME_PREFIX}{date}_{short_uuid}{BACKFILL_FILENAME_SUFFIX}"
        )

        group = group.drop(columns=[INTERNAL_BACKFILL_DATE_COLUMN])

        group["cursor"] = BACKFILL_CURSOR
        group["limit"] = BACKFILL_LIMIT
        group["feed_length"] = BACKFILL_FEED_LENGTH
        group["feed"] = BACKFILL_FEED_LABEL

        group_dicts: list[dict] = group.to_dict(orient="records")

        key = os.path.join(
            USER_SESSION_LOGS_S3_SEGMENT,
            f"partition_date={date}",
            filename,
        )
        s3.write_dicts_jsonl_to_s3(data=group_dicts, key=key)

        logger.info(
            f"(Backfill date: {date}): Wrote {len(group_dicts)} backfill records to {key}."
        )
        num_unique_user_dids = group["user_did"].nunique()
        logger.info(
            f"(Backfill date: {date}): Number of unique user_did values: {num_unique_user_dids}"
        )

    logger.info(
        f"Finished backfilling user session logs for {BACKFILL_LOOKBACK_DAYS} days."
    )
    logger.info(
        f"Start date: {df[INTERNAL_BACKFILL_DATE_COLUMN].min()}.  "
        f"End date: {df[INTERNAL_BACKFILL_DATE_COLUMN].max()}. Total records: {len(df)}."
    )


if __name__ == "__main__":
    main()
