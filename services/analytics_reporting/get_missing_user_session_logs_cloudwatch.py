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
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

logs_client = boto3.client("logs")
s3 = S3()
logger = get_logger(__name__)

log_group_name = "ec2-feed-api-logs"
log_stream_name = "i-0917ffe080d3d91f1/bsky-logs"
query = f'fields @timestamp, @message | filter @logStream = "{log_stream_name}" | filter @message like /User DID not in the study/'
# days = 10 # for an extended lookback, need to fetch a longer time period.
days = 1  # during normal lookback, just need the past day or so.
diff = timedelta(days=days)
start_time = int((datetime.now() - diff).timestamp()) * 1000
end_time = int(datetime.now().timestamp()) * 1000  # now

study_users: list[UserToBlueskyProfileModel] = get_all_users()
study_user_did_to_handle_map = {
    user.bluesky_user_did: user.bluesky_handle for user in study_users
}
valid_dids = {user.bluesky_user_did for user in study_users}


def extract_did(message):
    did_pattern = r"did:plc:[a-z0-9]+"
    match = re.search(did_pattern, message)
    return match.group(0) if match else None


def main() -> None:
    # Start the query
    response = logs_client.start_query(
        logGroupName=log_group_name,
        startTime=start_time,
        endTime=end_time,
        queryString=query,
    )

    query_id = response["queryId"]

    # Wait for the query to complete
    while True:
        response = logs_client.get_query_results(queryId=query_id)
        if response["status"] != "Running":
            break
        time.sleep(1)

    # Process the results with pagination
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

    # Process the results
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
            ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
            formatted_timestamp = ts.strftime(timestamp_format)
            date = ts.date().strftime("%Y-%m-%d")
            user_session_logs.append(
                {
                    "timestamp": formatted_timestamp,
                    "date": date,
                    "user_did": did,
                }
            )

    if len(user_session_logs) == 0:
        logger.info(
            f"No backfill user session logs to write for the past {days} days. Any missing log accesses are from non-study participants.Exiting..."
        )
        return

    df = pd.DataFrame(user_session_logs)

    # group by date.  Then, write each one to a separate file in S3.
    for date, group in df.groupby("date"):
        uuid = str(uuid4())[:8]
        filename = f"backfill_user_session_logs_{date}_{uuid}.jsonl"

        # drop "date" column
        group = group.drop(columns=["date"])

        # add new columns with default values, to match user_session_logs table.
        group["cursor"] = "backfill"
        group["limit"] = 50
        group["feed_length"] = 50
        group["feed"] = "default (backfill)"

        # convert to list of dicts
        group_dicts: list[dict] = group.to_dict(orient="records")

        # export to s3
        key = os.path.join("user_session_logs", f"partition_date={date}", filename)  # noqa
        s3.write_dicts_jsonl_to_s3(data=group_dicts, key=key)

        logger.info(
            f"(Backfill date: {date}): Wrote {len(group_dicts)} backfill records to {key}."
        )
        num_unique_user_dids = group["user_did"].nunique()
        logger.info(
            f"(Backfill date: {date}): Number of unique user_did values: {num_unique_user_dids}"
        )

    logger.info(f"Finished backfilling user session logs for {days} days.")
    logger.info(
        f"Start date: {df['date'].min()}.  End date: {df['date'].max()}. Total records: {len(df)}."
    )


if __name__ == "__main__":
    main()
