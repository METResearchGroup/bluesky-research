"""Wrapper class for all Athena-related access."""

import io
import time
from typing import Optional

import pandas as pd

from lib.aws.helper import create_client
from lib.aws.s3 import S3
from lib.log.logger import get_logger

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

DEFAULT_DB_NAME = "default_db"

# NOTE: the output location must correspond to the workgroup's output location
# set in the Terraform config.
DEFAULT_OUTPUT_LOCATION = "s3://bluesky-research/athena-results"
DEFAULT_MAX_WAITING_TRIES = 5
DEFAULT_WORKGROUP = "prod_workgroup"
MIN_POST_TEXT_LENGTH = 5

s3 = S3()
logger = get_logger(__name__)


class Athena:
    def __init__(self):
        self.client = create_client("athena")

    def run_query(
        self,
        query: str,
        db_name: str = DEFAULT_DB_NAME,
        output_location: str = DEFAULT_OUTPUT_LOCATION,
        max_waiting_tries: int = DEFAULT_MAX_WAITING_TRIES,
        workgroup: str = DEFAULT_WORKGROUP,
    ):
        logger.info(f"Running query: {query}")
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": db_name},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )
        query_execution_id = response["QueryExecutionId"]
        status = "RUNNING"

        num_waits = 0

        while status in ["RUNNING", "QUEUED"]:
            response = self.client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response["QueryExecution"]["Status"]["State"]
            if status in ["FAILED", "CANCELLED"]:
                raise Exception(
                    f"Query {status}: {response['QueryExecution']['Status']['StateChangeReason']}"
                )
            time.sleep(5)
            num_waits += 1
            if num_waits >= max_waiting_tries:
                raise Exception(f"Query exceeds max waiting tries: {status}")

        # Fetch query results
        result_location = response["QueryExecution"]["ResultConfiguration"][
            "OutputLocation"
        ]
        bucket_name = result_location.split("/")[2]
        key = "/".join(result_location.split("/")[3:])

        return bucket_name, key

    def query_results_as_df(
        self,
        query: str,
        db_name: str = DEFAULT_DB_NAME,
        output_location: str = DEFAULT_OUTPUT_LOCATION,
        max_waiting_tries: int = DEFAULT_MAX_WAITING_TRIES,
        workgroup: str = DEFAULT_WORKGROUP,
    ):
        _, result_key = self.run_query(
            query=query,
            db_name=db_name,
            output_location=output_location,
            max_waiting_tries=max_waiting_tries,
            workgroup=workgroup,
        )

        result_data = s3.read_from_s3(result_key)

        if result_data is None:
            raise Exception("Failed to read query results from S3")

        df = pd.read_csv(io.StringIO(result_data.decode("utf-8")))

        return df

    def remove_timestamp_partition_fields(self, dicts: list[dict]) -> list[dict]:
        """Remove fields related to timestamp partitions."""
        fields_to_remove = [
            "year",
            "month",
            "day",
            "hour",
            "minute",
        ]  # extra fields from preprocessing partitions.
        return [
            {k: v for k, v in post.items() if k not in fields_to_remove}
            for post in dicts
        ]

    def convert_nan_to_none(self, dicts: list[dict]) -> list[dict]:
        """Convert NaN values to None."""
        return [
            {k: (None if pd.isna(v) else v) for k, v in post.items()} for post in dicts
        ]

    def parse_converted_pandas_dicts(self, dicts: list[dict]) -> list[dict]:
        """Parse converted pandas dicts."""
        df_dicts = self.remove_timestamp_partition_fields(dicts)
        df_dicts = self.convert_nan_to_none(df_dicts)
        return df_dicts

    def get_latest_preprocessed_posts(
        self, timestamp: Optional[str] = None
    ) -> list[FilteredPreprocessedPostModel]:  # noqa
        where_filter = (
            f"preprocessing_timestamp > '{timestamp}'" if timestamp else "1=1"
        )  # noqa

        query = f"SELECT * FROM preprocessed_posts WHERE {where_filter}"

        df: pd.DataFrame = self.query_results_as_df(query)

        logger.info(f"Number of posts to classify: {len(df)}")

        df_dicts = df.to_dict(orient="records")
        # convert NaN values to None, remove extra fields.
        df_dicts = self.parse_converted_pandas_dicts(df_dicts)
        # remove values without text
        df_dicts_cleaned = [post for post in df_dicts if post["text"] is not None]

        # remove posts whose text fields are too short
        df_dicts_cleaned = [
            post
            for post in df_dicts_cleaned
            if len(post["text"]) > MIN_POST_TEXT_LENGTH
        ]  # noqa

        # convert to pydantic model
        posts_to_classify = [
            FilteredPreprocessedPostModel(**post) for post in df_dicts_cleaned
        ]

        return posts_to_classify
