"""Wrapper class for all Athena-related access."""
import time

import pandas as pd

from lib.aws.helper import create_client
from lib.aws.s3 import S3

DEFAULT_DB_NAME = "default-db"

# NOTE: the output location must correspond to the workgroup's output location
# set in the Terraform config.
DEFAULT_OUTPUT_LOCATION = f"s3://bluesky-research/athena-results"
DEFAULT_MAX_WAITING_TRIES = 5
DEFAULT_WORKGROUP = "prod_workgroup"

s3 = S3()


class Athena:
    def __init__(self):
        self.client = create_client("athena")

    def run_query(
        self,
        query: str,
        db_name: str = DEFAULT_DB_NAME,
        output_location: str = DEFAULT_OUTPUT_LOCATION,
        max_waiting_tries: int = DEFAULT_MAX_WAITING_TRIES,
        workgroup: str = DEFAULT_WORKGROUP
    ):
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": db_name},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup
        )
        query_execution_id = response['QueryExecutionId']
        status = 'RUNNING'

        num_waits = 0

        while status in ['RUNNING', 'QUEUED']:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Query {status}: {response['QueryExecution']['Status']['StateChangeReason']}")
            time.sleep(5)
            num_waits += 1
            if num_waits >= max_waiting_tries:
                raise Exception(f"Query exceeds max waiting tries: {status}")

        # Fetch query results
        result_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        bucket_name = result_location.split('/')[2]
        key = '/'.join(result_location.split('/')[3:])

        return bucket_name, key

    def query_results_as_df(
        self,
        query: str,
        db_name: str = DEFAULT_DB_NAME,
        output_location: str = DEFAULT_OUTPUT_LOCATION,
        max_waiting_tries: int = DEFAULT_MAX_WAITING_TRIES,
        workgroup: str = DEFAULT_WORKGROUP
    ):

        _, result_key = self.run_query(
            query=query,
            db_name=db_name,
            output_location=output_location,
            max_waiting_tries=max_waiting_tries,
            workgroup=workgroup
        )

        result_data = s3.read_from_s3(result_key)

        if result_data is None:
            raise Exception("Failed to read query results from S3")

        # Assuming the result is in CSV format
        df = pd.read_csv(pd.compat.StringIO(result_data.decode('utf-8')))

        return df
