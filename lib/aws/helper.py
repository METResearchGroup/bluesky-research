"""Helper tooling for interacting with AWS services."""
from dotenv import load_dotenv
import os

import boto3
from botocore.client import BaseClient

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../../.env"))
load_dotenv(env_path)

AWS_PROFILE_NAME = os.getenv("AWS_PROFILE_NAME")
session = boto3.Session(profile_name=AWS_PROFILE_NAME)


def create_client(client_name: str) -> BaseClient:
    """Creates generic AWS client."""
    return session.client(client_name)


def list_buckets(s3_client: BaseClient) -> list[str]:
    """Lists buckets available in S3."""
    response = s3_client.list_buckets()
    return [bucket['Name'] for bucket in response['Buckets']]


if __name__ == "__main__":
    s3_client = create_client("s3")
    buckets = list_buckets(s3_client)
    print(buckets)
