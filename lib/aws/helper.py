"""Helper tooling for interacting with AWS services."""

from dotenv import load_dotenv
import os
import time

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ProfileNotFound

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../../.env"))
load_dotenv(env_path)

AWS_PROFILE_NAME = os.getenv("AWS_PROFILE_NAME")
AWS_REGION = "us-east-2"
os.environ["AWS_DEFAULT_REGION"] = AWS_REGION


try:
    session = boto3.Session(profile_name=AWS_PROFILE_NAME, region_name=AWS_REGION)
except ProfileNotFound:
    print(f"Profile {AWS_PROFILE_NAME} not found. Falling back to default credentials.")
    session = boto3.Session(region_name=AWS_REGION)
    print("Session created from default credentials.")


def create_client(client_name: str) -> BaseClient:
    """Creates generic AWS client."""
    return session.client(client_name, region_name=AWS_REGION)


def retry_on_aws_rate_limit(
    func, retries: int = 3, use_exponential_backoff: bool = True
):
    """Decorator to retry on AWS rate limit."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "Rate exceeded" in str(e):
                print("Rate limit exceeded. Retrying...")
                if retries > 0:
                    if use_exponential_backoff:
                        time.sleep(2 ** (3 - retries))
                    return retry_on_aws_rate_limit(func, retry_limit=retries - 1)(
                        *args, **kwargs
                    )
                return func(*args, **kwargs)
            raise e

    return wrapper
