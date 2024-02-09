"""Helper tooling for interacting with AWS services."""
from dotenv import load_dotenv
import os
import time

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


def retry_on_aws_rate_limit(
    func,
    retries: int = 3,
    use_exponential_backoff: bool = True
):
    """Decorator to retry on AWS rate limit."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "Rate exceeded" in str(e):
                print(f"Rate limit exceeded. Retrying...")
                if retries > 0:
                    if use_exponential_backoff:
                        time.sleep(2 ** (3 - retries))
                    return retry_on_aws_rate_limit(
                        func, retry_limit=retries - 1
                    )(*args, **kwargs)
                return func(*args, **kwargs)
            raise e
    return wrapper
