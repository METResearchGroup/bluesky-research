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
