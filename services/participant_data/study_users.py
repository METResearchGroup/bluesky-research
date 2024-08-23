"""Tracks the users in the study, for checking against the streamed data.

Performs the following tasks:
- Checks to see if user activity (posts/likes/follows) is from a study user.
- Checks to see if a repost is of a post from a study user.
- Checks to see if a like is of a post from a study user.
"""

import json
import os
from typing import Optional

import boto3

from lib.aws.athena import Athena
from services.participant_data.helper import get_all_users

athena = Athena()


class StudyUserManager:
    """Singleton class for managing study users and their posts.

    We use this class for tracking the users in the study, for checking against
    the streamed data.
    """

    _instance = None

    @staticmethod
    def get_instance(**kwargs):
        if StudyUserManager._instance is None:
            StudyUserManager._instance = StudyUserManager(**kwargs)
        return StudyUserManager._instance

    def __init__(self, load_from_aws: bool = True, use_new_hashmap: bool = False):
        if StudyUserManager._instance is not None:
            raise Exception(
                "StudyUserManager class is intended to be a singleton instance."
            )  # noqa
        self.s3 = boto3.client("s3")
        self.s3_bucket = "bluesky-research"
        self.file_to_key_map = {
            "study_user_dids": os.path.join("participant_data", "study_user_dids.json"),
            "post_uri_to_study_user_did": os.path.join(
                "participant_data", "post_uri_to_study_user_did.json"
            ),
        }
        if load_from_aws:
            print("Using AWS version of StudyUserManager class.")
            self.study_users_dids_set: set = self._load_study_user_dids()
            self.post_uri_to_study_user_did_map: dict = (
                self._load_post_uri_to_study_user_did_map_from_s3(
                    use_new_hashmap=use_new_hashmap
                )
            )
            self.in_network_user_dids_set: set = self._load_in_network_user_dids()  # noqa
        else:
            print("Using local version of StudyUserManager class.")
            self.study_users_dids_set = set()
            self.post_uri_to_study_user_did_map = {}
            self.in_network_user_dids_set = set()

    def _load_study_user_dids(self) -> set[str]:
        """Load the study_users_dids_set from DynamoDB."""
        print(
            """
            THIS SHOULD ONLY BE CALLED AT THE BEGINNING OF THE STREAMING
            PROCESS
            """
        )
        print(
            """
            Since this is a singleton class, the study users should only
            be loaded once at the beginning of the streaming process.
            """
        )
        study_users = get_all_users()
        return set([user.bluesky_user_did for user in study_users])

    def _load_in_network_user_dids(self, is_refresh: bool = False) -> set[str]:
        """Load the in_network_user_dids_set from DynamoDB."""
        if is_refresh:
            print("Refreshing the list of in-network user DIDs.")
        else:
            print(
                """
                THIS SHOULD ONLY BE CALLED AT THE BEGINNING OF THE STREAMING
                PROCESS
                """
            )
            print(
                """
                Since this is a singleton class, the in-network user DIDs should
                only be loaded once at the beginning of the streaming process.
                """
            )
        query = """
        SELECT 
            CASE 
                WHEN relationship_to_study_user = 'follow' THEN follow_did
                WHEN relationship_to_study_user = 'follower' THEN follower_did
            END AS did
        FROM user_social_networks
        WHERE relationship_to_study_user IN ('follow', 'follower')
        """
        df = athena.query_results_as_df(query)
        user_dids = set(df["did"].tolist())
        print(f"Loaded {len(user_dids)} in-network user DIDs from Athena.")
        return user_dids

    def _load_post_uri_to_study_user_did_map_from_s3(
        self, use_new_hashmap: bool = False
    ) -> dict:
        """Load the post_uri_to_study_user_did_map from S3."""
        key = self.file_to_key_map["post_uri_to_study_user_did"]
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=key)
            post_uri_to_study_user_did_map = json.loads(response["Body"].read())
        except self.s3.exceptions.NoSuchKey or use_new_hashmap:
            post_uri_to_study_user_did_map = {}
        return post_uri_to_study_user_did_map

    def _load_aws_data(self):
        """Load the study user data from AWS."""
        self.study_users_dids_set = self._load_study_user_dids()
        self.post_uri_to_study_user_did_map = (
            self._load_post_uri_to_study_user_did_map_from_s3()
        )

    def _write_post_uri_to_study_user_did_map_to_s3(self):
        """Write the post_uri_to_study_user_did_map to S3."""
        key = self.file_to_key_map["post_uri_to_study_user_did"]
        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=json.dumps(self.post_uri_to_study_user_did_map),
        )

    def is_study_user(self, user_did: str) -> bool:
        """Check if a user is in the study."""
        return user_did in self.study_users_dids_set

    def is_study_user_post(self, post_uri: str) -> Optional[str]:
        """Check if a post is from a study user."""
        return self.post_uri_to_study_user_did_map.get(post_uri, None)

    def insert_study_user_post(self, post_uri: str, user_did: str):
        """Insert a post from a study user."""
        self.post_uri_to_study_user_did_map[post_uri] = user_did
        self._write_post_uri_to_study_user_did_map_to_s3()

    def is_in_network_user(self, user_did: str) -> bool:
        """Check if a user is in the in-network user DIDs."""
        return user_did in self.in_network_user_dids_set


def get_study_user_manager(load_from_aws: bool = True):
    """Get the singleton instance of the StudyUserManager."""
    return StudyUserManager.get_instance(load_from_aws=load_from_aws)
