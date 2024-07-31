"""Tracks the users in the study, for checking against the streamed data.

Performs the following tasks:
- Checks to see if user activity (posts/likes/follows) is from a study user.
- Checks to see if a repost is of a post from a study user.
- Checks to see if a like is of a post from a study user.
"""
import json
import os
import threading

import boto3
from services.participant_data.helper import get_all_users


class StudyUserManager:
    """Singleton class for managing study users and their posts.

    We use this class for tracking the users in the study, for checking against
    the streamed data.
    """
    _instance = None
    _lock = threading.Lock()

    _instance = None

    @staticmethod
    def get_instance():
        if StudyUserManager._instance is None:
            StudyUserManager._instance = StudyUserManager()
        return StudyUserManager._instance

    def __init__(self):
        if StudyUserManager._instance is not None:
            raise Exception("StudyUserManager class is intended to be a singleton instance.")  # noqa
        self.s3 = boto3.client("s3")
        self.s3_bucket = "bluesky-research"
        self.study_users_dids_set: set = self._load_study_user_dids_from_s3()
        self.post_uri_to_study_user_did_map: dict = (
            self._load_post_uri_to_study_user_did_map_from_s3()
        )
        self.file_to_key_map = {
            "study_user_dids": os.path.join(
                "participant_data", "study_user_dids.json"
            ),
            "post_uri_to_study_user_did": os.path.join(
                "participant_data", "post_uri_to_study_user_did.json"
            )
        }

    def _load_study_user_dids(self):
        """Load the study_users_dids_set from DynamoDB."""
        study_users = get_all_users()
        self.study_users_dids_set = set(
            [user.bluesky_user_did for user in study_users]
        )

    def _load_post_uri_to_study_user_did_map_from_s3(self):
        """Load the post_uri_to_study_user_did_map from S3."""
        key = self.file_to_key_map["post_uri_to_study_user_did"]
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            post_uri_to_study_user_did_map = json.loads(response["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            post_uri_to_study_user_did_map = {}
        self.post_uri_to_study_user_did_map = post_uri_to_study_user_did_map

    def _write_post_uri_to_study_user_did_map_to_s3(self):
        """Write the post_uri_to_study_user_did_map to S3."""
        key = self.file_to_key_map["post_uri_to_study_user_did"]
        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=json.dumps(self.post_uri_to_study_user_did_map)
        )

    def is_study_user(self, user_did: str) -> bool:
        """Check if a user is in the study."""
        return user_did in self.study_users_dids_set

    def is_study_user_post(self, post_uri: str) -> bool:
        """Check if a post is from a study user."""
        return post_uri in self.post_uri_to_study_user_did_map

    def insert_study_user_post(self, post_uri: str, user_did: str):
        """Insert a post from a study user."""
        self.post_uri_to_study_user_did_map[post_uri] = user_did
        self._write_post_uri_to_study_user_did_map_to_s3()


study_user_manager = StudyUserManager.get_instance()
