"""Helper functions for participant data service."""
import peewee
import os
import sqlite3

import pandas as pd


current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "study_participants.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class StudyUser(BaseModel):
    id = peewee.AutoField()
    name = peewee.CharField(unique=True)


class StudyQuestionnaireData(BaseModel):
    id = peewee.AutoField()
    user_id = peewee.ForeignKeyField(StudyUser, backref="questionnaire_data")
    questionnaire_data = peewee.TextField()


class UserToBlueskyProfile(BaseModel):
    study_user_id = peewee.CharField(unique=True, primary_key=True)
    bluesky_handle = peewee.CharField(unique=True)
    bluesky_user_did = peewee.CharField(unique=True)
    condition = peewee.CharField()


def create_initial_tables(drop_all_tables: bool = False) -> None:
    """Create the initial tables, optionally dropping all existing tables first.

    :param drop_all_tables: If True, drops all existing tables before creating new ones.
    """  # noqa
    with db.atomic():
        if drop_all_tables:
            print("Dropping all tables in database...")
            db.drop_tables([StudyUser, StudyQuestionnaireData,
                           UserToBlueskyProfile], safe=True)
        print("Creating tables in database...")
        db.create_tables(
            [StudyUser, StudyQuestionnaireData, UserToBlueskyProfile])


def insert_test_user_to_bsky_profile() -> None:
    """Inserts a test user to the bluesky profile table."""
    user_id = 999
    bluesky_handle = "markptorres.bsky.social"
    bluesky_user_did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    data = {
        "study_user_id": user_id,
        "condition": "reverse_chronological",
        "bluesky_handle": bluesky_handle,
        "bluesky_user_did": bluesky_user_did
    }
    with db.atomic():
        UserToBlueskyProfile.create(**data)


def test_insertion() -> None:
    """Tests if table creation is successful. Will fail if the test has already
    been run before, so should only be run at the start.
    """
    map_table_name = UserToBlueskyProfile._meta.table_name
    print(f"Testing insertion of a user into the {map_table_name} table...")
    insert_test_user_to_bsky_profile()
    # check that there is at least one row
    num_users = UserToBlueskyProfile.select().count()
    assert num_users > 0
    print(f"Successfully inserted a user into the {map_table_name} table.")
    print(f"Total number of users in the {map_table_name} table: {num_users}")


def get_user_to_bluesky_profiles() -> list[UserToBlueskyProfile]:
    """Get all user to bluesky profiles."""
    return UserToBlueskyProfile.select()


def get_user_to_bluesky_profiles_as_list_dicts() -> list[dict]:
    """Get all user to bluesky profiles as a list of dictionaries."""
    return list(get_user_to_bluesky_profiles().dicts())


def get_user_to_bluesky_profiles_as_df() -> pd.DataFrame:
    """Get all user to bluesky profiles as a DataFrame."""
    return pd.DataFrame(get_user_to_bluesky_profiles_as_list_dicts())


def initialize_study_tables(drop_all_tables: bool = False) -> None:
    """Initialize the study tables."""
    create_initial_tables(drop_all_tables=drop_all_tables)
    test_insertion()
    print("Tables created successfully.")


if __name__ == "__main__":
    initialize_study_tables(True)
