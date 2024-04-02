"""Helper functions for participant data service."""
import peewee
import os
import sqlite3


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
    study_user_id = peewee.CharField()
    bluesky_handle = peewee.CharField()
    bluesky_user_did = peewee.CharField()
    PRIMARY_KEY = peewee.CompositeKey("study_user_id", "bluesky_handle", "bluesky_user_did")


def create_initial_tables() -> None:
    """Create the initial tables."""
    with db.atomic():
        db.create_tables([StudyUser, StudyQuestionnaireData, UserToBlueskyProfile])


def test_insertion() -> None:
    """Tests if table creation is successful. Will fail if the test has already
    been run before, so should only be run at the start.
    """
    map_table_name = "user_to_bsky_profile"
    print(f"Testing insertion of a user into the {map_table_name} table...")
    user_id = 999
    bluesky_handle = "markptorres.bsky.social"
    bluesky_user_did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    data = {
        "study_user_id": user_id,
        "bluesky_handle": bluesky_handle,
        "bluesky_user_did": bluesky_user_did   
    }
    with db.atomic():
        UserToBlueskyProfile.create(**data)


def initialize_study_tables() -> None:
    """Initialize the study tables."""
    create_initial_tables()
    test_insertion()
    print("Tables created successfully.")