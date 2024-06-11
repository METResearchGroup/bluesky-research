"""Database connection and setup for the participant data database."""
import hashlib
import os
import peewee
import sqlite3

from services.participant_data.models import UserToBlueskyProfileModel

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
            db.drop_tables([UserToBlueskyProfile], safe=True)
        print("Creating tables in database...")
        db.create_tables([UserToBlueskyProfile])


def insert_bsky_user_to_study(
    bluesky_handle: str, condition: str, bluesky_user_did: str
) -> None:
    """Insert a user into the study."""
    study_user_id = hashlib.sha256(bluesky_handle.encode()).hexdigest()
    user_exists = bool(
        UserToBlueskyProfile.get_or_none(bluesky_handle=bluesky_handle)
    )
    if user_exists:
        raise ValueError(f"User with handle {bluesky_handle} already exists.")  # noqa
    with db.atomic():
        data = {
            "study_user_id": study_user_id,
            "bluesky_handle": bluesky_handle,
            "bluesky_user_did": bluesky_user_did,
            "condition": condition,
        }
        user_data = UserToBlueskyProfileModel(**data)
        UserToBlueskyProfile.create(**user_data.dict())


def get_user_to_bluesky_profiles() -> list[UserToBlueskyProfile]:
    """Get all user to bluesky profiles."""
    return UserToBlueskyProfile.select()


def get_users_in_condition(condition: str) -> list[UserToBlueskyProfile]:
    """Get all users in a given condition."""
    return UserToBlueskyProfile.select().where(
        UserToBlueskyProfile.condition == condition
    )


if __name__ == "__main__":
    # create_initial_tables(drop_all_tables=True)
    # print("Database setup complete.")
    pass
