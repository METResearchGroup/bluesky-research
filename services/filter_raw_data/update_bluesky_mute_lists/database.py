"""Database logic for storing users who are part of mute lists (and should
therefore be muted in our system)."""
from datetime import datetime
import os
import sqlite3

import peewee


current_file_directory = os.path.dirname(os.path.abspath(__file__))

SQLITE_DB_NAME = "mutelist.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

class BaseModel(peewee.Model):
    class Meta:
        database = db


class MuteList(BaseModel):
    """Model for the mute list."""
    uri = peewee.CharField(unique=True)
    cid = peewee.CharField(unique=True)
    name = peewee.CharField()
    description = peewee.CharField()
    author_did = peewee.CharField()
    author_handle = peewee.CharField()


class MutedUser(BaseModel):
    """Model for a user who is muted."""
    did = peewee.CharField(unique=True)
    handle = peewee.CharField()
    source_list_uri = peewee.CharField()
    source_list_name = peewee.CharField()
    timestamp_added = peewee.CharField()


def create_initial_tables():
    """Creates the initial tables for the mute list database."""
    with db.atomic():
        db.create_tables([MuteList, MutedUser])


def batch_create_mute_lists(mute_lists: list[dict]):
    """Batch creates mute lists."""
    with db.atomic():
        MuteList.insert_many(mute_lists).on_conflict_ignore().execute()
    print(f"Inserted {len(mute_lists)} mute lists.")


def batch_create_muted_users(muted_users: list[dict]):
    """Batch creates muted users."""
    with db.atomic():
        MutedUser.insert_many(muted_users).on_conflict_ignore().execute()
    print(f"Inserted {len(muted_users)} muted users.")


if __name__ == "__main__":
    create_initial_tables()
    print("Tables created.")
