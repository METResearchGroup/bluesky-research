"""Database connection and setup for storing network data."""
import os
import peewee
import sqlite3

from lib.helper import create_batches
from services.update_network_connections.models import (
    ConnectionModel, UserSocialNetworkCountsModel, UserToConnectionModel
)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "network_connections.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

insert_batch_size = 100


class BaseModel(peewee.Model):
    class Meta:
        database = db


class UserToConnection(BaseModel):
    """Class for tracking user connections."""
    study_user_id = peewee.CharField()
    user_did = peewee.CharField()
    user_handle = peewee.CharField()
    connection_did = peewee.CharField()
    connection_handle = peewee.CharField()
    connection_display_name = peewee.CharField()
    user_follows_connection = peewee.BooleanField()
    connection_follows_user = peewee.BooleanField()
    synctimestamp = peewee.CharField()

    class Meta:
        primary_key = peewee.CompositeKey('user_did', 'connection_did')


class UserSocialNetworkCounts(BaseModel):
    """Class for tracking user social network counts."""
    study_user_id = peewee.CharField()
    user_did = peewee.CharField()
    user_handle = peewee.CharField()
    user_followers_count = peewee.IntegerField()
    user_following_count = peewee.IntegerField()
    synctimestamp = peewee.CharField()


def create_initial_tables():
    db.connect()
    db.create_tables([UserToConnection, UserSocialNetworkCounts])


def batch_insert_network_connections(user_connections: list[UserToConnectionModel]):  # noqa
    with db.atomic():
        batches = create_batches(user_connections, insert_batch_size)
        for batch in batches:
            batch_dicts = [connections.dict() for connections in batch]
            UserToConnection.insert_many(batch_dicts).on_conflict_replace().execute()  # noqa
    print(f"Inserted {len(user_connections)} network connections.")


def get_connection_dids_for_user(user_did: str) -> set[str]:
    res = list(UserToConnection.select().where(UserToConnection.user_did == user_did))  # noqa
    connection_dids = set([r.connection_did for r in res])
    return connection_dids


def insert_user_network_counts(user_network_counts: UserSocialNetworkCountsModel):
    UserSocialNetworkCounts.insert(**user_network_counts.dict()).on_conflict_replace().execute()  # noqa
    print(f"Inserted user network counts for user {user_network_counts.study_user_id}.")  # noqa


def batch_insert_user_network_counts(user_network_counts: list[UserSocialNetworkCountsModel]):  # noqa
    with db.atomic():
        batches = create_batches(user_network_counts, insert_batch_size)
        for batch in batches:
            batch_dicts = [counts.dict() for counts in batch]
            UserSocialNetworkCounts.insert_many(batch_dicts).on_conflict_replace().execute()  # noqa
    print(f"Inserted {len(user_network_counts)} user network counts.")


def get_user_network_counts() -> list[UserSocialNetworkCountsModel]:
    res = list(UserSocialNetworkCounts.select())
    res_dicts: list[dict] = [r.__dict__['__data__'] for r in res]
    transformed_res = [
        UserSocialNetworkCountsModel(**res_dict) for res_dict in res_dicts
    ]
    return transformed_res
