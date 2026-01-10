"""Database connection and setup for storing network data."""

import os
import peewee
import sqlite3

from lib.batching_utils import create_batches
from services.update_network_connections.models import (
    UserSocialNetworkCountsModel,
    UserToConnectionModel,
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
        primary_key = peewee.CompositeKey("user_did", "connection_did")


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


def get_user_connections(
    user_did: str, connection_dids_list: list[str]
) -> list[UserToConnectionModel]:  # noqa
    """
    Retrieves a list of user connections filtered by a specific user ID and a list of connection IDs.

    Args:
        user_did (str): The user ID to filter connections by.
        connection_dids_list (list[str]): A list of connection IDs to include in the results.

    Returns:
        list[UserToConnectionModel]: A list of UserToConnectionModel instances representing the filtered user connections.
    """  # noqa
    res = list(
        UserToConnection.select().where(
            (UserToConnection.user_did == user_did)
            & (UserToConnection.connection_did << connection_dids_list)
        )
    )
    res_dicts: list[dict] = [r.__dict__["__data__"] for r in res]
    transformed_res = [UserToConnectionModel(**res_dict) for res_dict in res_dicts]
    return transformed_res


def get_all_followed_connections() -> list[UserToConnectionModel]:
    """Get all connections that are followed by a user in the study.

    We need to dedupe on did and handle since we have a unique row on the
    combination of study user + account, so if multiple users follow the same
    account, we only want to count that account once.
    """
    res = list(
        UserToConnection.select().where(UserToConnection.user_follows_connection)
    )  # noqa
    res_dicts: list[dict] = [r.__dict__["__data__"] for r in res]
    transformed_res = [UserToConnectionModel(**res_dict) for res_dict in res_dicts]
    deduped_res: list[UserToConnectionModel] = []
    seen_connection_did = set()
    seen_connection_handle = set()
    for res in transformed_res:
        if (
            res.connection_did not in seen_connection_did
            and res.connection_handle not in seen_connection_handle
        ):
            deduped_res.append(res)
            seen_connection_did.add(res.connection_did)
            seen_connection_handle.add(res.connection_handle)
    return deduped_res


def insert_user_network_counts(user_network_counts: UserSocialNetworkCountsModel):  # noqa
    UserSocialNetworkCounts.insert(
        **user_network_counts.dict()
    ).on_conflict_replace().execute()  # noqa
    print(f"Inserted user network counts for user {user_network_counts.study_user_id}.")  # noqa


def batch_insert_user_network_counts(
    user_network_counts: list[UserSocialNetworkCountsModel],
):  # noqa
    with db.atomic():
        batches = create_batches(user_network_counts, insert_batch_size)
        for batch in batches:
            batch_dicts = [counts.dict() for counts in batch]
            UserSocialNetworkCounts.insert_many(
                batch_dicts
            ).on_conflict_replace().execute()  # noqa
    print(f"Inserted {len(user_network_counts)} user network counts.")


def get_user_network_counts() -> list[UserSocialNetworkCountsModel]:
    res = list(UserSocialNetworkCounts.select())
    res_dicts: list[dict] = [r.__dict__["__data__"] for r in res]
    transformed_res = [
        UserSocialNetworkCountsModel(**res_dict) for res_dict in res_dicts
    ]
    return transformed_res


if __name__ == "__main__":
    create_initial_tables()
    # get total network counts per user handle and then print
    user_network_counts = get_user_network_counts()
    for user_network_count in user_network_counts:
        user_handle = user_network_count.user_handle
        user_followers_count = user_network_count.user_followers_count
        user_following_count = user_network_count.user_following_count
        print(
            f"User handle: {user_handle}, followers: {user_followers_count}, following: {user_following_count}"
        )  # noqa
