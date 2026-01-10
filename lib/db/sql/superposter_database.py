""" "Database for storing superposters."""

import os
import peewee
import sqlite3

from lib.batching_utils import create_batches
from services.calculate_superposters.models import SuperposterModel

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "superposters.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

insert_batch_size = 100


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Superposter(BaseModel):
    """Table for storing superposters per day.

    In practice, we will pull the superposters from a particular
    `superposter_date`.
    """

    user_did = peewee.CharField()
    user_handle = peewee.CharField()
    number_of_posts = peewee.IntegerField()
    superposter_date = peewee.CharField()  # YYYY-MM-DD
    insert_timestamp = peewee.CharField()


def create_initial_tables(drop_all_tables: bool = False) -> None:
    """Create the initial tables, optionally dropping all existing tables first.

    :param drop_all_tables: If True, drops all existing tables before creating new ones.
    """  # noqa
    with db.atomic():
        if drop_all_tables:
            print("Dropping all tables in database...")
            db.drop_tables([Superposter], safe=True)
        print("Creating tables in database...")
        db.create_tables([Superposter])


def batch_insert_superposters(superposters: list[SuperposterModel]) -> None:
    """Insert superposters into the database."""
    with db.atomic():
        batches = create_batches(superposters, insert_batch_size)
        for batch in batches:
            batch_dicts = [superposter.dict() for superposter in batch]
            Superposter.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(superposters)} superposters into the database.")  # noqa


def get_superposters(superposter_date: str) -> list[SuperposterModel]:
    """Get superposters for a given date."""
    res = Superposter.select().where(Superposter.superposter_date == superposter_date)  # noqa
    res_dicts = [superposter.__dict__ for superposter in res]
    transformed_res = [SuperposterModel(**superposter) for superposter in res_dicts]  # noqa
    return transformed_res


if db.is_closed():
    db.connect()
    db.create_tables([Superposter])


if __name__ == "__main__":
    # create_initial_tables(drop_all_tables=True)
    # print("Superposter database setup complete.")
    pass
