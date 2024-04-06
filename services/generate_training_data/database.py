"""Database logic for training data.

To keep this generic, we'll just have a basic schema for now. We'll serialize
the labels and have specific tasks that we specify.
"""
import os
import peewee
import sqlite3
from typing import Optional

current_file_directory = os.path.dirname(os.path.abspath(__file__))

SQLITE_DB_NAME = "annotated_training_data.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

class BaseModel(peewee.Model):
    class Meta:
        database = db

class AnnotatedTrainingData(BaseModel):
    uri = peewee.CharField()
    label = peewee.TextField() # serialized dict of training label
    task = peewee.CharField()
    notes = peewee.CharField()
    timestamp = peewee.CharField()


def create_initial_table() -> None:
    """Create initial training data table."""
    with db.atomic():
        db.create_tables([AnnotatedTrainingData])
    num_labels = AnnotatedTrainingData.select().count()
    if num_labels is not None:
        table_name = AnnotatedTrainingData._meta.table_name
        print(f"{table_name} exists, and has {num_labels} labels.")


def write_training_data_to_db(data: dict) -> None:
    """Write data to training data table."""
    with db.atomic():
        AnnotatedTrainingData.create(**data)


def load_ids_of_previous_annotated_samples(
    task_name: Optional[str] = None
) -> list[str]:
    """Load the ids of previously annotated samples."""
    if task_name:
        ids = AnnotatedTrainingData.select(AnnotatedTrainingData.id).where(
            AnnotatedTrainingData.task == task_name
        )
    else:
        ids = AnnotatedTrainingData.select(AnnotatedTrainingData.id)
    return [id_ for id_ in ids]


if __name__ == "__main__":
    create_initial_table()
