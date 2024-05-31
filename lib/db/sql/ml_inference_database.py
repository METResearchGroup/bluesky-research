"""Database logic for storing results of ML inference."""
import peewee
from peewee import (
     CharField, TextField, IntegerField, FloatField, BooleanField
)
import os
import sqlite3
from typing import Union

from lib.helper import create_batches
from lib.log.logger import Logger

from services.ml_inference.models import (
    RecordClassificationMetadataModel, PerspectiveApiLabelsModel,
    SociopoliticalLabelsModel
)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
ML_INFERENCE_SQLITE_DB_NAME = "ml_inference.db"
ML_INFERENCE_SQLITE_DB_PATH = os.path.join(
    current_file_directory, ML_INFERENCE_SQLITE_DB_NAME
)
metadata_insert_batch_size = 100
label_insert_batch_size = 100

db = peewee.SqliteDatabase(ML_INFERENCE_SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(ML_INFERENCE_SQLITE_DB_PATH)
cursor = conn.cursor()

logger = Logger(__name__)

class BaseModel(peewee.Model):
    class Meta:
        database = db

class RecordClassificationMetadata(BaseModel):
    """Metadata for the classification of a record."""
    uri = CharField(unique=True, index=True)
    text = TextField()
    synctimestamp = CharField()
    preprocessing_timestamp = CharField()
    source = CharField(
        choices=[("firehose", "firehose"), ("most_liked", "most_liked")]
    )
    url = CharField(null=True)
    like_count = IntegerField(null=True)
    reply_count = IntegerField(null=True)
    repost_count = IntegerField(null=True)

class PerspectiveApiLabels(BaseModel):
    """Stores results of classifications from Perspective API."""
    uri = CharField(unique=True, index=True)
    text = TextField()
    was_successfully_labeled = BooleanField()
    reason = TextField(null=True)
    label_timestamp = CharField()
    prob_toxic = FloatField(null=True)
    prob_severe_toxic = FloatField(null=True)
    prob_identity_attack = FloatField(null=True)
    prob_insult = FloatField(null=True)
    prob_profanity = FloatField(null=True)
    prob_threat = FloatField(null=True)
    prob_affinity = FloatField(null=True)
    prob_compassion = FloatField(null=True)
    prob_constructive = FloatField(null=True)
    prob_curiosity = FloatField(null=True)
    prob_nuance = FloatField(null=True)
    prob_personal_story = FloatField(null=True)
    prob_reasoning = FloatField(null=True)
    prob_respect = FloatField(null=True)
    prob_alienation = FloatField(null=True)
    prob_fearmongering = FloatField(null=True)
    prob_generalization = FloatField(null=True)
    prob_moral_outrage = FloatField(null=True)
    prob_scapegoating = FloatField(null=True)
    prob_sexually_explicit = FloatField(null=True)
    prob_flirtation = FloatField(null=True)
    prob_spam = FloatField(null=True)

class SociopoliticalLabels(BaseModel):
    """Stores results of sociopolitical and political ideology labels from
    the LLM."""
    uri = CharField(unique=True, index=True)
    text = TextField()
    model_name = TextField()
    was_successfully_labeled = BooleanField()
    reason = TextField(null=True)
    label_timestamp = CharField()
    is_sociopolitical = BooleanField()
    political_ideology_label = CharField(null=True)
    reason_sociopolitical = TextField()
    reason_political_ideology = TextField(null=True)


def batch_insert_metadata(
    metadata_lst: list[RecordClassificationMetadataModel]
) -> None:
    """Batch insert metadata into the database."""
    print(f"Inserting {len(metadata_lst)} metadata into the database.")
    record_count = RecordClassificationMetadata.select().count()
    print(f"Metadata count prior to insertion: {record_count}")
    with db.atomic():
        batches = create_batches(metadata_lst, metadata_insert_batch_size)
        for batch in batches:
            RecordClassificationMetadata.insert_many(batch).execute()
    print(f"Finished inserting {len(metadata_lst)} metadata into the database.")
    record_count = RecordClassificationMetadata.select().count()
    print(f"Metadata count after insertion: {record_count}")


def batch_insert_perspective_api_labels(
    labels: list[PerspectiveApiLabelsModel]
) -> None:
    """Batch insert Perspective API labels into the database."""
    print(f"Inserting {len(labels)} labels into the database.")
    with db.atomic():
        batches = create_batches(labels, label_insert_batch_size)
        for batch in batches:
            PerspectiveApiLabels.insert_many(batch).execute()
    print(f"Finished inserting {len(labels)} labels into the database.")


def batch_insert_sociopolitical_labels(
    labels: list[SociopoliticalLabelsModel]
) -> None:
    """Batch insert sociopolitical labels into the database."""
    print(f"Inserting {len(labels)} labels into the database.")
    with db.atomic():
        batches = create_batches(labels, label_insert_batch_size)
        for batch in batches:
            SociopoliticalLabels.insert_many(batch).execute()
    print(f"Finished inserting {len(labels)} labels into the database.")


def batch_insert_labels(
    labels: Union[
        list[PerspectiveApiLabelsModel], list[SociopoliticalLabelsModel]
    ]
) -> None:
    """Batch insert labels into the database."""
    if isinstance(labels[0], PerspectiveApiLabelsModel):
        batch_insert_perspective_api_labels(labels)
    elif isinstance(labels[0], SociopoliticalLabelsModel):
        batch_insert_sociopolitical_labels(labels)
    else:
        raise ValueError("Invalid label type.")


def create_initial_tables() -> None:
    """Create the initial tables."""
    with db.atomic():
        db.create_tables([
            RecordClassificationMetadata,
            PerspectiveApiLabels,
            SociopoliticalLabels
        ])


if __name__ == "__main__":
    create_initial_tables()
