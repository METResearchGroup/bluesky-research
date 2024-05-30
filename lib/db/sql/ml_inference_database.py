"""Database logic for storing results of ML inference."""
import peewee
from peewee import (
    Model, CharField, TextField, IntegerField, FloatField, BooleanField,
    Optional, SqliteDatabase, DoesNotExist
)
import os
import sqlite3
import typing_extensions as te

from lib.log.logger import Logger

current_file_directory = os.path.dirname(os.path.abspath(__file__))
ML_INFERENCE_SQLITE_DB_NAME = "ml_inference.db"
ML_INFERENCE_SQLITE_DB_PATH = os.path.join(
    current_file_directory, ML_INFERENCE_SQLITE_DB_NAME
)

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
    processed_timestamp = CharField()
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
    label_toxic = IntegerField(null=True)
    prob_severe_toxic = FloatField(null=True)
    label_severe_toxic = IntegerField(null=True)
    prob_identity_attack = FloatField(null=True)
    label_identity_attack = IntegerField(null=True)
    prob_insult = FloatField(null=True)
    label_insult = IntegerField(null=True)
    prob_profanity = FloatField(null=True)
    label_profanity = IntegerField(null=True)
    prob_threat = FloatField(null=True)
    label_threat = IntegerField(null=True)
    prob_affinity = FloatField(null=True)
    label_affinity = IntegerField(null=True)
    prob_compassion = FloatField(null=True)
    label_compassion = IntegerField(null=True)
    prob_constructive = FloatField(null=True)
    label_constructive = IntegerField(null=True)
    prob_curiosity = FloatField(null=True)
    label_curiosity = IntegerField(null=True)
    prob_nuance = FloatField(null=True)
    label_nuance = IntegerField(null=True)
    prob_personal_story = FloatField(null=True)
    label_personal_story = IntegerField(null=True)
    prob_reasoning = FloatField(null=True)
    label_reasoning = IntegerField(null=True)
    prob_respect = FloatField(null=True)
    label_respect = IntegerField(null=True)
    prob_alienation = FloatField(null=True)
    label_alienation = IntegerField(null=True)
    prob_fearmongering = FloatField(null=True)
    label_fearmongering = IntegerField(null=True)
    prob_generalization = FloatField(null=True)
    label_generalization = IntegerField(null=True)
    prob_moral_outrage = FloatField(null=True)
    label_moral_outrage = IntegerField(null=True)
    prob_scapegoating = FloatField(null=True)
    label_scapegoating = IntegerField(null=True)
    prob_sexually_explicit = FloatField(null=True)
    label_sexually_explicit = IntegerField(null=True)
    prob_flirtation = FloatField(null=True)
    label_flirtation = IntegerField(null=True)
    prob_spam = FloatField(null=True)
    label_spam = IntegerField(null=True)

class SociopoliticalLabels(BaseModel):
    """Stores results of sociopolitical and political ideology labels from
    the LLM."""
    uri = CharField(unique=True, index=True)
    text = TextField()
    was_successfully_labeled = BooleanField()
    reason = TextField(null=True)
    label_timestamp = CharField()
    is_sociopolitical = BooleanField()
    political_ideology_label = CharField(null=True)
    reason_sociopolitical = TextField()
    reason_political_ideology = TextField(null=True)


def create_initial_tables() -> None:
    """Create the initial tables."""
    with db.atomic():
        db.create_tables([
            RecordClassificationMetadata,
            PerspectiveApiLabels,
            SociopoliticalLabels
        ])


if __name__ == "__main__":
    if db.is_closed():
        db.connect()
    create_initial_tables()
    db.close()
