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
    llm_model_name = TextField()
    was_successfully_labeled = BooleanField()
    reason = TextField(null=True)
    label_timestamp = CharField()
    is_sociopolitical = BooleanField()
    political_ideology_label = CharField(null=True)
    reason_sociopolitical = TextField()
    reason_political_ideology = TextField(null=True)


def get_existing_metadata_uris() -> set[str]:
    """Get URIs of existing metadata."""
    uris = set()
    query = RecordClassificationMetadata.select(RecordClassificationMetadata.uri)
    for row in query:
        uris.add(row.uri)
    return uris


def batch_insert_metadata(
    metadata_lst: list[RecordClassificationMetadataModel]
) -> None:
    """Batch insert metadata into the database."""
    deduped_metadata_lst = []
    existing_uris = get_existing_metadata_uris()
    seen_uris = set()
    seen_uris.update(existing_uris)
    print(f"Attempting to insert {len(metadata_lst)} metadata objects into the database.") # noqa
    for metadata in metadata_lst:
        if metadata.uri not in seen_uris:
            deduped_metadata_lst.append(metadata)
            seen_uris.add(metadata.uri)
    print(f"Number of deduped metadata objects to insert: {len(deduped_metadata_lst)}") # noqa
    if len(deduped_metadata_lst) == 0:
        print("No metadata to insert.")
        return
    record_count = RecordClassificationMetadata.select().count()
    print(f"Metadata count prior to insertion: {record_count}")
    with db.atomic():
        batches = create_batches(deduped_metadata_lst, metadata_insert_batch_size) # noqa
        for batch in batches:
            batch_dicts = [metadata.dict() for metadata in batch]
            RecordClassificationMetadata.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(deduped_metadata_lst)} metadata into the database.") # noqa
    record_count = RecordClassificationMetadata.select().count()
    print(f"Metadata count after insertion: {record_count}")


def get_metadata() -> list[RecordClassificationMetadataModel]:
    query = RecordClassificationMetadata.select()
    res = list(query)
    res_dicts: list[dict] = [r.__dict__['__data__'] for r in res]
    transformed_res: list[RecordClassificationMetadataModel] = [
        RecordClassificationMetadataModel(
            uri=r["uri"],
            text=r["text"],
            synctimestamp=r["synctimestamp"],
            preprocessing_timestamp=r["preprocessing_timestamp"],
            source=r["source"],
            url=r["url"],
            like_count=r["like_count"],
            reply_count=r["reply_count"],
            repost_count=r["repost_count"]
        )
        for r in res_dicts
    ]
    return transformed_res


def get_existing_perspective_api_uris() -> set[str]:
    """Get URIs of existing Perspective API labels."""
    uris = set()
    query = PerspectiveApiLabels.select(PerspectiveApiLabels.uri)
    for row in query:
        uris.add(row.uri)
    return uris


def batch_insert_perspective_api_labels(
    labels: list[PerspectiveApiLabelsModel]
) -> None:
    """Batch insert Perspective API labels into the database."""
    deduped_labels = []
    existing_uris = get_existing_perspective_api_uris()
    seen_uris = set()
    seen_uris.update(existing_uris)
    print(f"Attempting to insert {len(labels)} labels into the database.")
    for label in labels:
        if label.uri not in seen_uris:
            deduped_labels.append(label)
            seen_uris.add(label.uri)
    print(f"Number of deduped labels to insert: {len(deduped_labels)}")
    if len(deduped_labels) == 0:
        print("No labels to insert.")
        return
    record_count = PerspectiveApiLabels.select().count()
    print(f"Labels count prior to insertion: {record_count}")
    with db.atomic():
        batches = create_batches(deduped_labels, label_insert_batch_size)
        for batch in batches:
            batch_dicts = [label.dict() for label in batch]
            PerspectiveApiLabels.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(deduped_labels)} labels into the database.") # noqa
    record_count = PerspectiveApiLabels.select().count()
    print(f"Labels count after insertion: {record_count}")


def get_perspective_api_labels() -> list[PerspectiveApiLabelsModel]:
    query = PerspectiveApiLabels.select()
    res = list(query)
    res_dicts: list[dict] = [r.__dict__['__data__'] for r in res]
    transformed_res: list[PerspectiveApiLabelsModel] = [
        PerspectiveApiLabelsModel(
            uri=r["uri"],
            text=r["text"],
            was_successfully_labeled=r["was_successfully_labeled"],
            reason=r["reason"],
            label_timestamp=r["label_timestamp"],
            prob_toxic=r["prob_toxic"],
            prob_severe_toxic=r["prob_severe_toxic"],
            prob_identity_attack=r["prob_identity_attack"],
            prob_insult=r["prob_insult"],
            prob_profanity=r["prob_profanity"],
            prob_threat=r["prob_threat"],
            prob_affinity=r["prob_affinity"],
            prob_compassion=r["prob_compassion"],
            prob_constructive=r["prob_constructive"],
            prob_curiosity=r["prob_curiosity"],
            prob_nuance=r["prob_nuance"],
            prob_personal_story=r["prob_personal_story"],
            prob_reasoning=r["prob_reasoning"],
            prob_respect=r["prob_respect"],
            prob_alienation=r["prob_alienation"],
            prob_fearmongering=r["prob_fearmongering"],
            prob_generalization=r["prob_generalization"],
            prob_moral_outrage=r["prob_moral_outrage"],
            prob_scapegoating=r["prob_scapegoating"],
            prob_sexually_explicit=r["prob_sexually_explicit"],
            prob_flirtation=r["prob_flirtation"],
            prob_spam=r["prob_spam"]
        )
        for r in res_dicts
    ]
    return transformed_res


def get_existing_sociopolitical_uris() -> set[str]:
    """Get URIs of existing sociopolitical labels."""
    uris = set()
    query = SociopoliticalLabels.select(SociopoliticalLabels.uri)
    for row in query:
        uris.add(row.uri)
    return uris


def batch_insert_sociopolitical_labels(
    labels: list[SociopoliticalLabelsModel]
) -> None:
    """Batch insert sociopolitical labels into the database."""
    deduped_labels = []
    existing_uris = get_existing_sociopolitical_uris()
    seen_uris = set()
    seen_uris.update(existing_uris)
    print(f"Attempting to insert {len(labels)} labels into the database.")
    for label in labels:
        if label.uri not in seen_uris:
            deduped_labels.append(label)
            seen_uris.add(label.uri)
    print(f"Number of deduped labels to insert: {len(deduped_labels)}")
    if len(deduped_labels) == 0:
        print("No labels to insert.")
        return
    record_count = SociopoliticalLabels.select().count()
    print(f"Labels count prior to insertion: {record_count}")
    with db.atomic():
        batches = create_batches(deduped_labels, label_insert_batch_size)
        for batch in batches:
            batch_dicts = [label.dict() for label in batch]
            SociopoliticalLabels.insert_many(batch_dicts).execute()
    print(f"Finished inserting {len(deduped_labels)} labels into the database.") # noqa
    record_count = SociopoliticalLabels.select().count()
    print(f"Labels count after insertion: {record_count}")


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
    # create_initial_tables()
    metadata_count = RecordClassificationMetadata.select().count()
    perspective_api_labels_count = PerspectiveApiLabels.select().count()
    # sociopolitical_labels_count = SociopoliticalLabels.select().count()
    print(f"Metadata count: {metadata_count}")
    print(f"Perspective API labels count: {perspective_api_labels_count}")
    # print(f"Sociopolitical labels count: {sociopolitical_labels_count}")

    # perspective_api_labels = get_perspective_api_labels()
    # print(perspective_api_labels[0])
    # print(perspective_api_labels[5])
    breakpoint()
