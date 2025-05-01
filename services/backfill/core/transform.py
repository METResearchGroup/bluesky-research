"""Transformations for backfilled records."""

from datetime import datetime
import json
import traceback

from lib.constants import convert_bsky_dt_to_pipeline_dt, timestamp_format
from lib.db.bluesky_models.transformations import (
    TransformedBlock,
    TransformedFollow,
    TransformedLike,
    TransformedPost,
    TransformedRepost,
    TransformedReply,
)
from lib.log.logger import get_logger
from services.backfill.core.validate import validate_record_timestamp

logger = get_logger(__name__)


def assign_default_backfill_synctimestamp(synctimestamp: str) -> str:
    """Assign a default synctimestamp to a record.

    Args:
        synctimestamp: The synctimestamp to assign

    Returns:
        The assigned synctimestamp

    We'll assign the record the synctimestamp corresponding to the
    1st or 15th of a given month, whichever is the earliest one that
    comes after a synctimestamp.

    For example, if a record is created on April 2nd, we assign it to April 15th,
    and if it's created April 16th, we assign it to May 1st.

    We also change the time to be 00:00:00.
    """
    synctimestamp_dt = datetime.strptime(synctimestamp, timestamp_format)
    new_hour = 0
    new_minute = 0
    new_second = 0
    try:
        if synctimestamp_dt.day <= 15:
            new_ts = synctimestamp_dt.replace(
                day=15,
                hour=new_hour,
                minute=new_minute,
                second=new_second,
            ).strftime(timestamp_format)
        else:
            current_month = synctimestamp_dt.month
            new_month = current_month + 1
            if new_month > 12:
                new_month = 1
                new_year = synctimestamp_dt.year + 1
            else:
                new_year = synctimestamp_dt.year
            new_ts = synctimestamp_dt.replace(
                year=new_year,
                month=new_month,
                day=1,
                hour=new_hour,
                minute=new_minute,
                second=new_second,
            ).strftime(timestamp_format)
        return new_ts
    except Exception as e:
        logger.error(f"Error assigning default synctimestamp: {e}")
        return synctimestamp


def transform_backfilled_record(
    did: str,
    record: dict,
    record_type: str,
    start_timestamp: str,
    end_timestamp: str,
) -> dict:
    """Transform a backfilled record.

    Args:
        record: The record to transform
        record_type: The type of the record

    Returns:
        The transformed record
    """
    record["author"] = did
    record["record_type"] = record_type

    record_value = record.pop("value")

    record["synctimestamp"] = convert_bsky_dt_to_pipeline_dt(record_value["createdAt"])  # noqa
    # for old records, use a different synctimestamp that'll allow
    # use to better partition the data.

    # validate the synctimestamp and if it's not in the range,
    # then set to a default timestamp.
    record_falls_in_study_range: bool = validate_record_timestamp(
        record_timestamp=record_value["createdAt"],
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    if not record_falls_in_study_range:
        record["synctimestamp"] = assign_default_backfill_synctimestamp(
            synctimestamp=record["synctimestamp"]
        )

    # transform the formats of the fields to be consistent with each other
    # for a given type (for optional fields, these show up only if the
    # record has the field, but I want to enforce consistent schemas).
    try:
        if record_type in ["post", "reply"]:
            # records from querying the PDS don't have their URIs for some reason,
            # and I can't get around that it seems?

            embed = record_value.get("embed", False)
            record["embed"] = json.dumps(embed) if embed else None

            entities = record_value.get("entities", False)
            record["entities"] = json.dumps(entities) if entities else None

            facets = record_value.get("facets", False)
            record["facets"] = json.dumps(facets) if facets else None

            langs = record_value.get("langs", False)
            record["langs"] = ",".join(langs) if langs else None

            tags = record_value.get("tags", False)
            record["tags"] = ",".join(tags) if tags else None

            labels = record_value.get("labels", False)
            record["labels"] = json.dumps(labels) if labels else None

            if record_type == "post":
                transformed_record = TransformedPost(**record)
            elif record_type == "reply":
                transformed_record = TransformedReply(**record)
                transformed_record = transformed_record.model_dump()
                transformed_record["reply"] = json.dumps(transformed_record["reply"])
        elif record_type == "repost":
            record["created_at"] = record_value["createdAt"]
            record["subject"] = record_value["subject"]
            transformed_record = TransformedRepost(**record)
            transformed_record = transformed_record.model_dump()
            transformed_record["subject"] = json.dumps(transformed_record["subject"])
        elif record_type == "like":
            record["created_at"] = record_value["createdAt"]
            record["subject"] = record_value["subject"]
            transformed_record = TransformedLike(**record)
            transformed_record = transformed_record.model_dump()
            transformed_record["subject"] = json.dumps(transformed_record["subject"])
        elif record_type == "follow":
            record["created_at"] = record_value["createdAt"]
            record["subject"] = record_value["subject"]
            transformed_record = TransformedFollow(**record)
        elif record_type == "block":
            record["created_at"] = record_value["createdAt"]
            record["subject"] = record_value["subject"]
            transformed_record = TransformedBlock(**record)
        if not isinstance(transformed_record, dict):
            return transformed_record.model_dump()
        return transformed_record
    except Exception as e:
        logger.error(f"Error transforming record: {e}")
        logger.error(traceback.format_exc())
        logger.info(f"Record: {record}")
        return record


def stub_unnecessary_fields(record: dict) -> dict:
    """Add stubs for fields whose data we don't need, if those fields exist.

    For example, we currently don't need the 'Embed' field in posts. We may
    in the future, but for now, we'll just add a stub value."""
    stub_value = "<removed>"

    if "embed" in record and record["embed"] is not None:
        record["embed"] = stub_value
    if "entities" in record and record["entities"] is not None:
        record["entities"] = stub_value
    if "facets" in record and record["facets"] is not None:
        record["facets"] = stub_value
    return record


def postprocess_backfilled_record(record: dict) -> dict:
    """Postprocess a backfilled record.

    Args:
        record: The record to postprocess

    Returns:
        The postprocessed record

    We separate this out of 'transform_backfilled_record' because there are
    steps in the postprocessing process that we might want to reconsider or
    remove in the future.
    """
    record = stub_unnecessary_fields(record)
    return record
