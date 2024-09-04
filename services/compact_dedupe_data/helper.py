"""Helper functions for compact dedupe data."""

import os

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.glue import Glue
from lib.aws.s3 import S3, ROOT_BUCKET
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

athena = Athena()
glue = Glue()
s3 = S3()

logger = get_logger(__name__)

pd.set_option("display.max_columns", None)
pd.set_option("max_colwidth", None)


# TODO: at some point, I should split up the compressed files tbh
# into files of size X (not sure if I will get to that scale).
def export_preprocessed_posts(posts: list[FilteredPreprocessedPostModel]):
    """Exports the deduped and compacted preprocessed posts to S3."""
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=generate_current_datetime_str()
    )
    filename = "preprocessed_posts.jsonl"
    full_key = os.path.join(
        "preprocessed_data",
        "post",
        "preprocessed_compressed_deduped_posts",
        partition_key,
        filename,
    )  # noqa
    post_dicts = [post.dict() for post in posts]
    s3.write_dicts_jsonl_to_s3(data=post_dicts, key=full_key)
    glue.start_crawler(crawler_name="preprocessed_posts_crawler")
    logger.info(f"Exported deduped and compacted preprocessed posts to {full_key}")  # noqa


def get_existing_keys() -> list[str]:
    res: list[str] = []
    root_keys = [
        os.path.join("preprocessed_data", "post", "firehose"),
        os.path.join("preprocessed_data", "post", "most_liked"),
    ]
    for root_key in root_keys:
        res.extend(s3.list_keys_given_prefix(root_key))
    return res


def delete_keys(keys: list[str]):
    for key in keys:
        s3.client.delete_object(Bucket=ROOT_BUCKET, Key=key)
    logger.info(f"Deleted {len(keys)} keys")


def compact_dedupe_preprocessed_data():
    posts: list[FilteredPreprocessedPostModel] = athena.get_latest_preprocessed_posts(
        timestamp=None
    )
    post_dicts = [post.dict() for post in posts]
    df = pd.DataFrame(post_dicts)
    logger.info(f"Number of posts to compact: {len(df)}")
    # Sort by preprocessing_timestamp and dedupe by uri
    df = df.sort_values(by="preprocessing_timestamp", ascending=False).drop_duplicates(
        subset=["uri"]
    )
    df_dicts = df.to_dict(orient="records")
    logger.info(f"Number of posts after dedupe: {len(df_dicts)}")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    df_dict_models = [FilteredPreprocessedPostModel(**df_dict) for df_dict in df_dicts]
    # load existing keys from S3 (load before exporting new file so that
    # we have all the key names for the non-compacted files).
    existing_keys: list[str] = get_existing_keys()
    logger.info(f"Number of existing keys: {len(existing_keys)}")
    # export new file to S3.
    export_preprocessed_posts(df_dict_models)
    # drop existing keys from S3.
    # delete_keys(existing_keys)
    logger.info("Successfully compacted dedupe preprocessed data")


if __name__ == "__main__":
    compact_dedupe_preprocessed_data()
