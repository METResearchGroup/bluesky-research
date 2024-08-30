"""Helper functions for compact dedupe data."""

import os

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.s3 import S3, ROOT_BUCKET
from lib.log.logger import get_logger
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

athena = Athena()
s3 = S3()

logger = get_logger(__name__)


def load_preprocessed_posts() -> list[FilteredPreprocessedPostModel]:
    """Load the preprocessed posts from the database."""
    query = """
    SELECT * FROM `preprocessed_firehose_posts`
    UNION ALL
    SELECT * FROM `preprocessed_most_liked_posts`
    """
    df = athena.query_results_as_df(query=query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    breakpoint()
    df_dict_models = [FilteredPreprocessedPostModel(**df_dict) for df_dict in df_dicts]
    return df_dict_models


def export_preprocessed_posts(posts: list[FilteredPreprocessedPostModel]):
    # TODO: add timestamp partition to the key, since this is what our
    # Glue table expects.
    # TODO: run crawler.
    pass


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
    posts = load_preprocessed_posts()
    df = pd.DataFrame(posts)

    # Sort by preprocessing_timestamp and dedupe by uri
    df = df.sort_values(by="preprocessing_timestamp", ascending=False).drop_duplicates(
        subset=["uri"]
    )

    df_dicts = df.to_dict(orient="records")
    df_dict_models = [FilteredPreprocessedPostModel(**df_dict) for df_dict in df_dicts]
    breakpoint()

    # load existing keys from S3 (load before exporting new file so that
    # we have all the key names for the non-compacted files).
    existing_keys: list[str] = get_existing_keys()
    # export new file to S3.
    export_preprocessed_posts(df_dict_models)
    # drop existing keys from S3.
    delete_keys(existing_keys)
    logger.info("Successfully compacted dedupe preprocessed data")


if __name__ == "__main__":
    pass
