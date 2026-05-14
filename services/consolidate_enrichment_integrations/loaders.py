"""Load input datasets for enrichment consolidation."""

from collections.abc import Callable
from typing import TypeVar

import pandas as pd
from pydantic import BaseModel

from lib.aws.athena import Athena
from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import load_data_from_local_storage
from services.generate_vector_embeddings.models import PostSimilarityScoreModel
from services.ml_inference.models import (
    PerspectiveApiLabelsModel,
    SociopoliticalLabelsModel,
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

T = TypeVar("T", bound=BaseModel)

athena = Athena()

POST_COSINE_SIMILARITY_TABLE = "post_cosine_similarity_scores"


def dataframe_to_models(
    df: pd.DataFrame,
    model_cls: type[T],
    row_filter: Callable[[dict], bool] | None = None,
) -> list[T]:
    """Convert a DataFrame to validated Pydantic models.

    Applies parse_converted_pandas_dicts for pandas/NaN normalization, then
    optionally filters rows before model construction.
    """
    df_dicts = df.to_dict(orient="records")
    df_dicts = parse_converted_pandas_dicts(df_dicts)
    if row_filter is not None:
        df_dicts = [row for row in df_dicts if row_filter(row)]
    return [model_cls(**row) for row in df_dicts]


def load_latest_preprocessed_posts(
    timestamp: str | None,
) -> list[FilteredPreprocessedPostModel]:
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        latest_timestamp=timestamp,
    )  # type: ignore
    return dataframe_to_models(
        df,
        FilteredPreprocessedPostModel,
        row_filter=lambda row: row.get("text") is not None,
    )


def load_previously_consolidated_enriched_post_uris() -> set[str]:
    """Return all URIs present in locally stored consolidated enriched post records."""
    df = load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        latest_timestamp=None,
    )  # type: ignore
    return set(df["uri"].tolist())


def load_latest_perspective_api_labels(
    timestamp: str | None,
) -> list[PerspectiveApiLabelsModel]:
    df = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        latest_timestamp=timestamp,
    )  # type: ignore
    return dataframe_to_models(df, PerspectiveApiLabelsModel)


def load_latest_sociopolitical_labels(
    timestamp: str | None,
) -> list[SociopoliticalLabelsModel]:
    df = load_data_from_local_storage(
        service="ml_inference_sociopolitical",
        latest_timestamp=timestamp,
    )  # type: ignore
    return dataframe_to_models(df, SociopoliticalLabelsModel)


def load_latest_similarity_scores(
    timestamp: str | None,
) -> list[PostSimilarityScoreModel]:
    """Load cosine similarity scores from Athena.

    When ``timestamp`` is falsy, all rows are returned (``WHERE 1=1``).
    Otherwise, rows with ``insert_timestamp`` strictly after ``timestamp`` are returned.
    """
    where_filter = f"insert_timestamp > '{timestamp}'" if timestamp else "1=1"  # noqa: S608
    query = f"""
    SELECT * FROM {POST_COSINE_SIMILARITY_TABLE}
    WHERE {where_filter}
    """
    df: pd.DataFrame = athena.query_results_as_df(query)
    return dataframe_to_models(df, PostSimilarityScoreModel)
