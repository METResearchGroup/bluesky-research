"""Study-specific helpers for loading Parquet data from S3 with DuckDB.

This module intentionally sits between:
- `lib/aws/s3.py` (raw S3 listing primitives)
- `services/...` adapters (domain/business logic, SQL, model conversion)

Study dataset layout (hardcoded for now):
  s3://bluesky-research/bluesky_research/2024_nature_paper_study_data/{dataset}/{tier}/partition_date=YYYY-MM-DD/*.parquet
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

import pandas as pd

from lib.db.models import StorageTier
from lib.db.sql.duckdb_wrapper import DuckDB
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger

# This is OK to have here, as we care about 
if TYPE_CHECKING:
    from lib.aws.s3 import S3

logger = get_logger(__name__)

# Study-specific constants (hardcoded for now).
STUDY_BUCKET = "bluesky-research"
STUDY_ROOT_KEY_PREFIX = "bluesky_research/2024_nature_paper_study_data"


@dataclass(frozen=True)
class S3ParquetDatasetRef:
    """Reference to a study dataset folder under the study root prefix."""

    dataset: str


class S3ParquetBackend:
    """Study S3 Parquet backend primitives (listing + DuckDB execution)."""

    def __init__(
        self,
        s3: Optional["S3"] = None,
        duckdb_engine: Optional[DuckDB] = None,
        aws_region: str | None = None,
        storage_tier_default: StorageTier = StorageTier.CACHE,
    ) -> None:
        if s3 is None:
            # Lazy import to avoid requiring boto3 at import-time for callers/tests
            # that inject a mock S3 client.
            from lib.aws.s3 import S3 as S3Client

            self.s3 = S3Client()
        else:
            self.s3 = s3
        self.duckdb_engine = duckdb_engine or DuckDB()
        self.aws_region = aws_region
        self.storage_tier_default = storage_tier_default

    @staticmethod
    def _key_to_uri(key: str, bucket: str = STUDY_BUCKET) -> str:
        return f"s3://{bucket}/{key}"

    @staticmethod
    def _build_partition_prefix_key(
        dataset: str, tier: StorageTier, partition_date: str
    ) -> str:
        # Example:
        # bluesky_research/2024_nature_paper_study_data/preprocessed_posts/cache/partition_date=2024-11-13/
        return (
            f"{STUDY_ROOT_KEY_PREFIX}/{dataset}/{tier.value}/partition_date={partition_date}/"
        )

    def list_parquet_uris(
        self,
        dataset: S3ParquetDatasetRef,
        storage_tiers: list[StorageTier] | None = None,
        partition_date: str | None = None,
        start_partition_date: str | None = None,
        end_partition_date: str | None = None,
        max_days: int | None = None,
        max_files: int | None = None,
    ) -> list[str]:
        """Return DuckDB-readable URIs like `s3://bucket/key`.

        Listing strategy is intentionally *bounded*:
        - If `partition_date` is given, list only that day's prefix.
        - Else, if `start_partition_date` and `end_partition_date` are given, enumerate
          all days in the range and list each day prefix.

        Note: Listing without any partition-date filter is intentionally not supported
        to avoid accidental large scans.
        """

        if storage_tiers is None:
            storage_tiers = [self.storage_tier_default]

        if partition_date and (start_partition_date or end_partition_date):
            raise ValueError(
                "Provide either partition_date OR start_partition_date/end_partition_date, not both."
            )

        if partition_date:
            partition_dates = [partition_date]
        elif start_partition_date and end_partition_date:
            partition_dates = get_partition_dates(
                start_date=start_partition_date, end_date=end_partition_date
            )
        else:
            raise ValueError(
                "Must provide either partition_date or start_partition_date/end_partition_date."
            )

        if max_days is not None and len(partition_dates) > max_days:
            raise ValueError(
                f"Refusing to list n_days={len(partition_dates)} > max_days={max_days}."
            )

        logger.info(
            f"Listing S3 parquet URIs for dataset={dataset.dataset}, "
            f"storage_tiers={storage_tiers}, n_days={len(partition_dates)}."
        )

        uris: list[str] = []
        total_files = 0

        for tier in storage_tiers:
            for day in partition_dates:
                prefix_key = self._build_partition_prefix_key(
                    dataset=dataset.dataset, tier=tier, partition_date=day
                )
                keys = self.s3.list_keys_given_prefix(prefix=prefix_key)
                parquet_keys = [k for k in keys if k.endswith(".parquet")]
                if parquet_keys:
                    logger.info(
                        f"[dataset={dataset.dataset} tier={tier.value} partition_date={day}] "
                        f"Found n_files={len(parquet_keys)} parquet objects."
                    )

                for k in parquet_keys:
                    uris.append(self._key_to_uri(k))

                total_files += len(parquet_keys)

                if max_files is not None and total_files >= max_files:
                    logger.warning(
                        f"Reached max_files={max_files}; truncating listed URIs."
                    )
                    return uris[:max_files]

        logger.info(
            f"Listed total_parquet_files={len(uris)} for dataset={dataset.dataset}."
        )
        return uris

    def query_parquet_as_df(
        self,
        uris: list[str],
        query: str,
        query_metadata: dict | None = None,
    ) -> pd.DataFrame:
        """Run DuckDB over the parquet URIs and return a pandas DataFrame."""
        return self.duckdb_engine.run_query_as_df(
            query=query,
            mode="parquet",
            filepaths=uris,
            query_metadata=query_metadata,
            aws_region=self.aws_region,
        )

    def query_dataset_as_df(
        self,
        dataset: S3ParquetDatasetRef,
        query: str,
        query_metadata: dict | None = None,
        storage_tiers: list[StorageTier] | None = None,
        partition_date: str | None = None,
        start_partition_date: str | None = None,
        end_partition_date: str | None = None,
        max_days: int | None = None,
        max_files: int | None = None,
    ) -> pd.DataFrame:
        """List parquet URIs for the dataset and execute a DuckDB query over them."""
        uris = self.list_parquet_uris(
            dataset=dataset,
            storage_tiers=storage_tiers,
            partition_date=partition_date,
            start_partition_date=start_partition_date,
            end_partition_date=end_partition_date,
            max_days=max_days,
            max_files=max_files,
        )
        return self.query_parquet_as_df(
            uris=uris,
            query=query,
            query_metadata=query_metadata,
        )
