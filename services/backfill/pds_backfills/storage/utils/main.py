"""Single interface for managing I/O from various storage systems."""

from services.backfill.storage.utils.parquet_utils import (
    load_uris_to_filter as parquet_load_uris_to_filter,
)
from services.backfill.storage.utils.queue_utils import (
    load_dids_to_query as sqlite_get_dids_to_query,
    load_existing_plc_results as sqlite_get_existing_plc_results,
)


def load_dids_to_query(type: str, path: str) -> list[str]:
    if type == "sqlite":
        return sqlite_get_dids_to_query(db_path=path)
    else:
        print(f"Unsupported type: {type}")
        return []


def load_existing_plc_results(type: str, path: str) -> dict[str, str]:
    if type == "sqlite":
        return sqlite_get_existing_plc_results(plc_storage_db_path=path)
    else:
        print(f"Unsupported type: {type}")
        return {}


def load_uris_to_filter(type: str, path: str) -> set[str]:
    if type == "parquet":
        return parquet_load_uris_to_filter(path=path)
    else:
        print(f"Unsupported type: {type}")
        return {}
