"""Single interface for managing I/O from various storage systems."""

from services.backfill.storage.utils.queue_utils import (
    load_existing_did_to_pds_endpoint_map as sqlite_get_existing_did_to_pds_endpoint_map,
)


def load_dids_to_query(type: str, path: str) -> list[str]:
    if type == "sqlite":
        pass
    else:
        print(f"Unsupported type: {type}")
        return []


def load_existing_did_to_pds_endpoint_map(type: str, path: str) -> dict[str, str]:
    if type == "sqlite":
        return sqlite_get_existing_did_to_pds_endpoint_map(path=path)
    else:
        print(f"Unsupported type: {type}")
        return {}
