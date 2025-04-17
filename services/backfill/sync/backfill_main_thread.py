import os
import sqlite3
import threading

from lib.log.logger import get_logger
from services.backfill.sync.backfill import get_plc_directory_doc
from services.backfill.sync.backfill_endpoint_thread import (
    run_backfill_for_plc_endpoint,
)
from services.backfill.sync.determine_dids_to_backfill import (
    current_dir,
    sqlite_db_path,
)

did_plc_sqlite_db_path = os.path.join(current_dir, "did_plc.sqlite")

logger = get_logger(__name__)


def load_dids_from_local_db() -> list[str]:
    """Loads the DIDs from the local SQLite database."""
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT did FROM dids")
    dids = cursor.fetchall()
    conn.close()
    return [did[0] for did in dids]


def export_did_to_plc_endpoint_map_to_local_db(
    did_to_plc_endpoint_map: dict[str, str],
) -> None:
    """Exports the DID to PLC endpoint map to the local SQLite database."""
    conn = sqlite3.connect(did_plc_sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS did_plc (did TEXT PRIMARY KEY, plc_endpoint TEXT)"
    )
    for did, plc_endpoint in did_to_plc_endpoint_map.items():
        cursor.execute(
            "INSERT OR REPLACE INTO did_plc (did, plc_endpoint) VALUES (?, ?)",
            (did, plc_endpoint),
        )
    conn.commit()
    conn.close()
    logger.info(
        f"Exported DID to PLC endpoint map to local database at {did_plc_sqlite_db_path}"
    )


def load_did_to_plc_endpoint_map() -> dict[str, str]:
    """Loads the DID to PLC endpoint map from the database."""
    conn = sqlite3.connect(did_plc_sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT did, plc_endpoint FROM did_plc")
    did_to_plc_endpoint_map = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return did_to_plc_endpoint_map


def generate_plc_endpoint_to_dids_map(
    did_to_plc_endpoint_map: dict[str, str],
) -> dict[str, list[str]]:
    """Generates the PLC endpoint to DIDs map."""
    plc_endpoint_to_dids_map: dict[str, list[str]] = {}
    for did, plc_endpoint in did_to_plc_endpoint_map.items():
        if plc_endpoint not in plc_endpoint_to_dids_map:
            plc_endpoint_to_dids_map[plc_endpoint] = []
        plc_endpoint_to_dids_map[plc_endpoint].append(did)
    return plc_endpoint_to_dids_map


def export_plc_endpoint_to_dids_map_to_local_db(
    plc_endpoint_to_dids_map: dict[str, list[str]],
) -> None:
    """Exports the PLC endpoint to DIDs map to the local SQLite database."""
    conn = sqlite3.connect(did_plc_sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS plc_endpoint_to_dids (plc_endpoint TEXT PRIMARY KEY, dids TEXT)"
    )
    for plc_endpoint, dids in plc_endpoint_to_dids_map.items():
        dids_str = ",".join(dids)
        cursor.execute(
            "INSERT OR REPLACE INTO plc_endpoint_to_dids (plc_endpoint, dids) VALUES (?, ?)",
            (plc_endpoint, dids_str),
        )
    conn.commit()
    conn.close()
    logger.info(
        f"Exported PLC endpoint to DIDs map to local database at {did_plc_sqlite_db_path}"
    )


def load_plc_endpoint_to_dids_map() -> dict[str, list[str]]:
    """Loads the PLC endpoint to DIDs map from the database."""
    conn = sqlite3.connect(did_plc_sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT plc_endpoint, dids FROM plc_endpoint_to_dids")
    plc_endpoint_to_dids_map = {row[0]: row[1].split(",") for row in cursor.fetchall()}
    logger.info(
        f"Loaded PLC endpoint to DIDs map from local database at {did_plc_sqlite_db_path}"
    )
    conn.close()
    return plc_endpoint_to_dids_map


def backfill_did_to_plc_endpoint_map(
    dids: list[str],
    current_did_to_plc_endpoint_map: dict[str, str],
) -> dict[str, str]:
    """Backfills the DID to PLC endpoint map to include any missing DIDs."""
    missing_dids = [did for did in dids if did not in current_did_to_plc_endpoint_map]
    if len(missing_dids) == 0:
        logger.info("No missing DIDs, returning current map.")
        return current_did_to_plc_endpoint_map

    missing_dids_to_plc_endpoint_map = {}

    for did in missing_dids:
        plc_directory_doc = get_plc_directory_doc(did)
        service_endpoint = plc_directory_doc["service"][0]["serviceEndpoint"]
        if not service_endpoint:
            raise ValueError(f"No service endpoint found for DID {did}")
        missing_dids_to_plc_endpoint_map[did] = service_endpoint
    return {
        **current_did_to_plc_endpoint_map,
        **missing_dids_to_plc_endpoint_map,
    }


def run_backfills(
    dids: list[str],
    load_existing_endpoints_to_dids_map: bool = False,
) -> None:
    """Runs backfills for a list of DIDs."""
    if load_existing_endpoints_to_dids_map:
        plc_endpoint_to_dids_map: dict[str, list[str]] = load_plc_endpoint_to_dids_map()
    else:
        did_to_plc_endpoint_map: dict[str, str] = load_did_to_plc_endpoint_map()
        did_to_plc_endpoint_map: dict[str, str] = backfill_did_to_plc_endpoint_map(
            dids,
            did_to_plc_endpoint_map,
        )
        export_did_to_plc_endpoint_map_to_local_db(did_to_plc_endpoint_map)
        plc_endpoint_to_dids_map: dict[str, list[str]] = (
            generate_plc_endpoint_to_dids_map(
                did_to_plc_endpoint_map=did_to_plc_endpoint_map,
            )
        )
        export_plc_endpoint_to_dids_map_to_local_db(plc_endpoint_to_dids_map)
    for plc_endpoint in plc_endpoint_to_dids_map.keys():
        logger.info(
            f"Number of records for PLC endpoint {plc_endpoint}: {len(plc_endpoint_to_dids_map[plc_endpoint])}"
        )

    for plc_endpoint in plc_endpoint_to_dids_map.keys():
        logger.info(
            f"Running backfill for PLC endpoint {plc_endpoint} for {len(plc_endpoint_to_dids_map[plc_endpoint])} DIDs"
        )
        threading.Thread(
            target=run_backfill_for_plc_endpoint,
            kwargs={
                "plc_endpoint": plc_endpoint,
                "dids": plc_endpoint_to_dids_map[plc_endpoint],
            },
        ).start()

    logger.info("Backfills started")


def main():
    dids: list[str] = load_dids_from_local_db()
    run_backfills(
        dids=dids,
        load_existing_endpoints_to_dids_map=False,
    )


if __name__ == "__main__":
    main()
