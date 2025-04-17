import threading

from lib.log.logger import get_logger
from services.backfill.sync.backfill import get_plc_directory_doc
from services.backfill.sync.backfill_endpoint_thread import (
    run_backfill_for_plc_endpoint,
)

logger = get_logger(__name__)


def load_did_to_plc_endpoint_map() -> dict[str, str]:
    """Loads the DID to PLC endpoint map from the database."""
    pass


def generate_plc_endpoint_to_dids_map() -> dict[str, list[str]]:
    """Generates the PLC endpoint to DIDs map."""
    pass


def load_plc_endpoint_to_dids_map() -> dict[str, list[str]]:
    """Loads the PLC endpoint to DIDs map from the database."""
    pass


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
        plc_endpoint_to_dids_map: dict[str, list[str]] = (
            generate_plc_endpoint_to_dids_map(
                did_to_plc_endpoint_map=did_to_plc_endpoint_map,
            )
        )
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
