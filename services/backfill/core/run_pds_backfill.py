import os

from api.backfill_router.config.schema import BackfillConfigSchema
from lib.constants import project_home_directory
from lib.log.logger import get_logger
from services.backfill.core.manager import PdsEndpointManager
from services.backfill.storage.utils.main import (
    load_dids_to_query,
    load_existing_plc_results,
)

logger = get_logger(__name__)


def run_pds_backfill(config: BackfillConfigSchema) -> None:
    """For a given set of DIDs and PLC docs, run the PDS backfills."""
    logger.info("Running PDS backfill...")
    full_plc_storage_fp = os.path.join(
        project_home_directory,
        config.plc_storage.path,
    )
    full_did_storage_fp = os.path.join(
        project_home_directory,
        config.source.path,
    )
    dids: list[str] = load_dids_to_query(
        type=config.source.type, path=full_did_storage_fp
    )
    dids_set = set(dids)
    existing_plc_docs: list[dict] = load_existing_plc_results(
        type=config.plc_storage.type, path=full_plc_storage_fp
    )
    valid_plc_docs: list[dict] = [
        plc_result for plc_result in existing_plc_docs if plc_result["did"] in dids_set
    ]
    if len(valid_plc_docs) == 0:
        logger.info("No valid PLC results found. Skipping PDS backfill.")
        return
    if len(valid_plc_docs) != len(dids_set):
        num_unmatched_dids = len(dids_set) - len(valid_plc_docs)
        logger.info(
            f"Some DIDs do not have a valid PLC result. Skipping PDS backfill for {num_unmatched_dids} DIDs."
        )

    pds_endpoint_manager = PdsEndpointManager(config=config, plc_docs=valid_plc_docs)
    pds_endpoint_manager.run_backfills()
    logger.info("Completed PDS backfill.")
