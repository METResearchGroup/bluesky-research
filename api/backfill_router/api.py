import os
import yaml

from api.backfill_router.config.schema import BackfillConfigSchema
from lib.constants import project_home_directory
from lib.log.logger import get_logger
from services.backfill.core.query_plc_endpoint import query_plc_endpoint
# from services.backfill.core.pds_endpoint_backfill import run_pds_backfill  # Uncomment and implement when ready

logger = get_logger(__name__)


def load_config(config_path: str) -> BackfillConfigSchema:
    if project_home_directory not in config_path:
        config_path = os.path.join(project_home_directory, config_path)
    with open(config_path, "r") as f:
        logger.info(f"Loading config from {config_path}")
        config_dict = yaml.safe_load(f)
    return BackfillConfigSchema(**config_dict["backfill"])


def run_query_plc(config_path: str) -> None:
    config = load_config(config_path)
    query_plc_endpoint(config=config)


def run_pds_backfill_api(config_path: str) -> None:
    config = load_config(config_path)
    # run_pds_backfill(config=config)  # Uncomment and implement when ready
    print(
        "[STUB] PDS endpoint backfill not yet implemented. Would run with config:",
        config,
    )
