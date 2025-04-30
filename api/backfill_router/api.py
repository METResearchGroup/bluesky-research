import yaml
from api.backfill_router.config.schema import BackfillConfigSchema
from services.backfill.core.query_plc_endpoint import query_plc_endpoint
# from services.backfill.core.pds_endpoint_backfill import run_pds_backfill  # Uncomment and implement when ready


def load_config(config_path: str) -> BackfillConfigSchema:
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
    return BackfillConfigSchema(**config_dict["backfill"])


def run_query_plc(config_path: str) -> None:
    config = load_config(config_path)
    query_plc_endpoint(dids=None, config=config)  # dids=None: load from config


def run_pds_backfill_api(config_path: str) -> None:
    config = load_config(config_path)
    # run_pds_backfill(config=config)  # Uncomment and implement when ready
    print(
        "[STUB] PDS endpoint backfill not yet implemented. Would run with config:",
        config,
    )
