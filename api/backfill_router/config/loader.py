import yaml
from api.backfill_router.config.schema import BackfillConfigSchema  # Pydantic model


def load_yaml_config(path: str) -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    # Validate with Pydantic
    validated = BackfillConfigSchema(**config)
    return validated
