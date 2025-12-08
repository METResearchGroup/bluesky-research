import os
import yaml

from services.backfill.config.schema import BackfillConfigSchema
from lib.constants import project_home_directory
from lib.log.logger import get_logger

logger = get_logger(__name__)


def load_config(config_path: str) -> BackfillConfigSchema:
    """Load and validate backfill configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file. If relative, will be
            resolved relative to project_home_directory.

    Returns:
        BackfillConfigSchema: Validated configuration object.
    """
    if project_home_directory not in config_path:
        config_path = os.path.join(project_home_directory, config_path)
    with open(config_path, "r") as f:
        logger.info(f"Loading config from {config_path}")
        config_dict = yaml.safe_load(f)
    return BackfillConfigSchema(**config_dict["backfill"])


def load_yaml_config(path: str) -> BackfillConfigSchema:
    """Load YAML configuration file.

    Deprecated: Use load_config() instead. This function is kept for backward
    compatibility but may be removed in a future version.

    Args:
        path: Path to YAML configuration file.

    Returns:
        BackfillConfigSchema: Validated configuration object.
    """
    if project_home_directory not in path:
        path = os.path.join(project_home_directory, path)
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    # Validate with Pydantic
    validated = BackfillConfigSchema(**config)
    return validated
