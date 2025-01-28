"""Logging utilities."""

import wandb

from api.integrations_router.models import RunExecutionMetadata
from lib.helper import RUN_MODE


def log_run_to_wandb(service_name: str):
    """Decorator that logs run metadata to W&B under the specified project name.

    Args:
        service_name (str): Name of the W&B project to log to
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            if RUN_MODE == "test":
                return func(*args, **kwargs)
            wandb.init(project=service_name)
            run_metadata: RunExecutionMetadata = func(*args, **kwargs)
            wandb.log(run_metadata.dict())
            return run_metadata

        return wrapper

    return decorator
