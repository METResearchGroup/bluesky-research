"""Logging utilities."""

import wandb

from lib.load_env_vars import EnvVarsContainer
from lib.metadata.models import RunExecutionMetadata


def log_run_to_wandb(service_name: str):
    """Decorator that logs run metadata to W&B under the specified project name.

    Args:
        service_name (str): Name of the W&B project to log to
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            if EnvVarsContainer.get_env_var("RUN_MODE") == "test":
                return func(*args, **kwargs)
            wandb.init(project=service_name)
            run_metadata: RunExecutionMetadata = func(*args, **kwargs)
            wandb.log(run_metadata.model_dump())
            return run_metadata

        return wrapper

    return decorator


# NOTE: check if I want to add additional metrics or anything per-epoch
# or if I want to visualize anything. If not, I can just use the above
# decorator.
def log_batch_classification_to_wandb(service="ml_inference_ime"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if EnvVarsContainer.get_env_var("RUN_MODE") == "test":
                return func(*args, **kwargs)
            wandb.init(project=service)
            run_metadata: dict = func(*args, **kwargs)
            hyperparameters = run_metadata.pop("hyperparameters")
            metrics = run_metadata.pop("metrics")
            wandb.log(run_metadata)
            wandb.log(hyperparameters)
            wandb.log(metrics)
            return run_metadata

        return wrapper

    return decorator
