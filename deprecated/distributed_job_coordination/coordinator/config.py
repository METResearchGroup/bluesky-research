"""
Job configuration handling for the distributed job coordination framework.

This module provides utilities for loading, validating, and processing job
configuration files that define distributed jobs.
"""

import os
import yaml
import json
import jsonschema
from typing import Dict, Any, List


# JSON Schema for validating job configurations
JOB_CONFIG_SCHEMA = {
    "type": "object",
    "required": ["job_type", "handler", "batch_size"],
    "properties": {
        "job_type": {
            "type": "string",
            "description": "Type of job (e.g., 'backfill', 'aggregation')",
        },
        "handler": {
            "type": "string",
            "description": "Path to handler function for processing items",
        },
        "batch_size": {
            "type": "integer",
            "minimum": 1,
            "description": "Number of items to process in each batch",
        },
        "slurm": {
            "type": "object",
            "properties": {
                "memory": {
                    "type": "string",
                    "description": "Memory allocation for Slurm jobs (e.g., '8G')",
                },
                "cpus": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "CPU cores per Slurm job",
                },
                "time": {
                    "type": "string",
                    "description": "Time limit for Slurm jobs (e.g., '01:00:00')",
                },
                "partition": {
                    "type": "string",
                    "description": "Slurm partition to use",
                },
            },
        },
        "phases": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of job phases to execute sequentially",
        },
        "retry": {
            "type": "object",
            "properties": {
                "max_attempts": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Maximum number of retry attempts",
                },
                "backoff_factor": {
                    "type": "number",
                    "minimum": 1.0,
                    "description": "Factor for exponential backoff between retries",
                },
                "retry_delay": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Initial delay (seconds) before first retry",
                },
            },
        },
        "rate_limits": {
            "type": "object",
            "properties": {
                "tokens_per_second": {
                    "type": "number",
                    "minimum": 0.1,
                    "description": "Rate limit in tokens per second",
                },
                "max_tokens": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum tokens that can be accumulated",
                },
            },
        },
        "extends": {
            "type": "string",
            "description": "Path to base configuration file to extend",
        },
    },
}


class ConfigValidationError(Exception):
    """Exception raised for job configuration validation errors."""

    pass


class JobConfig:
    """
    Configuration for a distributed job.

    Handles loading, validating, and accessing job configuration parameters.
    Supports inheritance from base configurations.
    """

    def __init__(self, config_path: str):
        """
        Initialize configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Raises:
            FileNotFoundError: If the configuration file does not exist
            ConfigValidationError: If the configuration is invalid
        """
        self.config_path = config_path
        self.config_data = {}
        self.base_configs = []

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        self._load_config()

    def _load_config(self) -> None:
        """
        Load and process the configuration file.

        Handles inheritance by recursively loading base configurations.

        Raises:
            ConfigValidationError: If there are issues parsing or validating the config
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)

            if not config:
                raise ConfigValidationError(
                    f"Empty configuration file: {self.config_path}"
                )

            # Handle configuration inheritance
            if "extends" in config:
                base_path = config.pop("extends")
                if not os.path.isabs(base_path):
                    # Resolve relative path based on current config file
                    base_dir = os.path.dirname(os.path.abspath(self.config_path))
                    base_path = os.path.join(base_dir, base_path)

                base_config = JobConfig(base_path)
                self.base_configs.append(base_config)

                # Merge with base configuration (current config takes precedence)
                merged_config = base_config.get_all()
                merged_config.update(config)
                config = merged_config

            self.config_data = config

            # Validate the configuration
            self.validate()

        except yaml.YAMLError as e:
            raise ConfigValidationError(f"Error parsing YAML configuration: {e}")
        except jsonschema.exceptions.ValidationError as e:
            raise ConfigValidationError(f"Configuration validation error: {e.message}")

    def validate(self) -> bool:
        """
        Validate the configuration against the schema.

        Returns:
            True if the configuration is valid

        Raises:
            ConfigValidationError: If the configuration is invalid
        """
        try:
            jsonschema.validate(self.config_data, JOB_CONFIG_SCHEMA)
            return True
        except jsonschema.exceptions.ValidationError as e:
            raise ConfigValidationError(f"Configuration validation error: {e.message}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.

        Args:
            key: Configuration key to retrieve
            default: Default value if key does not exist

        Returns:
            The configuration value or default
        """
        return self.config_data.get(key, default)

    def get_all(self) -> Dict[str, Any]:
        """
        Get the entire configuration as a dictionary.

        Returns:
            Dict containing all configuration values
        """
        return self.config_data.copy()

    def get_nested(self, *keys: str, default: Any = None) -> Any:
        """
        Get a nested configuration value.

        Args:
            *keys: Sequence of keys to navigate the nested structure
            default: Default value if path does not exist

        Returns:
            The nested configuration value or default
        """
        data = self.config_data
        for key in keys:
            if not isinstance(data, dict) or key not in data:
                return default
            data = data[key]
        return data

    @property
    def job_type(self) -> str:
        """Get the job type."""
        return self.get("job_type")

    @property
    def handler_path(self) -> str:
        """Get the handler function path."""
        return self.get("handler")

    @property
    def batch_size(self) -> int:
        """Get the batch size for this job."""
        return self.get("batch_size")

    @property
    def slurm_config(self) -> Dict[str, Any]:
        """Get the Slurm configuration."""
        return self.get("slurm", {})

    @property
    def phases(self) -> List[str]:
        """Get the job phases."""
        return self.get("phases", ["initial"])

    @property
    def retry_config(self) -> Dict[str, Any]:
        """Get the retry configuration."""
        default_retry = {"max_attempts": 3, "backoff_factor": 2.0, "retry_delay": 60}
        return {**default_retry, **self.get("retry", {})}

    @property
    def rate_limits(self) -> Dict[str, Any]:
        """Get the rate limiting configuration."""
        default_limits = {"tokens_per_second": 1.0, "max_tokens": 100}
        return {**default_limits, **self.get("rate_limits", {})}

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to a dictionary.

        Returns:
            Dict representation of the configuration
        """
        return {"config_file": self.config_path, **self.get_all()}

    def to_json(self) -> str:
        """
        Convert configuration to JSON string.

        Returns:
            JSON string representation of the configuration
        """
        return json.dumps(self.to_dict())


def load_job_config(config_path: str) -> JobConfig:
    """
    Load and validate a job configuration file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Validated JobConfig object

    Raises:
        FileNotFoundError: If the configuration file does not exist
        ConfigValidationError: If the configuration is invalid
    """
    return JobConfig(config_path)
