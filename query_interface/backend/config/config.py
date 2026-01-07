"""Configuration loader for query interface backend."""

import threading
from pathlib import Path
from typing import Any

import yaml


class Config:
    """Configuration class that lazy-loads config from YAML file."""

    def __init__(self, config_path: str | None = None):
        """Initialize config with optional config path.

        Args:
            config_path: Path to config.yaml file. If None, will look for
                config.yaml in the same directory as this module.
        """
        self._config: dict[str, Any] | None = None
        self._lock = threading.Lock()
        if config_path is None:
            # Default to config.yaml in the same directory as this module
            config_path = str(Path(__file__).parent / "config.yaml")
        self._config_path = Path(config_path)

    def _load_config(self) -> dict[str, Any]:
        """Load configuration from YAML file (thread-safe).

        Returns:
            Configuration dictionary.
        """
        if self._config is None:
            with self._lock:
                if self._config is None:
                    if not self._config_path.exists():
                        raise FileNotFoundError(
                            f"Configuration file not found: {self._config_path}"
                        )
                    with open(self._config_path, "r") as f:
                        self._config = yaml.safe_load(f)
        return self._config or {} # type: ignore

    def get_config_value(self, *keys: str) -> Any:
        """Get configuration value using dot-separated keys.

        Args:
            *keys: Variable number of keys to traverse the config dict.
                For example: get_config_value("llm", "default_model")
                or get_config_value("sql", "default_limit")

        Returns:
            The configuration value at the specified path.

        Raises:
            KeyError: If any key in the path does not exist.
            ValueError: If config cannot be loaded.
        """
        config = self._load_config()
        value = config
        for key in keys:
            if not isinstance(value, dict):
                raise KeyError(
                    f"Cannot traverse key '{key}' - parent is not a dictionary. "
                    f"Path so far: {' -> '.join(keys[:keys.index(key)])}"
                )
            if key not in value:
                raise KeyError(
                    f"Configuration key not found: {' -> '.join(keys[:keys.index(key) + 1])}"
                )
            value = value[key]
        return value


# Global config instance (lazy-loaded)
_config_instance: Config | None = None
_config_lock = threading.Lock()


def get_config() -> Config:
    """Get the global config instance (thread-safe singleton).

    Returns:
        The global Config instance.
    """
    global _config_instance
    if _config_instance is None:
        with _config_lock:
            if _config_instance is None:
                _config_instance = Config()
    return _config_instance


def get_config_value(*keys: str) -> Any:
    """Convenience function to get config value from global config.

    Args:
        *keys: Variable number of keys to traverse the config dict.

    Returns:
        The configuration value at the specified path.
    """
    return get_config().get_config_value(*keys)
