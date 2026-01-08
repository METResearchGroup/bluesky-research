"""Centralized environment variable loading.

This module exists to avoid import-time side effects spread across the codebase.
It encapsulates:
- Loading `../.env` (repo root) via python-dotenv
- RUN_MODE-specific overrides (notably in tests)
- Optional prod-only secret fetching for Bluesky credentials

Public API: `EnvVarsContainer.get_env_var(name)`
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from lib.aws.secretsmanager import get_secret


class EnvVarsContainer:
    """Thread-safe singleton container for environment variables."""

    _instance: Optional["EnvVarsContainer"] = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self._initialized = False
        # Store raw values; apply defaults/casting at retrieval time.
        self._env_vars: dict[str, Any] = {}
        # Known env-var expected types. If a key is not present here, `get_env_var`
        # will return None when missing.
        self._env_var_types: dict[str, type] = {
            # Execution/config
            "RUN_MODE": str,
            "BSKY_DATA_DIR": str,
            # Bluesky credentials
            "BLUESKY_HANDLE": str,
            "BLUESKY_PASSWORD": str,
            "DEV_BLUESKY_HANDLE": str,
            "DEV_BLUESKY_PASSWORD": str,
            # API keys / integration secrets
            "GOOGLE_API_KEY": str,
            "NYTIMES_KEY": str,
            "HF_TOKEN": str,
            "OPENAI_API_KEY": str,
            "GOOGLE_AI_STUDIO_KEY": str,
            "NEWSAPI_API_KEY": str,
            "GROQ_API_KEY": str,
            "MONGODB_URI": str,
            "LANGTRACE_API_KEY": str,
            "COMET_API_KEY": str,
        }
        self._init_lock = threading.Lock()

    @classmethod
    def get_env_var(cls, name: str) -> Any:
        """Get an environment variable value after container initialization.

        Defaults when missing:
        - `str`  -> ""
        - `int`  -> 0
        - `float`-> 0.0
        - other/unknown -> None
        """
        instance = cls._get_instance()
        expected_type = instance._env_var_types.get(name)
        raw = instance._env_vars.get(name, None)

        # Missing value: apply defaults by expected type.
        if raw is None:
            if expected_type is str:
                return ""
            if expected_type is int:
                return 0
            if expected_type is float:
                return 0.0
            return None

        # Present: cast if needed.
        if expected_type is None:
            return raw
        if expected_type is str:
            return str(raw)
        if expected_type is int:
            try:
                return int(raw)
            except (TypeError, ValueError):
                return 0
        if expected_type is float:
            try:
                return float(raw)
            except (TypeError, ValueError):
                return 0.0
        # Unknown/unsupported expected type -> return as-is.
        return raw

    @classmethod
    def _get_instance(cls) -> "EnvVarsContainer":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        cls._instance._ensure_initialized()
        return cls._instance

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self._initialize_env_vars()
            self._initialized = True

    def _initialize_env_vars(self) -> None:
        # Load .env from repo root (../.env relative to lib/)
        current_file_directory = Path(__file__).resolve().parent
        env_path = (current_file_directory / "../.env").resolve()
        load_dotenv(env_path)

        run_mode = os.getenv("RUN_MODE", "test")  # local, test, or prod
        if run_mode not in {"local", "test", "prod"}:
            raise ValueError("RUN_MODE must be set to either 'local', 'test', or 'prod'")

        # RUN_MODE is itself an "env var" we serve via this container.
        self._env_vars["RUN_MODE"] = run_mode

        if run_mode == "test":
            # Keep tests hermetic and avoid external calls.
            self._env_vars["BLUESKY_HANDLE"] = "test"
            self._env_vars["BLUESKY_PASSWORD"] = "test"
            self._env_vars["DEV_BLUESKY_HANDLE"] = "test"
            self._env_vars["DEV_BLUESKY_PASSWORD"] = "test"

            # Expand ~ for correctness; tests can control HOME.
            data_dir = os.path.expanduser("~/tmp/")
            self._env_vars["BSKY_DATA_DIR"] = data_dir
            os.makedirs(data_dir, exist_ok=True)
        else:
            self._env_vars["BLUESKY_HANDLE"] = os.getenv("BLUESKY_HANDLE")
            self._env_vars["BLUESKY_PASSWORD"] = os.getenv("BLUESKY_PASSWORD")
            self._env_vars["DEV_BLUESKY_HANDLE"] = os.getenv("DEV_BLUESKY_HANDLE")
            self._env_vars["DEV_BLUESKY_PASSWORD"] = os.getenv("DEV_BLUESKY_PASSWORD")
            self._env_vars["BSKY_DATA_DIR"] = os.getenv("BSKY_DATA_DIR")

        # Prod-only secret loading for Bluesky creds.
        if (
            run_mode == "prod"
            and (not self._env_vars.get("BLUESKY_HANDLE") or not self._env_vars.get("BLUESKY_PASSWORD"))
        ):
            bsky_credentials = json.loads(get_secret("bluesky_account_credentials"))
            self._env_vars["BLUESKY_HANDLE"] = bsky_credentials.get("bluesky_handle")
            self._env_vars["BLUESKY_PASSWORD"] = bsky_credentials.get("bluesky_password")

        # Required prod settings.
        if run_mode == "prod" and not self._env_vars.get("BSKY_DATA_DIR"):
            raise ValueError(
                "BSKY_DATA_DIR must be set to the path to the Bluesky data directory"
            )

        # Other integration keys (left as Optional[str], matching os.getenv behavior)
        self._env_vars["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")
        self._env_vars["NYTIMES_KEY"] = os.getenv("NYTIMES_KEY")
        self._env_vars["HF_TOKEN"] = os.getenv("HF_TOKEN")
        self._env_vars["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
        self._env_vars["GOOGLE_AI_STUDIO_KEY"] = os.getenv("GOOGLE_AI_STUDIO_KEY")
        self._env_vars["NEWSAPI_API_KEY"] = os.getenv("NEWSAPI_API_KEY")
        self._env_vars["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")
        self._env_vars["MONGODB_URI"] = os.getenv("MONGODB_URI")
        self._env_vars["LANGTRACE_API_KEY"] = os.getenv("LANGTRACE_API_KEY")
        self._env_vars["COMET_API_KEY"] = os.getenv("COMET_API_KEY")

