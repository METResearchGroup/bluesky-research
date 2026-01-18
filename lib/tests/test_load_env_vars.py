import threading
from pathlib import Path

import pytest

from lib.load_env_vars import EnvVarsContainer


@pytest.fixture(autouse=True)
def _reset_env_vars_container(monkeypatch):
    """Ensure EnvVarsContainer doesn't leak state across tests.
    
    This fixture:
    1. Mocks load_dotenv to prevent .env file from being loaded (tests should use monkeypatch for env vars)
    2. Resets the singleton instance to prevent state leakage between tests
    """
    # Mock load_dotenv to prevent .env file from interfering with tests
    # Tests should use monkeypatch.setenv/delenv to control environment variables
    monkeypatch.setattr("lib.load_env_vars.load_dotenv", lambda *args, **kwargs: None)
    
    EnvVarsContainer._instance = None
    yield
    EnvVarsContainer._instance = None


def test_test_mode_overrides_bluesky_vars_and_creates_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("BLUESKY_HANDLE", "real_handle_should_be_ignored")
    monkeypatch.setenv("BLUESKY_PASSWORD", "real_password_should_be_ignored")

    assert EnvVarsContainer.get_env_var("RUN_MODE") == "test"
    assert EnvVarsContainer.get_env_var("BLUESKY_HANDLE") == "test"
    assert EnvVarsContainer.get_env_var("BLUESKY_PASSWORD") == "test"

    data_dir = EnvVarsContainer.get_env_var("BSKY_DATA_DIR")
    assert data_dir is not None
    assert Path(data_dir).exists()
    assert Path(data_dir).is_dir()


def test_prod_mode_fetches_bluesky_creds_from_secrets_manager(monkeypatch, tmp_path, mocker):
    monkeypatch.setenv("RUN_MODE", "prod")
    monkeypatch.delenv("BLUESKY_HANDLE", raising=False)
    monkeypatch.delenv("BLUESKY_PASSWORD", raising=False)
    monkeypatch.setenv("BSKY_DATA_DIR", str(tmp_path / "data"))

    mocker.patch(
        "lib.load_env_vars.get_secret",
        return_value='{"bluesky_handle":"prod_handle","bluesky_password":"prod_password"}',
    )

    assert EnvVarsContainer.get_env_var("RUN_MODE") == "prod"
    assert EnvVarsContainer.get_env_var("BLUESKY_HANDLE") == "prod_handle"
    assert EnvVarsContainer.get_env_var("BLUESKY_PASSWORD") == "prod_password"


def test_prod_mode_requires_bsky_data_dir(monkeypatch):
    monkeypatch.setenv("RUN_MODE", "prod")
    monkeypatch.setenv("BLUESKY_HANDLE", "prod_handle")
    monkeypatch.setenv("BLUESKY_PASSWORD", "prod_password")
    monkeypatch.delenv("BSKY_DATA_DIR", raising=False)

    with pytest.raises(ValueError, match="BSKY_DATA_DIR must be set"):
        EnvVarsContainer.get_env_var("BSKY_DATA_DIR")


def test_singleton_is_thread_safe(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))

    results: list[str | None] = []
    errors: list[BaseException] = []

    def worker():
        try:
            results.append(EnvVarsContainer.get_env_var("RUN_MODE"))
        except BaseException as e:  # noqa: BLE001 (test harness)
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert results and all(r == "test" for r in results)
    assert EnvVarsContainer._instance is not None


def test_required_env_var_missing_raises_error(monkeypatch, tmp_path):
    """Test that required=True raises ValueError when env var is missing."""
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with pytest.raises(ValueError, match="OPENAI_API_KEY is required but is missing"):
        EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=True)


def test_required_env_var_empty_string_raises_error(monkeypatch, tmp_path):
    """Test that required=True raises ValueError when env var is an empty string."""
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("OPENAI_API_KEY", "")

    with pytest.raises(ValueError, match="OPENAI_API_KEY is required but is empty"):
        EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=True)


def test_required_env_var_whitespace_only_raises_error(monkeypatch, tmp_path):
    """Test that required=True raises ValueError when env var is only whitespace."""
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("OPENAI_API_KEY", "   ")

    with pytest.raises(ValueError, match="OPENAI_API_KEY is required but is empty"):
        EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=True)


def test_required_env_var_with_valid_value_returns_value(monkeypatch, tmp_path):
    """Test that required=True returns the value when env var is set."""
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-key-123")

    result = EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=True)
    assert result == "sk-test-key-123"


def test_required_false_returns_defaults_when_missing(monkeypatch, tmp_path):
    """Test that required=False (default) returns defaults when env var is missing."""
    monkeypatch.setenv("RUN_MODE", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    # Should return empty string for string type when not required
    result = EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=False)
    assert result == ""

    # Should also work with default (required=False)
    result_default = EnvVarsContainer.get_env_var("OPENAI_API_KEY")
    assert result_default == ""

