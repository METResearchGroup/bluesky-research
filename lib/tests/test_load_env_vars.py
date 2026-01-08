import threading
from pathlib import Path

import pytest

from lib.load_env_vars import EnvVarsContainer


@pytest.fixture(autouse=True)
def _reset_env_vars_container():
    """Ensure EnvVarsContainer doesn't leak state across tests."""
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

