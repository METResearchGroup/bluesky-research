from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import boto3

_HELPER_PATH = Path(
    "/workspace/pipelines/get_existing_user_social_network/src/get_existing_user_social_network/helper.py"
)


def _load_helper_module():
    """Load the pipeline helper module without sys.path modifications.

    CI runs tests with `PYTHONPATH=/workspace` (repo root), which does not include
    the workspace member's `src/` directory on sys.path. To keep this test
    hermetic and avoid runtime sys.path mutations, we load the module directly
    from its file path.
    """
    spec = importlib.util.spec_from_file_location(
        "_get_existing_user_social_network_helper", _HELPER_PATH
    )
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_get_users_whose_social_network_has_been_fetched(monkeypatch):
    # Avoid any real AWS client/resource initialization at import time.
    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr("lib.aws.s3.create_client", lambda _name: Mock())
    mock_ddb_resource = Mock()
    mock_ddb_resource.Table = Mock(return_value=Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: mock_ddb_resource)

    # Avoid creating a real atproto client at import time.
    import lib.helper

    monkeypatch.setattr(lib.helper, "get_client", lambda: Mock())

    mod = _load_helper_module()

    mod.dynamodb.get_all_items_from_table = Mock(
        return_value=[{"user_handle": "a.bsky.social"}, {"user_handle": "b.bsky.social"}]
    )

    assert mod.get_users_whose_social_network_has_been_fetched() == {
        "a.bsky.social",
        "b.bsky.social",
    }


def test_get_users_whose_social_network_has_been_fetched_empty(monkeypatch):
    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr("lib.aws.s3.create_client", lambda _name: Mock())
    mock_ddb_resource = Mock()
    mock_ddb_resource.Table = Mock(return_value=Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: mock_ddb_resource)

    import lib.helper

    monkeypatch.setattr(lib.helper, "get_client", lambda: Mock())

    mod = _load_helper_module()

    mod.dynamodb.get_all_items_from_table = Mock(return_value=[])
    assert mod.get_users_whose_social_network_has_been_fetched() == set()

