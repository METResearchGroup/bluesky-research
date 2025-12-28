from __future__ import annotations

import importlib
import sys
from unittest.mock import Mock

import boto3


def test_get_users_whose_social_network_has_been_fetched(monkeypatch):
    # Ensure the workspace member package is importable in test runs that rely
    # only on PYTHONPATH=/workspace (without `pip install -e`).
    sys.path.insert(0, "/workspace/pipelines/get_existing_user_social_network/src")

    # Avoid any real AWS client/resource initialization at import time.
    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr("lib.aws.s3.create_client", lambda _name: Mock())
    mock_ddb_resource = Mock()
    mock_ddb_resource.Table = Mock(return_value=Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: mock_ddb_resource)

    # Avoid creating a real atproto client at import time.
    import lib.helper

    monkeypatch.setattr(lib.helper, "get_client", lambda: Mock())

    mod = importlib.import_module("get_existing_user_social_network.helper")
    importlib.reload(mod)

    mod.dynamodb.get_all_items_from_table = Mock(
        return_value=[{"user_handle": "a.bsky.social"}, {"user_handle": "b.bsky.social"}]
    )

    assert mod.get_users_whose_social_network_has_been_fetched() == {
        "a.bsky.social",
        "b.bsky.social",
    }


def test_get_users_whose_social_network_has_been_fetched_empty(monkeypatch):
    sys.path.insert(0, "/workspace/pipelines/get_existing_user_social_network/src")

    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr("lib.aws.s3.create_client", lambda _name: Mock())
    mock_ddb_resource = Mock()
    mock_ddb_resource.Table = Mock(return_value=Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: mock_ddb_resource)

    import lib.helper

    monkeypatch.setattr(lib.helper, "get_client", lambda: Mock())

    mod = importlib.import_module("get_existing_user_social_network.helper")
    importlib.reload(mod)

    mod.dynamodb.get_all_items_from_table = Mock(return_value=[])
    assert mod.get_users_whose_social_network_has_been_fetched() == set()

