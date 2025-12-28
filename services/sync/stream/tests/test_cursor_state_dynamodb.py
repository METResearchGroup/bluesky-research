from __future__ import annotations

import importlib
from unittest.mock import Mock

import boto3


def test_load_cursor_state_dynamodb_returns_model(monkeypatch):
    # Avoid real AWS client/resource initialization during module import.
    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr("lib.aws.s3.create_client", lambda _name: Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: Mock())

    import services.sync.stream.streaming.cursor as cursor

    importlib.reload(cursor)

    cursor.dynamodb.get_item_from_table = Mock(
        return_value={"service": "svc", "cursor": 123, "timestamp": "2025-01-01-00:00:00"}
    )

    out = cursor.load_cursor_state_dynamodb(service_name="svc")
    assert out is not None
    assert out.service == "svc"
    assert out.cursor == 123
    assert out.timestamp == "2025-01-01-00:00:00"


def test_load_cursor_state_dynamodb_returns_none(monkeypatch):
    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr("lib.aws.s3.create_client", lambda _name: Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: Mock())

    import services.sync.stream.streaming.cursor as cursor

    importlib.reload(cursor)

    cursor.dynamodb.get_item_from_table = Mock(return_value=None)
    assert cursor.load_cursor_state_dynamodb(service_name="svc") is None

