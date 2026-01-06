from __future__ import annotations

from unittest.mock import Mock, call

import boto3
import pytest

from lib.aws.dynamodb import DynamoDB


@pytest.fixture()
def dynamodb(monkeypatch: pytest.MonkeyPatch) -> DynamoDB:
    # Avoid any real AWS client/resource initialization in unit tests.
    monkeypatch.setattr("lib.aws.dynamodb.create_client", lambda _name: Mock())
    monkeypatch.setattr(boto3, "resource", lambda _name: Mock())
    return DynamoDB()


def test_get_item_from_table_deserializes_types(dynamodb: DynamoDB) -> None:
    dynamodb.client.get_item = Mock(
        return_value={
            "Item": {
                "service": {"S": "svc"},
                "cursor": {"N": "123"},
                "ratio": {"N": "0.5"},
                "flag": {"BOOL": True},
                "nullish": {"NULL": True},
                "tags": {"L": [{"S": "x"}, {"S": "y"}]},
                "nested": {"M": {"a": {"N": "1"}, "b": {"S": "two"}}},
            }
        }
    )

    out = dynamodb.get_item_from_table(key={"service": {"S": "svc"}}, table_name="t")
    assert out == {
        "service": "svc",
        "cursor": 123,
        "ratio": 0.5,
        "flag": True,
        "nullish": None,
        "tags": ["x", "y"],
        "nested": {"a": 1, "b": "two"},
    }


def test_get_item_from_table_returns_none_when_missing(dynamodb: DynamoDB) -> None:
    dynamodb.client.get_item = Mock(return_value={})
    out = dynamodb.get_item_from_table(key={"pk": {"S": "x"}}, table_name="t")
    assert out is None


def test_get_all_items_from_table_deserializes_items(dynamodb: DynamoDB) -> None:
    dynamodb.client.scan = Mock(
        return_value={
            "Items": [
                {"k": {"S": "a"}, "n": {"N": "1"}},
                {"k": {"S": "b"}, "n": {"N": "2"}},
            ]
        }
    )

    out = dynamodb.get_all_items_from_table(table_name="t")
    assert out == [{"k": "a", "n": 1}, {"k": "b", "n": 2}]


def test_get_all_items_from_table_paginates_scan(dynamodb: DynamoDB) -> None:
    last_evaluated_key = {"k": {"S": "b"}}
    dynamodb.client.scan = Mock(
        side_effect=[
            {
                "Items": [{"k": {"S": "a"}, "n": {"N": "1"}}],
                "LastEvaluatedKey": last_evaluated_key,
            },
            {"Items": [{"k": {"S": "c"}, "n": {"N": "3"}}]},
        ]
    )

    out = dynamodb.get_all_items_from_table(table_name="t")
    assert out == [{"k": "a", "n": 1}, {"k": "c", "n": 3}]
    assert dynamodb.client.scan.call_args_list == [
        call(TableName="t"),
        call(TableName="t", ExclusiveStartKey=last_evaluated_key),
    ]

