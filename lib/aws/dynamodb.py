"""Wrapper client for DynamoDB."""

from typing import Optional

import boto3
from boto3.dynamodb.types import TypeSerializer

from lib.aws.helper import create_client, retry_on_aws_rate_limit

serializer = TypeSerializer()


class DynamoDB:
    """Wrapper class for all DynamoDB-related access."""

    def __init__(self):
        self.client = create_client("dynamodb")
        self.resource = boto3.resource("dynamodb")

    def __getattr__(self, name):
        """Delegate attribute access to the DynamoDB client."""
        try:
            return getattr(self.client, name)
        except AttributeError:
            raise AttributeError(f"'DynamoDB' object has no attribute '{name}'")

    def is_serialized(self, item: dict) -> bool:
        """Check if the item is already serialized for DynamoDB."""
        if not isinstance(item, dict):
            return False
        for value in item.values():
            if not isinstance(value, dict) or len(value) != 1:
                return False
            if not any(k in value for k in ["S", "N", "B", "BOOL", "NULL", "M", "L"]):
                return False
        return True

    @retry_on_aws_rate_limit
    def insert_item_into_table(self, item: dict, table_name: str) -> None:
        """Inserts an item into a table."""
        try:
            if not self.is_serialized(item):
                item = {k: serializer.serialize(v) for k, v in item.items()}
            self.client.put_item(TableName=table_name, Item=item)
        except Exception as e:
            print(f"Failure in putting item to DynamoDB: {e}")
            raise e

    @retry_on_aws_rate_limit
    def get_item_from_table(self, key: dict, table_name: str) -> Optional[dict]:  # noqa
        """Gets an item from a table. If item doesn't exist, return None."""
        try:
            response = self.client.get_item(TableName=table_name, Key=key)
            return response.get("Item")
        except Exception as e:
            print(f"Failure in getting item from DynamoDB: {e}")
            raise e

    @retry_on_aws_rate_limit
    def get_all_items_from_table(self, table_name: str) -> list[dict]:
        """Gets all items from a table."""
        try:
            response = self.client.scan(TableName=table_name)
            return response.get("Items")
        except Exception as e:
            print(f"Failure in getting all items from DynamoDB: {e}")
            raise e

    @retry_on_aws_rate_limit
    def query_items_by_service(self, table_name: str, service: str) -> list[dict]:  # noqa
        """Query items by service."""
        try:
            response = self.client.query(
                TableName=table_name,
                IndexName="service-index",
                KeyConditionExpression="service = :service",
                ExpressionAttributeValues={":service": {"S": service}},
            )
            return response.get("Items", [])
        except Exception as e:
            print(f"Failure in querying items from DynamoDB: {e}")
            raise e

    @retry_on_aws_rate_limit
    def query_items_by_inference_type(
        self, table_name: str, inference_type: str
    ) -> list[dict]:  # noqa
        """Query items by inference_type."""
        try:
            response = self.client.query(
                TableName=table_name,
                IndexName="inference_type-index",
                KeyConditionExpression="inference_type = :inference_type",
                ExpressionAttributeValues={":inference_type": {"S": inference_type}},
            )
            return response.get("Items", [])
        except Exception as e:
            print(f"Failure in querying items from DynamoDB: {e}")
            raise e
