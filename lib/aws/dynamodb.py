"""Wrapper client for DynamoDB."""
from typing import Optional

from lib.aws.helper import create_client, retry_on_aws_rate_limit


class DynamoDB:
    """Wrapper class for all DynamoDB-related access."""

    def __init__(self):
        self.client = create_client("dynamodb")

    def __getattr__(self, name):
        """Delegate attribute access to the DynamoDB client."""
        try:
            return getattr(self.client, name)
        except AttributeError:
            raise AttributeError(f"'DynamoDB' object has no attribute '{name}'")

    @retry_on_aws_rate_limit
    def insert_item_into_table(self, item: dict, table_name: str) -> None:
        """Inserts an item into a table."""
        try:
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
