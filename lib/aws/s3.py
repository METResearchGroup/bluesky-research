import json

from lib.aws.helper import create_client

ROOT_BUCKET = "bluesky-research"


class S3:
    """Wrapper class for all S3-related access."""

    def __init__(self):
        self.client = create_client("s3")

    def list_buckets(self) -> list[str]:
        """Lists buckets available in S3."""
        response = self.client.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]

    def write_to_s3(self, blob: bytes, bucket: str, key: str) -> None:
        """Writes blob to S3."""
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=blob)
        except Exception as e:
            print(f"Failure in putting object to S3: {e}")
            raise e

    def write_dict_json_to_s3(
        self, data: dict, bucket: str, key: str
    ) -> None:
        """Writes dictionary as JSON to S3."""
        if not data:
            return
        if not key.endswith(".json"):
            key = f"{key}.json"
        json_body = json.dumps(data)
        json_body_bytes = bytes(json_body, "utf-8")
        self.write_to_s3(json_body_bytes, bucket, key)

    def write_dicts_jsonl_to_s3(
        self, data: list[dict], bucket: str, key: str
    ) -> None:
        """Writes list of dictionaries as JSONL to S3."""
        if not data:
            return
        if not key.endswith(".jsonl"):
            key = f"{key}.jsonl"
        jsonl_body = "\n".join([json.dumps(d) for d in data])
        jsonl_body_bytes = bytes(jsonl_body, "utf-8")
        self.write_to_s3(jsonl_body_bytes, bucket, key)

    def read_from_s3(self, bucket: str, key: str) -> bytes:
        """Reads blob from S3."""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            print(f"Failure in getting object from S3: {e}")
            raise e

    def read_json_from_s3(self, bucket: str, key: str) -> dict:
        """Reads JSON from S3."""
        blob = self.read_from_s3(bucket, key)
        return json.loads(blob)
