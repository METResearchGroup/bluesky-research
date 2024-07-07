"""Wrapper class for all S3-related access."""
import gzip
import io
import json
import os
import threading
import time
from typing import Optional

import botocore

from lib.aws.helper import create_client, retry_on_aws_rate_limit

ROOT_BUCKET = "bluesky-research"
POST_BATCH_SIZE = 100

# S3 key roots
SYNC_KEY_ROOT = "sync"
FEED_KEY_ROOT = "feeds"  # final feeds after postprocessing
PREPROCESSED_DATA_KEY_ROOT = "preprocessed_data"
RECOMMENDATIONS_KEY_ROOT = "recommendations"
S3_FIREHOSE_KEY_ROOT = "firehose"
USERS_KEY_ROOT = "users"

thread_lock = threading.Lock()


class S3:
    """Wrapper class for all S3-related access."""

    def __init__(self):
        self.client = create_client("s3")

    @retry_on_aws_rate_limit
    def list_buckets(self) -> list[str]:
        """Lists buckets available in S3."""
        response = self.client.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]

    @retry_on_aws_rate_limit
    def write_to_s3(
        self, blob: bytes, key: str, bucket: str = ROOT_BUCKET
    ) -> None:
        """Writes blob to S3."""
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=blob)
        except Exception as e:
            print(f"Failure in putting object to S3: {e}")
            raise e

    # NOTE: misleading name, doesn't need to be dict. Just needs to be
    # JSON-serializable.
    def write_dict_json_to_s3(
        self, data: dict, key: str, bucket: str = ROOT_BUCKET
    ) -> None:
        """Writes dictionary as JSON to S3."""
        if not data:
            return
        if not key.endswith(".json"):
            key = f"{key}.json"
        json_body = json.dumps(data)
        json_body_bytes = bytes(json_body, "utf-8")
        self.write_to_s3(blob=json_body_bytes, bucket=bucket, key=key)

    def write_dicts_jsonl_to_s3(
        self, data: list[dict], key: str, bucket: str = ROOT_BUCKET
    ) -> None:
        """Writes list of dictionaries as JSONL to S3."""
        if not data:
            return
        if not key.endswith(".jsonl"):
            key = f"{key}.jsonl"
        jsonl_body = "\n".join([json.dumps(d) for d in data])
        jsonl_body_bytes = bytes(jsonl_body, "utf-8")
        self.write_to_s3(blob=jsonl_body_bytes, bucket=bucket, key=key)

    @retry_on_aws_rate_limit
    def read_from_s3(self, key: str, bucket: str = ROOT_BUCKET) -> Optional[bytes]:  # noqa
        """Reads blob from S3."""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Key not found in S3: {key}")
                return None
            else:
                print(f"Failure in getting object from S3: {e}")
                raise e
        except Exception as e:
            print(f"Failure in getting object from S3: {e}")
            raise e

    def read_json_from_s3(
        self, key: str, bucket: str = ROOT_BUCKET
    ) -> Optional[dict]:
        """Reads JSON from S3."""
        blob = self.read_from_s3(bucket=bucket, key=key)
        if not blob:
            return None
        if key.endswith('.gz'):
            with gzip.open(io.BytesIO(blob), 'rt', encoding='utf-8') as f:
                return json.load(f)
        return json.loads(blob)

    def read_jsonl_from_s3(self, key: str) -> Optional[list[dict]]:
        """Reads JSONL from S3."""
        blob = self.read_from_s3(key=key)
        if not blob:
            return None
        if key.endswith('.gz'):
            with gzip.open(io.BytesIO(blob), 'rt', encoding='utf-8') as f:
                jsons = f.read().split("\n")
        else:
            jsons = blob.decode("utf-8").split("\n")
        return [json.loads(j) for j in jsons if j]

    def list_keys(self):
        """Lists keys in S3."""
        response = self.client.list_objects_v2(Bucket=ROOT_BUCKET)
        return [obj["Key"] for obj in response["Contents"]]

    @retry_on_aws_rate_limit
    def list_keys_given_prefix(self, prefix: str):
        """Lists keys given a prefix in S3."""
        response = self.client.list_objects_v2(Bucket=ROOT_BUCKET, Prefix=prefix)  # noqa
        if "Contents" not in response:
            return []
        return [obj["Key"] for obj in response["Contents"]]

    @retry_on_aws_rate_limit
    def check_if_prefix_exists(self, prefix: str) -> bool:
        """Checks if prefix exists in S3."""
        response = self.client.list_objects_v2(
            Bucket=ROOT_BUCKET, Prefix=prefix
        )
        return "Contents" in response or 'CommonPrefixes' in response

    @retry_on_aws_rate_limit
    def get_keys_given_prefix(self, prefix: str) -> list[str]:
        """Gets keys given a prefix."""
        response = self.client.list_objects_v2(
            Bucket=ROOT_BUCKET, Prefix=prefix
        )
        return [obj["Key"] for obj in response["Contents"]]

    def delete_from_s3(self, key: str, bucket: str = ROOT_BUCKET) -> None:
        """Deletes object from S3."""
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(f"Failure in deleting object from S3: {e}")
            raise e

    def write_batch_posts_to_s3(
        self, posts: list[dict], batch_size: int = POST_BATCH_SIZE
    ) -> None:
        """Writes batch of posts to s3."""
        with thread_lock:
            print(f"Writing batch of {len(posts)} posts to S3 in chunks of {batch_size}...")  # noqa
            while posts:
                batch = posts[:batch_size]
                timestamp = str(int(time.time()))
                key = os.path.join(
                    S3_FIREHOSE_KEY_ROOT, f"posts_{timestamp}.jsonl"
                )
                if not isinstance(batch, list):
                    raise ValueError("Data must be a list of dictionaries.")
                try:
                    self.write_dicts_jsonl_to_s3(
                        data=batch, bucket=ROOT_BUCKET, key=key
                    )
                except Exception as e:
                    print(f"Unable to write post to S3: {e}")
                    print(f"Batch: {batch}")
                posts = posts[batch_size:]
            print(f"Finished writing {len(posts)} posts to S3.")

    def write_local_jsons_to_s3(
        self,
        directory: str,
        key: Optional[str] = None,
        filename: Optional[str] = None,
        compressed: bool = True
    ) -> None:
        """Given a directory, pull all the JSONs and write them as a .jsonl.

        Optionally, clear the cache directory of .json files after writing.
        """
        with thread_lock:
            filepaths = []
            jsons = []
            for file in os.listdir(directory):
                if file.endswith(".json"):
                    fp = os.path.join(directory, file)
                    with open(fp, "r") as f:
                        jsons.append(json.load(f))
                    filepaths.append(fp)
            if not key:
                key = os.path.join(S3_FIREHOSE_KEY_ROOT, filename)
            if compressed:
                key += ".gz"
            try:
                if compressed:
                    compressed_data = gzip.compress(bytes(json.dumps(jsons), "utf-8"))  # noqa
                    self.write_to_s3(
                        blob=compressed_data, bucket=ROOT_BUCKET, key=key
                    )
                    print(f"Successfully wrote compressed data to S3: {key}")
                else:
                    self.write_dicts_jsonl_to_s3(
                        data=jsons, bucket=ROOT_BUCKET, key=key
                    )
                    print(f"Successfully wrote data to S3: {key}")
            except Exception as e:
                print(f"Unable to write post to S3: {e}")

    @classmethod
    def create_partition_key_based_on_timestamp(cls, timestamp_str: str) -> str:
        """Given the timestamp string, create a partition key.

        Assumes the timestamp format is 'YYYY-MM-DD-HH:MM:SS'.

        Example:
        >>> create_partition_key_based_on_timestamp('2024-07-06-20:39:30')
        'year=2024/month=07/day=06/hour=20/minute=39'
        """
        # Split the timestamp string into components directly
        parts = timestamp_str.split("-")
        year, month, day = parts[:3]
        time_part = parts[3]
        hour, minute = time_part.split(":")[:2]

        # Construct the partition key string
        partition_key = f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}"
        return partition_key
