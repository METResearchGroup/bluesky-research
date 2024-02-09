import json
import os
import threading
import time

import polars as pl

from lib.aws.helper import create_client, retry_on_aws_rate_limit

ROOT_BUCKET = "bluesky-research"
POST_BATCH_SIZE = 100
S3_FIREHOSE_KEY_ROOT = "firehose"


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

    @retry_on_aws_rate_limit
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
    
    def write_batch_posts_to_s3(
        self, posts: list[dict], batch_size: int = POST_BATCH_SIZE
    ) -> None:
        """Writes batch of posts to s3."""
        with thread_lock:
            print(f"Writing batch of {len(posts)} posts to S3 in chunks of {batch_size}...") # noqa
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
        self, dir: str,
        filename: str,
        clear_cache: bool = True
    ) -> None:
        """Given a directory, pull all the JSONs and write them as a .jsonl.

        Optionally, clear the cache directory of .json files after writing.
        """
        with thread_lock:
            filepaths = []
            jsons = []
            for file in os.listdir(dir):
                if file.endswith(".json"):
                    fp = os.path.join(dir, file)
                    with open(fp, "r") as f:
                        jsons.append(json.load(f))
                    filepaths.append(fp)
            key = os.path.join(S3_FIREHOSE_KEY_ROOT, filename)
            try:
                self.write_dicts_jsonl_to_s3(
                    data=jsons, bucket=ROOT_BUCKET, key=key
                )
            except Exception as e:
                print(f"Unable to write post to S3: {e}")
            if clear_cache:
                for fp in filepaths:
                    os.remove(fp)

    def return_jsons_as_polars_df(self, bucket: str, key: str) -> pl.DataFrame:
        """Reads JSONL from S3 and returns as Polars DataFrame."""
        blob = self.read_from_s3(bucket, key)
        jsons = blob.decode("utf-8").split("\n")
        return pl.DataFrame(jsons)
