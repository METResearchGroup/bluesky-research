"""Helper utility code for deleting old objects in S3 that should be TTLed.

We have a lifecycle policy for this now, but the lifecycle policy doesn't
apply retroactively, so objects created before that policy was in place
don't have this TTL.

We can use this script to delete old objects from the S3 bucket.
"""

from datetime import datetime, timedelta, timezone

from lib.aws.s3 import S3
from lib.log.logger import get_logger

s3_client = S3()

logger = get_logger(__name__)


def delete_old_objects(s3_client: S3, prefix: str) -> None:
    """Deletes S3 objects older than 24 hours from the specified prefix."""
    keys = s3_client.list_keys_given_prefix(prefix)
    logger.info(f"Loaded {len(keys)} objects from {prefix}")
    total_deleted_objects = 0
    for i, key in enumerate(keys):
        # Get the object's metadata to retrieve the last modified timestamp
        response = s3_client.client.head_object(Bucket=s3_client.bucket, Key=key)
        last_modified = response["LastModified"]
        now = datetime.now(timezone.utc)
        # Check if the object is older than 24 hours
        if now - last_modified > timedelta(hours=24):
            s3_client.delete_from_s3(key)
            total_deleted_objects += 1
        if i % 100 == 0:
            logger.info(
                f"Index={i}: Deleted {total_deleted_objects} old objects from {prefix}"
            )
    logger.info(f"Deleted {total_deleted_objects} old objects from {prefix}")


if __name__ == "__main__":
    prefixes = ["daily-posts/", "athena-results/", "sqs-messages/"]
    for prefix in prefixes:
        delete_old_objects(s3_client, prefix)
