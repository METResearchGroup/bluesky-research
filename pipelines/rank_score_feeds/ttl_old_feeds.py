"""TTLs old feeds from the active directory to the cache directory."""

from lib.aws.s3 import S3
from lib.log.logger import get_logger

s3 = S3()

logger = get_logger(__name__)


def main() -> None:
    logger.info("TTLing old feeds from active to cache.")
    # we name the files with the timestamp, so we sort by key
    s3.sort_and_move_files_from_active_to_cache(
        prefix="custom_feeds", keep_count=3, sort_field="Key"
    )
    logger.info("Done TTLing old feeds from active to cache.")


if __name__ == "__main__":
    main()
