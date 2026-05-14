"""Upload analytics CSV files from local storage to S3.

By default this script runs in dry-run mode and only prints planned uploads.
Pass --execute to perform uploads.

Example mapping:
Local path:
services/calculate_analytics/analyses/toxicity_join_date_analysis_2025_09_28/statistical_analysis/two_sample_t_test/2025-09-29_11:14:00/outrage_detailed_results.csv
S3 prefix for that path:
analytics_data/services/calculate_analytics/analyses/toxicity_join_date_analysis_2025_09_28/statistical_analysis/two_sample_t_test/2025-09-29_11:14:00/

Run:
PYTHONPATH=. uv run python scripts/upload_analytics_csvs_to_s3.py
PYTHONPATH=. uv run python scripts/upload_analytics_csvs_to_s3.py --execute

Once this is done, we'll go in and manually delete the local files.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from tqdm import tqdm

from lib.aws.s3 import S3
from lib.log.logger import get_logger

logger = get_logger(__name__)

ANALYTICS_ROOT = Path("services/calculate_analytics")
S3_PREFIX_ROOT = "analytics_data"


def discover_csv_files(root: Path) -> list[Path]:
    """Return all CSV files under the provided root directory."""
    return sorted(root.rglob("*.csv"))


def get_s3_key_for_file(file_path: Path) -> str:
    """Build S3 key as analytics_data/{repo-relative-local-path}."""
    return f"{S3_PREFIX_ROOT}/{file_path.as_posix()}"


def upload_file_to_s3(local_path: Path, s3_key: str, s3_client: S3) -> None:
    """Upload one file to S3 with boto3's streamed uploader."""
    if s3_client.client is None:
        raise RuntimeError("S3 client is not initialized")
    s3_client.client.upload_file(str(local_path), s3_client.bucket, s3_key)  # type: ignore


def run(execute: bool) -> int:
    """Run upload workflow. Returns number of files processed."""
    if not ANALYTICS_ROOT.exists():
        raise FileNotFoundError(
            f"Analytics directory not found: {ANALYTICS_ROOT.as_posix()}"
        )

    csv_files = discover_csv_files(ANALYTICS_ROOT)
    if not csv_files:
        logger.info("No CSV files found under services/calculate_analytics.")
        return 0

    mode = "EXECUTE" if execute else "DRY RUN"
    logger.info(f"Mode: {mode}")
    logger.info(f"Discovered {len(csv_files)} CSV files to process.")

    s3_client = S3(create_client_flag=execute)

    if execute:
        with tqdm(csv_files, desc="Uploading CSVs to S3", unit="file") as pbar:
            for file_path in pbar:
                s3_key = get_s3_key_for_file(file_path=file_path)
                pbar.set_postfix(file=file_path.name[:48])
                try:
                    upload_file_to_s3(
                        local_path=file_path,
                        s3_key=s3_key,
                        s3_client=s3_client,
                    )
                except Exception as e:
                    logger.error(f"Failed upload for {file_path.as_posix()}: {e}")
                    raise
    else:
        for idx, file_path in enumerate(csv_files, start=1):
            s3_key = get_s3_key_for_file(file_path=file_path)
            logger.info(
                f"[{idx}/{len(csv_files)}] "
                f"{file_path.as_posix()} -> "
                f"s3://{s3_client.bucket}/{s3_key}"
            )

    logger.info(f"Completed processing {len(csv_files)} CSV files.")
    return len(csv_files)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload all CSV files under services/calculate_analytics to "
            "s3://bluesky-research/analytics_data/{local path}."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform real uploads. Without this flag, script runs in dry-run mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(execute=args.execute)


if __name__ == "__main__":
    main()
