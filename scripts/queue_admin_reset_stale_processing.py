"""Admin helper: reset stale processing queue rows back to pending.

This is an operational escape hatch for the concurrency-safe queue claiming
mechanism used by ML inference integrations. If a worker crashes after claiming
rows (pending -> processing) but before completing/deleting them, those rows can
remain in 'processing' and will not be picked up by future runs (which only
select pending rows).

Example:
  BSKY_DATA_DIR=... \
  uv run python scripts/queue_admin_reset_stale_processing.py \
    --queue-name input_ml_inference_intergroup \
    --older-than-hours 6 \
    --apply
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import click

from lib.db.queue import Queue
from lib.constants import timestamp_format


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_pipeline_ts(dt: datetime) -> str:
    return dt.strftime(timestamp_format)


@click.command()
@click.option(
    "--queue-name",
    required=True,
    type=str,
    help="Queue name (e.g. input_ml_inference_intergroup).",
)
@click.option(
    "--older-than-hours",
    required=True,
    type=int,
    help="Reset processing rows with created_at older than this many hours.",
)
@click.option(
    "--apply",
    is_flag=True,
    default=False,
    help="Apply changes. If omitted, runs in dry-run mode (no updates).",
)
def main(queue_name: str, older_than_hours: int, apply: bool) -> None:
    if older_than_hours <= 0:
        raise click.UsageError("--older-than-hours must be > 0")

    cutoff_dt = _utc_now() - timedelta(hours=older_than_hours)
    cutoff_ts = _format_pipeline_ts(cutoff_dt)

    queue = Queue(queue_name=queue_name)

    dry_run = not apply
    count = queue.reset_processing_items_to_pending(
        older_than_timestamp=cutoff_ts,
        dry_run=dry_run,
    )

    mode = "DRY RUN" if dry_run else "APPLIED"
    click.echo(f"[{mode}] queue={queue_name} cutoff={cutoff_ts} reset_count={count}")


if __name__ == "__main__":
    main()
