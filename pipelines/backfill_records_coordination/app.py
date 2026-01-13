"""CLI app for triggering backfill of records and writing cache buffers."""

import click
from pipelines.backfill_records_coordination.handler import lambda_handler
from pipelines.write_cache_buffers.handler import lambda_handler as write_cache_handler
from datetime import datetime
from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__name__)

INTEGRATION_MAP = {
    "p": "ml_inference_perspective_api",
    "s": "ml_inference_sociopolitical",
    "i": "ml_inference_ime",
    "v": "ml_inference_valence_classifier",
    "g": "ml_inference_intergroup",
}


def resolve_integration(integration: str) -> str:
    """Resolves integration abbreviation to full name if needed."""
    if integration in INTEGRATION_MAP:
        return INTEGRATION_MAP[integration]
    return integration


DEFAULT_INTEGRATION_KWARGS = {
    "ml_inference_perspective_api": {
        "backfill_period": None,
        "backfill_duration": None,
        "run_classification": True,
    },
    "ml_inference_sociopolitical": {
        "backfill_period": None,
        "backfill_duration": None,
        "run_classification": True,
    },
    "ml_inference_ime": {
        "backfill_period": None,
        "backfill_duration": None,
        "run_classification": True,
    },
    "ml_inference_valence_classifier": {
        "backfill_period": None,
        "backfill_duration": None,
        "run_classification": True,
    },
    "ml_inference_intergroup": {
        "backfill_period": None,
        "backfill_duration": None,
        "run_classification": True,
    },
}


def validate_date_format(ctx, param, value):
    """Validates date string format (YYYY-MM-DD)."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise click.BadParameter("Invalid date format. Please use YYYY-MM-DD format.")


@click.command()
@click.option(
    "--record-type",
    "-r",
    type=click.Choice(["posts", "posts_used_in_feeds"]),
    required=False,
    default=None,
    help="Type of records to backfill",
)
@click.option(
    "--add-to-queue",
    is_flag=True,
    default=False,
    help="Add records to integration queues",
)
@click.option(
    "--run-integrations",
    is_flag=True,
    default=False,
    help="Run the integrations after queueing",
)
@click.option(
    "--integration",
    "-i",
    type=click.Choice(
        [
            "ml_inference_perspective_api",
            "p",
            "ml_inference_sociopolitical",
            "s",
            "ml_inference_ime",
            "i",
            "ml_inference_valence_classifier",
            "v",
            "ml_inference_intergroup",
            "g",
        ]
    ),
    multiple=True,
    help="Integration service(s) to run. Can be specified multiple times. If not provided, runs all integrations.",
)
@click.option(
    "--backfill-period",
    "-p",
    type=click.Choice(["days", "hours"]),
    default=None,
    help="Period unit for backfilling (days or hours)",
)
@click.option(
    "--backfill-duration",
    "-d",
    type=int,
    default=None,
    help="Duration of backfill period",
)
@click.option(
    "--run-classification/--no-run-classification",
    default=True,
    help="Whether to run classification on posts",
)
@click.option(
    "--write-cache",
    type=click.Choice(
        [
            "all",
            "ml_inference_perspective_api",
            "ml_inference_sociopolitical",
            "ml_inference_ime",
            "ml_inference_valence_classifier",
            "ml_inference_intergroup",
            "preprocess_raw_data",
        ]
    ),
    default=None,
    help="Write cache buffer for specified service to database. Use 'all' to write all services.",
)
@click.option(
    "--clear-queue",
    is_flag=True,
    default=False,
    help="Clear the queue after writing cache buffer. Only used with --write-cache.",
)
@click.option(
    "--bypass-write",
    is_flag=True,
    default=False,
    help="Skip writing to DB and only clear cache. Only valid with --write-cache and --clear-queue.",
)
@click.option(
    "--start-date",
    type=str,
    default=None,
    callback=validate_date_format,
    help="Start date for backfill (YYYY-MM-DD format, inclusive). If provided with end-date, only processes records within date range.",
)
@click.option(
    "--end-date",
    type=str,
    default=None,
    callback=validate_date_format,
    help="End date for backfill (YYYY-MM-DD format, inclusive). If provided with start-date, only processes records within date range.",
)
@click.option(
    "--clear-input-queues",
    is_flag=True,
    default=False,
    help="Clear all input queues for specified integrations. Will prompt for confirmation.",
)
@click.option(
    "--clear-output-queues",
    is_flag=True,
    default=False,
    help="Clear all output queues for specified integrations. Will prompt for confirmation.",
)
def backfill_records(
    record_type: str | None,
    add_to_queue: bool,
    run_integrations: bool,
    integration: tuple[str, ...],
    backfill_period: str | None,
    backfill_duration: int | None,
    run_classification: bool,
    write_cache: str | None,
    clear_queue: bool,
    bypass_write: bool,
    start_date: str | None,
    end_date: str | None,
    clear_input_queues: bool,
    clear_output_queues: bool,
):
    """CLI app for triggering backfill of records and optionally writing cache buffers.

    Examples:
        # Backfill all posts for all integrations
        $ python -m pipelines.backfill_records_coordination.app -r posts

        # Only queue posts for Perspective API, don't run integration
        $ python -m pipelines.backfill_records_coordination.app -r posts -i p --no-run-integrations

        # Run existing queued posts for multiple integrations without adding new ones
        $ python -m pipelines.backfill_records_coordination.app -i p -i s --run-integrations

        # Backfill posts for last 2 days with classification
        $ python -m pipelines.backfill_records_coordination.app -r posts -p days -d 2

        # Write cache buffer for all services
        $ python -m pipelines.backfill_records_coordination.app --write-cache all

        # Write cache buffer for specific service
        $ python -m pipelines.backfill_records_coordination.app --write-cache ml_inference_perspective_api

        # Backfill posts within a date range
        $ python -m pipelines.backfill_records_coordination.app -r posts --start-date 2024-01-01 --end-date 2024-01-31

        # Clear input queues for specific integrations
        $ python -m pipelines.backfill_records_coordination.app -i p -i s --clear-input-queues

        # Clear output queues for specific integrations
        $ python -m pipelines.backfill_records_coordination.app -i p -i s --clear-output-queues
    """
    # Handle queue clearing first if requested
    if clear_input_queues or clear_output_queues:
        # Determine which integrations to clear
        integrations_to_clear = (
            [resolve_integration(i) for i in integration]
            if integration
            else DEFAULT_INTEGRATION_KWARGS.keys()
        )

        # Ask for confirmation
        queue_type = "input" if clear_input_queues else "output"
        integrations_str = ", ".join(integrations_to_clear)
        if not click.confirm(
            f"Are you sure you want to clear all {queue_type} queues for these integrations: {integrations_str}?"
        ):
            click.echo("Operation cancelled.")
            return

        # Clear the queues
        for integration_name in integrations_to_clear:
            if clear_input_queues:
                logger.warning(f"Clearing input queue for {integration_name}...")
                queue = Queue(
                    queue_name=f"input_{integration_name}", create_new_queue=True
                )
                deleted_count = queue.clear_queue()
                logger.info(
                    f"Cleared {deleted_count} items from input queue for {integration_name}"
                )

            if clear_output_queues:
                logger.warning(f"Clearing output queue for {integration_name}...")
                queue = Queue(
                    queue_name=f"output_{integration_name}", create_new_queue=True
                )
                deleted_count = queue.clear_queue()
                logger.info(
                    f"Cleared {deleted_count} items from output queue for {integration_name}"
                )

    # Validate that record_type is provided when add_to_queue is True
    if add_to_queue and not record_type:
        raise click.UsageError("--record-type is required when --add-to-queue is used")

    # Validate that posts_used_in_feeds requires both start_date and end_date
    if record_type == "posts_used_in_feeds":
        if not (start_date and end_date):
            raise click.UsageError(
                "Both --start-date and --end-date are required when record_type is 'posts_used_in_feeds'"
            )

    # Validate bypass_write usage
    if bypass_write and not (write_cache and clear_queue):
        raise click.UsageError(
            "--bypass-write requires --write-cache and --clear-queue"
        )

    # Convert integrations from abbreviations if needed
    resolved_integrations = (
        [resolve_integration(i) for i in integration] if integration else None
    )

    # Construct payload matching the format expected by backfill_records
    payload = {
        "record_type": record_type,
        "add_posts_to_queue": add_to_queue,
        "run_integrations": run_integrations,
        "integration_kwargs": {},
        "start_date": start_date,
        "end_date": end_date,
    }

    # Only proceed if adding to queue or running integrations
    if add_to_queue or run_integrations:
        # Add integration kwargs based on CLI args or defaults
        for integration_name in (
            resolved_integrations or DEFAULT_INTEGRATION_KWARGS.keys()
        ):
            if integration_name in DEFAULT_INTEGRATION_KWARGS:
                integration_kwargs = DEFAULT_INTEGRATION_KWARGS[integration_name].copy()
                integration_kwargs.update(
                    {
                        "backfill_period": backfill_period,
                        "backfill_duration": backfill_duration,
                        "run_classification": run_classification,
                    }
                )
                payload["integration_kwargs"][integration_name] = integration_kwargs

        if resolved_integrations:
            payload["integration"] = resolved_integrations

        # Call handler with constructed event
        response = lambda_handler({"payload": payload}, None)
        try:
            click.echo(f"Backfill completed with status: {response['statusCode']}")
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error(f"Response: {response}")

    # Only write cache if explicitly requested
    if write_cache:
        cache_response = write_cache_handler(
            {
                "payload": {
                    "service": write_cache,
                    "clear_queue": clear_queue,
                    "bypass_write": bypass_write,
                }
            },
            None,
        )
        click.echo(
            f"Cache buffer write completed with status: {cache_response['statusCode']}"
        )


if __name__ == "__main__":
    backfill_records()
