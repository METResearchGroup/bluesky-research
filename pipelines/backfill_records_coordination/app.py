"""CLI app for triggering backfill of records and writing cache buffers."""

from datetime import datetime

import click

from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.models import (
    EnqueueServicePayload,
    BackfillPeriod,
    IntegrationRunnerConfigurationPayload,
)
from services.backfill.services.enqueue_service import EnqueueService
from services.backfill.services.integration_runner_service import (
    IntegrationRunnerService,
)
from services.backfill.services.cache_flusher_service import CacheFlusherService

logger = get_logger(__name__)

INTEGRATION_MAP = {
    "p": "ml_inference_perspective_api",
    "s": "ml_inference_sociopolitical",
    "i": "ml_inference_ime",
    "v": "ml_inference_valence_classifier",
    "g": "ml_inference_intergroup",
}


DEFAULT_INTEGRATION_KWARGS = {
    "backfill_period": BackfillPeriod.DAYS.value,
    "backfill_duration": None,
}

enqueue_service = EnqueueService()
integration_runner_service = IntegrationRunnerService()
cache_flusher_service = CacheFlusherService()


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
    "--integrations",
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
    integrations: tuple[str, ...],
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
    mapped_integration_names: list[str] = _resolve_integration_names(integrations)

    # first, we clear out the queues (if the user requested it).
    # this allows us to start with a fresh slate.
    # In our use case, we assume that if a user both (1) wants to clear an input/output
    # queue and (2) specifies an integration name, that the user wants to clear out
    # the queue(s) for that integration.
    manage_queue_clearing(
        integrations_to_clear=mapped_integration_names,
        clear_input_queues=clear_input_queues,
        clear_output_queues=clear_output_queues,
    )

    run_validation_checks(
        add_to_queue=add_to_queue,
        record_type=record_type,
        run_integrations=run_integrations,
        integrations=integrations,
        write_cache=write_cache,
        clear_queue=clear_queue,
        bypass_write=bypass_write,
        start_date=start_date,
        end_date=end_date,
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

    # TODO: add defaults for start_date and end_date, and
    # then make sure that they're used here.
    if add_to_queue:
        enqueue_service_payload = EnqueueServicePayload(
            record_type=str(record_type),
            integrations=mapped_integration_names,
            start_date=str(start_date),
            end_date=str(end_date),
        )
        enqueue_service.enqueue_records(payload=enqueue_service_payload)
    if run_integrations:
        integration_backfill_period: str = (
            DEFAULT_INTEGRATION_KWARGS["backfill_period"]
            if backfill_period is None
            else backfill_period
        )
        integration_backfill_duration: int | None = (
            DEFAULT_INTEGRATION_KWARGS["backfill_duration"]
            if backfill_duration is None
            else backfill_duration
        )
        integration_runner_configuration_payloads: list[
            IntegrationRunnerConfigurationPayload
        ] = [
            IntegrationRunnerConfigurationPayload(
                integration_name=integration_name,
                backfill_period=BackfillPeriod(integration_backfill_period),
                backfill_duration=integration_backfill_duration,
            )
            for integration_name in mapped_integration_names
        ]
        integration_runner_service.run_integrations(
            payloads=integration_runner_configuration_payloads
        )
    if write_cache:
        cache_flusher_service.write_cache(payload=payload)

    # Only proceed if adding to queue or running integrations
    # TODO: will refactor into separate services for delegation.
    # if add_to_queue or run_integrations:
    #     # Add integration kwargs based on CLI args or defaults
    #     for integration_name in (
    #         mapped_integration_names or DEFAULT_INTEGRATION_KWARGS.keys()
    #     ):
    #         if integration_name in DEFAULT_INTEGRATION_KWARGS:
    #             integration_kwargs = DEFAULT_INTEGRATION_KWARGS[integration_name].copy()
    #             integration_kwargs.update(
    #                 {
    #                     "backfill_period": backfill_period,
    #                     "backfill_duration": backfill_duration,
    #                     "run_classification": run_classification,
    #                 }
    #             )
    #             payload["integration_kwargs"][integration_name] = integration_kwargs

    #     if mapped_integration_names:
    #         payload["integration"] = mapped_integration_names

    #     # Call handler with constructed event
    #     response = lambda_handler({"payload": payload}, None)
    #     try:
    #         click.echo(f"Backfill completed with status: {response['statusCode']}")
    #     except Exception as e:
    #         logger.error(f"Error: {e}")
    #         logger.error(f"Response: {response}")

    # Only write cache if explicitly requested
    # if write_cache:
    #     cache_response = write_cache_handler(
    #         {
    #             "payload": {
    #                 "service": write_cache,
    #                 "clear_queue": clear_queue,
    #                 "bypass_write": bypass_write,
    #             }
    #         },
    #         None,
    #     )
    #     click.echo(
    #         f"Cache buffer write completed with status: {cache_response['statusCode']}"
    #     )


def manage_queue_clearing(
    integrations_to_clear: list[str],
    clear_input_queues: bool,
    clear_output_queues: bool,
):
    """Clears input/output queues for specified integrations."""
    if not clear_input_queues and not clear_output_queues:
        logger.info("Not clearing any queues.")
        return

    _validate_clear_queues(
        clear_input_queues=clear_input_queues,
        clear_output_queues=clear_output_queues,
        integrations_to_clear=integrations_to_clear,
    )

    _clear_queues(
        integrations_to_clear=integrations_to_clear,
        clear_input_queues=clear_input_queues,
        clear_output_queues=clear_output_queues,
    )


def _resolve_single_integration(integration: str) -> str:
    """Resolves integration abbreviation to full name if needed."""
    if integration in INTEGRATION_MAP:
        return INTEGRATION_MAP[integration]
    return integration


def _resolve_integration_names(integrations: tuple[str, ...] | None) -> list[str]:
    if integrations is None or len(integrations) == 0:
        return []
    return [_resolve_single_integration(i) for i in integrations]


def _validate_clear_queues(
    clear_input_queues: bool,
    clear_output_queues: bool,
    integrations_to_clear: list[str],
):
    queue_type = (
        "input" if clear_input_queues else "output" if clear_output_queues else None
    )
    if queue_type is None:
        raise ValueError(
            "Either --clear-input-queues or --clear-output-queues must be True"
        )
    integrations_str = ", ".join(integrations_to_clear)
    if not click.confirm(
        f"Are you sure you want to clear all {queue_type} queues for these integrations: {integrations_str}?"
    ):
        raise click.UsageError("Operation cancelled.")


def _clear_queues(
    integrations_to_clear: list[str],
    clear_input_queues: bool,
    clear_output_queues: bool,
):
    for integration_name in integrations_to_clear:
        if clear_input_queues:
            _clear_input_queue(integration_name=integration_name)
        if clear_output_queues:
            _clear_output_queue(integration_name=integration_name)


def _clear_input_queue(integration_name: str):
    queue_name = f"input_{integration_name}"
    deleted_count = _clear_single_queue(queue_name)
    logger.info(f"Cleared {deleted_count} items from {queue_name}")


def _clear_output_queue(integration_name: str):
    logger.warning(f"Clearing output queue for {integration_name}...")
    queue_name = f"output_{integration_name}"
    deleted_count = _clear_single_queue(queue_name)
    logger.info(f"Cleared {deleted_count} items from {queue_name}")


def _clear_single_queue(queue_name: str) -> int:
    logger.warning(f"Clearing {queue_name}...")
    queue = Queue(queue_name=queue_name, create_new_queue=True)
    deleted_count = queue.clear_queue()
    logger.info(f"Cleared {deleted_count} items from {queue_name}")
    return deleted_count


def run_validation_checks(
    add_to_queue: bool,
    record_type: str | None,
    run_integrations: bool,
    integrations: tuple[str, ...],
    write_cache: str | None,
    clear_queue: bool,
    bypass_write: bool,
    start_date: str | None,
    end_date: str | None,
):
    """Validates CLI arguments and raises click.UsageError if invalid."""
    # Validate that record_type is provided when add_to_queue is True
    if add_to_queue and not record_type:
        raise click.UsageError("--record-type is required when --add-to-queue is used")

    # Running integrations always requires explicit integrations (avoid accidental "run everything").
    if run_integrations and (not integrations):
        raise click.UsageError(
            "--integrations is required when --run-integrations is used"
        )

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


if __name__ == "__main__":
    backfill_records()
