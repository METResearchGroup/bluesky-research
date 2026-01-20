"""CLI app for triggering backfill of records and writing cache buffers."""

from datetime import datetime

import click

from lib.log.logger import get_logger
from services.backfill.models import (
    EnqueueServicePayload,
    BackfillPeriod,
    IntegrationRunnerConfigurationPayload,
    IntegrationRunnerServicePayload,
)
from services.backfill.services.enqueue_service import EnqueueService
from services.backfill.services.integration_runner_service import (
    IntegrationRunnerService,
)
from services.backfill.services.cache_buffer_writer_service import (
    CacheBufferWriterService,
)
from services.backfill.services.backfill_data_loader_service import (
    BackfillDataLoaderService,
)
from services.backfill.repositories.adapters import LocalStorageAdapter, S3Adapter
from services.backfill.repositories.repository import BackfillDataRepository

logger = get_logger(__name__)

INTEGRATION_ABBREVIATION_TO_NAME_MAP = {
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


def validate_date_format(ctx, param, value):
    """Validates date string format (YYYY-MM-DD)."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise click.BadParameter("Invalid date format. Please use YYYY-MM-DD format.")


def _build_backfill_data_repository(
    source_data_location: str,
) -> BackfillDataRepository:
    """Build the data repository used for loading posts and label history."""
    if source_data_location == "local":
        return BackfillDataRepository(adapter=LocalStorageAdapter())
    if source_data_location == "s3":
        return BackfillDataRepository(adapter=S3Adapter())
    raise ValueError(f"Invalid source_data_location: {source_data_location}")


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
    "--source-data-location",
    type=click.Choice(["local", "s3"]),
    default="local",
    show_default=True,
    help="Where to read posts (and label history) from.",
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
    "--write-cache-buffer-to-storage",
    is_flag=True,
    default=False,
    help="Write cache buffer to persistent storage. Requires --service-source-buffer to specify which service.",
)
@click.option(
    "--service-source-buffer",
    type=click.Choice(
        [
            "ml_inference_perspective_api",
            "ml_inference_sociopolitical",
            "ml_inference_ime",
            "ml_inference_valence_classifier",
            "ml_inference_intergroup",
            "preprocess_raw_data",
        ]
    ),
    default=None,
    help="Service whose cache buffer to write to storage. Only used with --write-cache-buffer-to-storage.",
)
@click.option(
    "--clear-queue",
    is_flag=True,
    default=False,
    help="Clear the queue after writing cache buffer. Only used with --write-cache-buffer-to-storage.",
)
@click.option(
    "--bypass-write",
    is_flag=True,
    default=False,
    help="Skip writing to DB and only clear cache. Only valid with --write-cache-buffer-to-storage and --clear-queue.",
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
    source_data_location: str,
    backfill_period: str | None,
    backfill_duration: int | None,
    write_cache_buffer_to_storage: bool,
    service_source_buffer: str | None,
    clear_queue: bool,
    bypass_write: bool,
    start_date: str | None,
    end_date: str | None,
    clear_input_queues: bool,
    clear_output_queues: bool,
    _enqueue_service: EnqueueService | None = None,
    _integration_runner_service: IntegrationRunnerService | None = None,
    _cache_buffer_writer_service: CacheBufferWriterService | None = None,
):
    """CLI app for triggering backfill of records and optionally writing cache buffers."""
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
        write_cache_buffer_to_storage=write_cache_buffer_to_storage,
        service_source_buffer=service_source_buffer,
        clear_queue=clear_queue,
        bypass_write=bypass_write,
        start_date=start_date,
        end_date=end_date,
    )

    if add_to_queue:
        repo = _build_backfill_data_repository(
            source_data_location=source_data_location
        )
        data_loader = BackfillDataLoaderService(data_repository=repo)
        enqueue_svc = _enqueue_service or EnqueueService(
            backfill_data_loader_service=data_loader
        )
        enqueue_service_payload = EnqueueServicePayload(
            record_type=str(record_type),
            integrations=mapped_integration_names,
            start_date=str(start_date),
            end_date=str(end_date),
        )
        enqueue_svc.enqueue_records(payload=enqueue_service_payload)

    if run_integrations:
        integration_runner_svc = (
            _integration_runner_service or IntegrationRunnerService()
        )
        integration_runner_service_payload = _create_integration_runner_payload(
            mapped_integration_names=mapped_integration_names,
            backfill_period=backfill_period,
            backfill_duration=backfill_duration,
        )
        integration_runner_svc.run_integrations(
            payload=integration_runner_service_payload
        )

    if write_cache_buffer_to_storage:
        repo = _build_backfill_data_repository(
            source_data_location=source_data_location
        )
        cache_buffer_writer_svc = (
            _cache_buffer_writer_service
            or CacheBufferWriterService(data_repository=repo)
        )
        if not bypass_write:
            cache_buffer_writer_svc.write_cache(
                integration_name=str(service_source_buffer)
            )
        if clear_queue:
            cache_buffer_writer_svc.clear_cache(
                integration_name=str(service_source_buffer)
            )


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


def _create_integration_runner_payload(
    mapped_integration_names: list[str],
    backfill_period: str | None,
    backfill_duration: int | None,
) -> IntegrationRunnerServicePayload:
    """Creates an IntegrationRunnerServicePayload from integration names and backfill parameters.

    Args:
        mapped_integration_names: List of integration names to run
        backfill_period: Backfill period (days or hours), or None to use default
        backfill_duration: Backfill duration, or None to use default

    Returns:
        IntegrationRunnerServicePayload configured with the provided parameters
    """
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
    integration_configs: list[IntegrationRunnerConfigurationPayload] = [
        IntegrationRunnerConfigurationPayload(
            integration_name=integration_name,
            backfill_period=BackfillPeriod(integration_backfill_period),
            backfill_duration=integration_backfill_duration,
        )
        for integration_name in mapped_integration_names
    ]
    return IntegrationRunnerServicePayload(
        integration_configs=integration_configs,
    )


def _resolve_single_integration(integration: str) -> str:
    """Resolves integration abbreviation to full name if needed."""
    if integration in INTEGRATION_ABBREVIATION_TO_NAME_MAP:
        return INTEGRATION_ABBREVIATION_TO_NAME_MAP[integration]
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
    from services.backfill.services.queue_manager_service import QueueManagerService

    queue_manager_svc = QueueManagerService()
    for integration_name in integrations_to_clear:
        if clear_input_queues:
            queue_manager_svc.delete_records_from_queue(
                integration_name=integration_name, queue_type="input"
            )
        if clear_output_queues:
            queue_manager_svc.delete_records_from_queue(
                integration_name=integration_name, queue_type="output"
            )


def run_validation_checks(
    add_to_queue: bool,
    record_type: str | None,
    run_integrations: bool,
    integrations: tuple[str, ...],
    write_cache_buffer_to_storage: bool,
    service_source_buffer: str | None,
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

    # Validate that add_to_queue requires both start_date and end_date
    if add_to_queue:
        if not integrations:
            raise click.UsageError(
                "At least one --integrations value is required when --add-to-queue is used"
            )
        if not (start_date and end_date):
            raise click.UsageError(
                "Both --start-date and --end-date are required when --add-to-queue is used"
            )

    # Validate write_cache_buffer_to_storage usage
    if write_cache_buffer_to_storage and not service_source_buffer:
        raise click.UsageError(
            "--service-source-buffer is required when --write-cache-buffer-to-storage is used"
        )

    # Validate bypass_write usage
    if bypass_write and not (write_cache_buffer_to_storage and clear_queue):
        raise click.UsageError(
            "--bypass-write requires --write-cache-buffer-to-storage and --clear-queue"
        )


if __name__ == "__main__":
    backfill_records()
