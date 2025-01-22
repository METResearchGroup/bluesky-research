"""CLI app for triggering backfill of records."""

import click
from pipelines.backfill_records_coordination.handler import lambda_handler

INTEGRATION_MAP = {
    "p": "ml_inference_perspective_api",
    "s": "ml_inference_sociopolitical",
    "i": "ml_inference_ime",
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
    }
}


@click.command()
@click.option(
    "--record-type",
    "-r",
    type=click.Choice(["posts"]),
    required=True,
    help="Type of records to backfill",
)
@click.option(
    "--add-to-queue/--no-add-to-queue",
    default=True,
    help="Whether to add records to integration queues",
)
@click.option(
    "--run-integrations/--no-run-integrations",
    default=True,
    help="Whether to run the integrations after queueing",
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
def backfill_records(
    record_type: str,
    add_to_queue: bool,
    run_integrations: bool,
    integration: tuple[str, ...],
    backfill_period: str | None,
    backfill_duration: int | None,
    run_classification: bool,
):
    """CLI app for triggering backfill of records.

    Examples:
        # Backfill all posts for all integrations
        $ python -m pipelines.backfill_records_coordination.app -r posts

        # Only queue posts for Perspective API, don't run integration
        $ python -m pipelines.backfill_records_coordination.app -r posts -i p --no-run-integrations

        # Run existing queued posts for multiple integrations without adding new ones
        $ python -m pipelines.backfill_records_coordination.app -r posts -i p -i s --no-add-to-queue

        # Backfill posts for last 2 days with classification
        $ python -m pipelines.backfill_records_coordination.app -r posts -p days -d 2
    """
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
    }

    # Add integration kwargs based on CLI args or defaults
    for integration_name in resolved_integrations or DEFAULT_INTEGRATION_KWARGS.keys():
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

    breakpoint()

    # Call handler with constructed event
    response = lambda_handler({"payload": payload}, None)
    click.echo(f"Backfill completed with status: {response['statusCode']}")


if __name__ == "__main__":
    backfill_records()
