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
def backfill_records(
    record_type: str,
    add_to_queue: bool,
    run_integrations: bool,
    integration: tuple[str, ...],
):
    """CLI app for triggering backfill of records.

    Examples:
        # Backfill all posts for all integrations
        $ python -m pipelines.backfill_records_coordination.app -r posts

        # Only queue posts for Perspective API, don't run integration
        $ python -m pipelines.backfill_records_coordination.app -r posts -i p --no-run-integrations

        # Run existing queued posts for multiple integrations without adding new ones
        $ python -m pipelines.backfill_records_coordination.app -r posts -i p -i s --no-add-to-queue
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
    }
    if resolved_integrations:
        payload["integration"] = resolved_integrations

    # Call handler with constructed event
    response = lambda_handler({"payload": payload}, None)
    click.echo(f"Backfill completed with status: {response['statusCode']}")


if __name__ == "__main__":
    backfill_records()
