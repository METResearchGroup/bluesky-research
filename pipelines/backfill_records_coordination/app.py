"""CLI app for triggering backfill of records."""

import click
from api.integrations_router.main import route_and_run_integration_request

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
    required=True,
    help="Integration service to run",
)
def run_integration(integration: str):
    """CLI app for triggering backfill of records via integrations."""
    resolved_integration = resolve_integration(integration)
    request = {"service": resolved_integration, "payload": {}, "metadata": {}}
    response = route_and_run_integration_request(request)
    click.echo(
        f"Integration {resolved_integration} completed with status: {response.statusCode}"
    )


if __name__ == "__main__":
    run_integration()
