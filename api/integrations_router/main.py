"""Interface for routing integration requests to the appropriate integration service."""

from api.integrations_router.models import IntegrationRequest, RunExecutionMetadata
from api.integrations_router.run import run_integration_request


def parse_integration_request(request: dict) -> IntegrationRequest:
    """Parses an integration request into the expected format.

    Currently, the request is expected to be a JSON object with the following keys:
    - service: str
    - payload: dict
    - metadata: dict

    Kept generic in case we want to add more fields in the future.
    """
    return IntegrationRequest(**request)


def route_and_run_integration_request(request: dict) -> RunExecutionMetadata:
    """Routes an integration request to the appropriate integration service and runs it."""
    parsed_request: IntegrationRequest = parse_integration_request(request)
    response: RunExecutionMetadata = run_integration_request(request=parsed_request)
    return response
