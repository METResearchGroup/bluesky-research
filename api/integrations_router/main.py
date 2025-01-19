"""Interface for routing integration requests to the appropriate integration service."""

from api.integrations_router.models import IntegrationRequest, IntegrationResponse
from api.integrations_router.run import run_integration_request


def parse_integration_request(request: dict) -> IntegrationRequest:
    pass


def route_and_run_integration_request(request: dict) -> IntegrationResponse:
    parsed_request: IntegrationRequest = parse_integration_request(request)
    response: IntegrationResponse = run_integration_request(request=parsed_request)
    return response
