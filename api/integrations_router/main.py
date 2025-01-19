"""Interface for routing integration requests to the appropriate integration service."""

from api.integrations_router.models import IntegrationRequest, IntegrationResponse


def parse_integration_request(request: dict) -> IntegrationRequest:
    pass


def _route_integration_request(request: IntegrationRequest) -> IntegrationResponse:
    pass


def route_integration_request(request: dict) -> IntegrationResponse:
    parsed_request: IntegrationRequest = parse_integration_request(request)
    return _route_integration_request(parsed_request)
