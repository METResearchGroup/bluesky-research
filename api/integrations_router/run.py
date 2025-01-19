"""Logic for running integration requests."""

from typing import Callable

from api.integrations_router.models import IntegrationRequest, IntegrationResponse
from lib.log.logger import get_logger

logger = get_logger(__file__)

MAP_INTEGRATION_REQUEST_TO_SERVICE = {
    "ml_inference_perspective_api": None  # TODO: load the main.py for it.
}


# TODO: check how I do this elsewhere.
def write_execution_metadata_to_db(
    request: IntegrationRequest, execution_metadata: dict
) -> None:
    """Writes execution metadata to DynamoDB."""
    pass


def run_integration_request(request: IntegrationRequest) -> IntegrationResponse:
    """Runs an integration request."""
    try:
        service_fn: Callable = MAP_INTEGRATION_REQUEST_TO_SERVICE[request.service]
        execution_metadata = service_fn(**request.payload)
    except Exception as e:
        logger.error(f"Error running integration request: {e}")
        execution_metadata = {}
    write_execution_metadata_to_db(
        request=request, execution_metadata=execution_metadata
    )
    job_status = execution_metadata.get("job_status", "failed")
    return IntegrationResponse(status=job_status)
