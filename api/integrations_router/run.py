"""Logic for running integration requests."""

from typing import Callable, Optional

from api.integrations_router.map import MAP_INTEGRATION_REQUEST_TO_SERVICE
from api.integrations_router.models import (
    IntegrationRequest,
    IntegrationResponse,
    RunExecutionMetadata,
)
from lib.aws.dynamodb import DynamoDB
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.ml_inference.helper import dynamodb_table_name

logger = get_logger(__file__)

dynamodb = DynamoDB()


@track_performance
def run_integration_service(
    request: IntegrationRequest,
    previous_run_metadata: Optional[dict] = None,
) -> RunExecutionMetadata:
    """Runs an integration service."""
    service = request.service
    payload = request.payload
    if not previous_run_metadata:
        previous_run_metadata = {}
    service_fn: Callable = MAP_INTEGRATION_REQUEST_TO_SERVICE[service]
    execution_metadata = service_fn(**payload)
    transformed_execution_metadata = RunExecutionMetadata(**execution_metadata)
    return transformed_execution_metadata


def load_latest_run_metadata(service: str) -> dict:
    if service in [
        "ml_inference_perspective_api",
        "ml_inference_sociopolitical",
        "ml_inference_ime",
    ]:
        inference_type = (
            "perspective_api"
            if service == "ml_inference_perspective_api"
            else "llm"
            if service == "ml_inference_sociopolitical"
            else "ime"
        )
        filtered_items: list[dict] = dynamodb.query_items_by_inference_type(
            table_name=dynamodb_table_name,
            inference_type=inference_type,
        )
    else:
        filtered_items = []

    if not filtered_items:
        logger.info(
            f"No previous run metadata found for {service}. Returning empty dict."
        )
        return {}

    # Sort filtered items by inference_timestamp in descending order
    sorted_items = sorted(
        filtered_items,
        key=lambda x: x.get("timestamp", {}).get("S", ""),
        reverse=True,
    )  # noqa

    logger.info(f"Latest run metadata: {sorted_items[0]}")

    # Return the most recent item
    return sorted_items[0]


def write_run_metadata_to_db(execution_metadata: RunExecutionMetadata) -> None:
    """Writes execution metadata to DynamoDB."""
    try:
        dynamodb.insert_item_into_table(
            item=execution_metadata.dict(),
            table_name=execution_metadata.metadata_table_name,
        )
        logger.info(
            f"Successfully inserted execution metadata: {execution_metadata.dict()}"
        )
    except Exception as e:
        logger.error(f"Error writing execution metadata to DynamoDB: {e}")
        raise


@track_performance
def run_integration_request(request: IntegrationRequest) -> IntegrationResponse:
    """Runs an integration request."""
    previous_run_metadata: dict = load_latest_run_metadata(request.service)
    run_metadata: RunExecutionMetadata = run_integration_service(
        request=request,
        previous_run_metadata=previous_run_metadata,
    )
    write_run_metadata_to_db(run_metadata=run_metadata)
    return IntegrationResponse(
        service=run_metadata.service,
        timestamp=run_metadata.timestamp,
        status_code=run_metadata.status_code,
        body=run_metadata.body,
    )
