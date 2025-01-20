"""Map integration requests to services."""

from pipelines.classify_records.perspective_api.handler import (
    lambda_handler as ml_inference_perspective_api_handler,
)

MAP_INTEGRATION_REQUEST_TO_SERVICE = {
    "ml_inference_perspective_api": ml_inference_perspective_api_handler,
}
