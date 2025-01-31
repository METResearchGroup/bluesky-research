"""Map integration requests to services."""

from pipelines.classify_records.perspective_api.handler import (
    lambda_handler as ml_inference_perspective_api_handler,
)
from pipelines.classify_records.sociopolitical.handler import (
    lambda_handler as ml_inference_sociopolitical_handler,
)
from pipelines.classify_records.ime.handler import (
    lambda_handler as ml_inference_ime_handler,
)

MAP_INTEGRATION_REQUEST_TO_SERVICE = {
    "ml_inference_perspective_api": ml_inference_perspective_api_handler,
    "ml_inference_sociopolitical": ml_inference_sociopolitical_handler,
    "ml_inference_ime": ml_inference_ime_handler,
}
