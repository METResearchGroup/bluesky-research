"""Triggers the preprocess_raw_data pipeline."""

from api.integrations_router.models import IntegrationRequest
from api.integrations_router.run import run_integration_request


def main():
    """Triggers the preprocess_raw_data pipeline."""
    request = {
        "service": "preprocess_raw_data",
        "payload": {},
        "metadata": {},
    }
    request = IntegrationRequest(**request)
    run_integration_request(request)


if __name__ == "__main__":
    main()
