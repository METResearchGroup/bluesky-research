from typing import Callable
from lib.log.logger import get_logger
from services.backfill.models import (
    IntegrationRunnerConfigurationPayload,
    IntegrationRunnerServicePayload,
)
from services.backfill.exceptions import IntegrationRunnerServiceError

logger = get_logger(__file__)


class IntegrationRunnerService:
    def __init__(self):
        self.integration_strategies = {}

    def _load_integration_strategy(self, integration_name: str) -> Callable:
        """Loads the integration strategy for the given integration name.

        We directly return the classification functions because for the current
        services, they have identical signatures for the classification functions.
        If this changes, we would have a protocol for the classification functions
        and consider using specific *Strategy classes. Right now, creating the Strategy
        classes would be overkill.
        """
        if integration_name == "ml_inference_perspective_api":
            from services.ml_inference.perspective_api.perspective_api import (
                classify_latest_posts,
            )

            return classify_latest_posts
        elif integration_name == "ml_inference_sociopolitical":
            from services.ml_inference.sociopolitical.sociopolitical import (
                classify_latest_posts,
            )

            return classify_latest_posts
        elif integration_name == "ml_inference_ime":
            from services.ml_inference.ime.ime import classify_latest_posts

            return classify_latest_posts
        elif integration_name == "ml_inference_valence_classifier":
            from services.ml_inference.valence_classifier.valence_classifier import (
                classify_latest_posts,
            )

            return classify_latest_posts
        elif integration_name == "ml_inference_intergroup":
            from services.ml_inference.intergroup.intergroup import (
                classify_latest_posts,
            )

            return classify_latest_posts
        else:
            raise IntegrationRunnerServiceError(
                f"Invalid integration name: {integration_name}"
            )

    def _get_or_load_integration_entrypoint(self, integration_name: str) -> Callable:
        """Gets the integration entrypoint for the given integration name.
        If the entrypoint is not found, it loads it from the integration strategies dictionary.
        """
        if integration_name not in self.integration_strategies:
            self.integration_strategies[integration_name] = (
                self._load_integration_strategy(integration_name)
            )
        return self.integration_strategies[integration_name]

    def run_integrations(self, payload: IntegrationRunnerServicePayload):
        """Runs integrations for a list of integration configurations."""
        integration_configs = payload.integration_configs
        integration_names = [config.integration_name for config in integration_configs]
        total_integrations = len(integration_names)

        logger.info(f"Running integrations: {','.join(integration_names)}")

        try:
            for i, integration_config_payload in enumerate(integration_configs):
                logger.info(
                    f"Running integration {i+1} of {total_integrations}: {integration_config_payload.integration_name}"
                )
                self._run_single_integration(integration_config_payload)
                logger.info(
                    f"Integration {i+1} of {total_integrations}: {integration_config_payload.integration_name} completed successfully"
                )
            logger.info(
                f"Integrations completed successfully: {','.join(integration_names)}"
            )

        except Exception as e:
            logger.error(f"Error running integrations: {e}")
            raise IntegrationRunnerServiceError(
                f"Error running integrations: {e}"
            ) from e

    def _run_single_integration(self, payload: IntegrationRunnerConfigurationPayload):
        """Runs a single integration."""
        try:
            dispatch_payload = self._create_integration_dispatch_payload(payload)
            integration_fn = self._get_or_load_integration_entrypoint(
                payload.integration_name
            )
            integration_fn(**dispatch_payload)
        except Exception as e:
            logger.error(f"Error running integration: {e}")
            raise IntegrationRunnerServiceError(
                f"Error running integration: {e}"
            ) from e

    def _create_integration_dispatch_payload(
        self, payload: IntegrationRunnerConfigurationPayload
    ) -> dict:
        """Creates a dispatch payload matching the signature of the classification
        functions."""
        return {
            "backfill_period": payload.backfill_period.value,
            "backfill_duration": payload.backfill_duration,
            "max_records_per_run": payload.max_records_per_run,
            "run_classification": True,  # TODO: need to deprecate run_classification. Here for backwards compatibility.
            "previous_run_metadata": None,  # TODO: need to deprecate previous_run_metadata. Here for backwards compatibility.
            "event": None,  # TODO: need to deprecate event. Here for backwards compatibility.
        }
