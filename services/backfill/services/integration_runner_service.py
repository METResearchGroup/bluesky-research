from lib.log.logger import get_logger
from services.backfill.models import (
    IntegrationRunnerConfigurationPayload,
    IntegrationRunnerServicePayload,
)
from services.backfill.exceptions import IntegrationRunnerServiceError

logger = get_logger(__name__)


class IntegrationRunnerService:
    def run_integrations(self, payload: IntegrationRunnerServicePayload):
        integration_configs = payload.integration_configs
        integration_names = [config.integration_name for config in integration_configs]
        total_integrations = len(integration_names)

        logger.info(f"Running integrations: {','.join(integration_names)}")
        for i, integration_config_payload in enumerate(integration_configs):
            logger.info(
                f"Running integration {i+1} of {total_integrations}: {integration_config_payload.integration_name}"
            )
            try:
                self._run_single_integration(integration_config_payload)
            except Exception as e:
                logger.error(
                    f"Error running integration {i+1} of {total_integrations}: {integration_config_payload.integration_name} - {e}"
                )
                raise IntegrationRunnerServiceError(
                    f"Error running integration {i+1} of {total_integrations}: {integration_config_payload.integration_name} - {e}"
                )
        logger.info(
            f"Integrations completed successfully: {','.join(integration_names)}"
        )

    def _run_single_integration(self, payload: IntegrationRunnerConfigurationPayload):
        pass
