from lib.log.logger import get_logger
from services.backfill.models import IntegrationRunnerConfigurationPayload

logger = get_logger(__name__)


class IntegrationRunnerService:
    def run_integrations(self, payloads: list[IntegrationRunnerConfigurationPayload]):
        logger.info(f"Running integrations: {payloads}")
        for i, integration_config_payload in enumerate(payloads):
            logger.info(
                f"Running integration {i+1} of {len(payloads)}: {integration_config_payload.integration_name}"
            )
            try:
                self._run_single_integration(integration_config_payload)
            # TODO: add custom exception handling here.
            except Exception as e:
                logger.error(
                    f"Error running integration {i+1} of {len(payloads)}: {integration_config_payload.integration_name} - {e}"
                )
                raise e
        logger.info("Integrations completed.")

    def _run_single_integration(self, payload: IntegrationRunnerConfigurationPayload):
        pass
