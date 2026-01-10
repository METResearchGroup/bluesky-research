"""Opik client.

Stub implementation for now until we actually figure out how to use Opik."""

import os
from typing import Any

import opik

from lib.log.logger import get_logger

logger = get_logger(__name__)


class OpikClient:
    """Wrapper around the Opik client.

    This decouples implementation details from Opik itself, allowing clients
    to use Opik without having to import it directly.
    """

    def __init__(self, project_name: str):
        os.environ["OPIK_PROJECT_NAME"] = project_name
        self.client = opik.Opik(project_name=project_name)
        logger.info(f"Opik client initialized for project {project_name}")

    def get_or_create_dataset(self, name: str) -> opik.Dataset:
        return self.client.get_or_create_dataset(name)

    def register_prompt(
        self,
        prompt_name: str,
        prompt_text: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Registers a prompt with Opik.

        Opik automatically deduplicates prompts, so this operation
        is idempotent.

        See https://www.comet.com/docs/opik/prompt_engineering/prompt_management
        for more information
        """
        self.client.create_prompt(
            name=prompt_name,
            prompt=prompt_text,
            metadata=metadata or {},
        )
        logger.info(
            f"[Project {self.client.project_name}]: Prompt '{prompt_name}' registered with Opik."
        )

    def get_prompt(self, prompt_name: str) -> opik.Prompt:
        prompt = self.client.get_prompt(prompt_name)
        if prompt is None:
            logger.error(
                f"[Project {self.client.project_name}]: Prompt '{prompt_name}' not found in Opik."
            )
            raise ValueError(f"Prompt '{prompt_name}' not found in Opik.")
        return prompt
