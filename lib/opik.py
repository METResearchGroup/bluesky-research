"""Opik client.

Stub implementation for now until we actually figure out how to use Opik."""

import os

import opik


class OpikClient:
    """Wrapper around the Opik client.

    This decouples implementation details from Opik itself, allowing clients
    to use Opik without having to import it directly.
    """

    def __init__(self, project_name: str):
        os.environ["OPIK_PROJECT_NAME"] = project_name
        self.client = opik.Opik(project_name=project_name)
        print(f"Opik client initialized for project {project_name}")

    def get_or_create_dataset(self, name: str) -> opik.Dataset:
        return self.client.get_or_create_dataset(name)
