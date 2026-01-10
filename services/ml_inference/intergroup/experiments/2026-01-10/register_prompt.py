"""Registers our latest prompt with Opik.

Opik automatically manages prompt versioning.
"""
from lib.opik import OpikClient

from constants import opik_project_name
from prompts import INTERGROUP_PROMPT

def main():
    opik_client = OpikClient(project_name=opik_project_name)
    opik_client.register_prompt(prompt_name="intergroup_prompt", prompt_text=INTERGROUP_PROMPT)

if __name__ == "__main__":
    main()
