"""Runs intergroup prompt on the eval dataset.

See the following for more information:
- Evaluating prompts: https://www.comet.com/docs/opik/evaluation/evaluate_prompt
- Eval metrics: https://www.comet.com/docs/opik/evaluation/metrics/overview
"""
import os

from opik.evaluation import evaluate_prompt
from opik.evaluation.metrics import Equals

from lib.load_env_vars import EnvVarsContainer
from lib.opik import OpikClient

from constants import opik_project_name

# gpt-5-nano is much cheaper than gpt-4o-mini while also outperforming it (at least
# in benchmarks; probably true also in this task as well).
# https://platform.openai.com/docs/pricing
# https://llm-stats.com/models/compare/gpt-4o-mini-2024-07-18-vs-gpt-5-nano-2025-08-07

def main():
    env_vars = EnvVarsContainer()
    env_vars._initialize_env_vars()
    openai_api_key = env_vars.get_env_var("OPENAI_API_KEY", required=True)
    os.environ["OPENAI_API_KEY"] = openai_api_key # need to set this so LiteLLM client can use it.

    opik_client = OpikClient(project_name=opik_project_name)
    opik_prompt = opik_client.get_prompt("intergroup_prompt")
    dataset = opik_client.get_or_create_dataset("intergroup_eval_dataset_2026-01-10")

    evaluate_prompt(
        dataset=dataset,
        messages=[
            {"role": "user", "content": opik_prompt},
        ],
        model="gpt-5-nano",
        scoring_metrics=[Equals()],
    )

if __name__ == "__main__":
    main()
