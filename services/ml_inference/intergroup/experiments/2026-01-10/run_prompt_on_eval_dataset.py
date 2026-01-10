"""Runs intergroup prompt on the eval dataset.

See the following for more information:
- Evaluating prompts: https://www.comet.com/docs/opik/evaluation/evaluate_prompt
- Eval metrics: https://www.comet.com/docs/opik/evaluation/metrics/overview
"""
from opik.evaluation import evaluate_prompt, models
from opik.evaluation.metrics import Equals

from lib.opik import OpikClient

from constants import opik_project_name
from prompts import INTERGROUP_PROMPT

gpt4o_mini = models.LiteLLMChatModel(
    model="gpt-4o-mini",
    temperature=0.0,
    max_tokens=512,
)

def main():
    opik_client = OpikClient(project_name=opik_project_name)
    dataset = opik_client.get_or_create_dataset("intergroup_eval_dataset_2026-01-10")
    evaluation_result = evaluate_prompt(
        dataset=dataset,
        messages=[
            {"role": "user", "content": INTERGROUP_PROMPT},
        ],
        model=gpt4o_mini,
        scoring_metrics=[Equals()],
    )

if __name__ == "__main__":
    metric = Equals()
    score = metric.score(output="1", reference="1")
    print(score)

    # main()
