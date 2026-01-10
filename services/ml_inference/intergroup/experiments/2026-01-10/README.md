# Experiments, 2026-01-10

In this set of experiments, we take our eval dataset, we upload to opik, and then we iterate on a few prompts to be able to refine and perfect the prompt that we wanna use for our intergroup classifier.

Steps:

1. Run `create_opik_dataset.py` to upload the .csv file to Opik.
2. Run `run_prompt_on_eval_dataset.py`, which runs the prompt on the Opik dataset.
3. Go to the Opik dashboard and review results.
