# Intergroup Classifier Experiments

Experimental folder for developing and iterating on an LLM-based intergroup classifier using Opik.

## Overview

We previously classified the presence of intergroup discussion (interactions or situations that involve two or more groups) using our IME classifier. However, we're interested in seeing if an LLM-based classifier would perform better than a fine-tuned BERT model.

## Scripts

- `create_opik_dataset.py`: Loads CSV dataset and uploads to Opik.
- `register_prompt.py`: Registers prompt versions with Opik for traceability.
- `run_prompt_on_eval_dataset.py`: Evaluates current prompt on the eval dataset; logs traces and results to Opik.
- `prompts.py`: Prompt text definitions.

## Quick Start

1. Prepare dataset in CSV format with columns: [describe structure]
2. Upload to Opik: `python create_opik_dataset.py`
3. Register prompt: `python register_prompt.py`
4. Run evaluation: `python run_prompt_on_eval_dataset.py`

## Iteration Workflow

Update `prompts.py` → run `register_prompt.py` → rerun evaluation.

## Results

Current best equals_metric: ~74.2% (after prompt refinement). See Opik dashboard for full trace history.