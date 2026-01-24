"""A/B compare LLMService vs AsyncOpenAIService on intergroup eval data.

This script compares:
- Sync baseline: `services.ml_inference.intergroup.classifier.IntergroupClassifier`
  (which uses `ml_tooling.llm.llm_service.LLMService` via LiteLLM)
- Async approach: `AsyncIntergroupClassifier` wrapper
  (which uses `AsyncOpenAIService` calling OpenAI directly via async SDK)

Metrics:
- accuracy (vs `gold_label`)
- runtime_seconds

Notes:
- No Opik integration.
- Strict all-or-nothing semantics: if any prompt in a batch fails, the run fails.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from pydantic import BaseModel
from typing import cast

from lib.constants import timestamp_format
from lib.datetime_utils import generate_current_datetime_str
from ml_tooling.llm.prompt_utils import generate_batch_prompts
from services.ml_inference.intergroup.constants import DEFAULT_LLM_MODEL_NAME
from services.ml_inference.intergroup.models import (
    IntergroupLabelModel,
    LabelChoiceModel,
)
from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT
from services.ml_inference.models import PostToLabelModel
from services.ml_inference.intergroup.classifier import IntergroupClassifier

from ml_tooling.llm.experiments.async_openai.async_openai_service import (
    AsyncOpenAIService,
)


class ExperimentConfigModel(BaseModel):
    batch_size: int
    model: str
    max_concurrency: int
    include_synthetic_records: bool
    seed: int
    n_runs: int
    warmup: bool


class ClassifierResultsModel(BaseModel):
    accuracy: float
    runtime_seconds: float


class ComparisonResultsModel(BaseModel):
    timestamp: str
    config: ExperimentConfigModel
    sync_results: ClassifierResultsModel
    async_results: ClassifierResultsModel
    notes: list[str]


class AsyncIntergroupClassifier:
    """Experiment-only wrapper mirroring IntergroupClassifier but async."""

    def __init__(self, llm_service: AsyncOpenAIService, *, max_concurrency: int):
        self._llm_service = llm_service
        self._max_concurrency = max_concurrency

    async def classify_batch(
        self,
        batch: list[PostToLabelModel],
        llm_model_name: str = DEFAULT_LLM_MODEL_NAME,
    ) -> list[IntergroupLabelModel]:
        batch_prompts = self._generate_batch_prompts(batch=batch)
        llm_responses: list[LabelChoiceModel] = (
            await self._llm_service.structured_batch_completion(
                prompts=batch_prompts,
                response_model=LabelChoiceModel,
                model=llm_model_name,
                role="user",
                max_concurrency=self._max_concurrency,
            )
        )
        return self._merge_llm_responses_with_batch(batch=batch, llm_responses=llm_responses)

    def _generate_batch_prompts(self, batch: list[PostToLabelModel]) -> list[str]:
        return generate_batch_prompts(
            batch=batch,
            prompt_template=INTERGROUP_PROMPT,
            template_variable_to_model_field_mapping={"input": "text"},
        )

    def _merge_llm_responses_with_batch(
        self,
        batch: list[PostToLabelModel],
        llm_responses: list[LabelChoiceModel],
    ) -> list[IntergroupLabelModel]:
        if len(llm_responses) != len(batch):
            raise ValueError(
                f"Number of LLM responses ({len(llm_responses)}) does not match "
                f"number of posts ({len(batch)})."
            )

        label_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
        return [
            IntergroupLabelModel(
                uri=post.uri,
                text=post.text,
                preprocessing_timestamp=post.preprocessing_timestamp,
                was_successfully_labeled=True,
                reason=None,
                label=llm_response.label,
                label_timestamp=label_timestamp,
            )
            for post, llm_response in zip(batch, llm_responses)
        ]


def _load_posts_from_experiment_data(*, include_synthetic_records: bool) -> pd.DataFrame:
    """Load eval dataset from 2026-01-10; optionally append synthetic posts.

    We intentionally load by file path (not module imports) because experiment
    directories are named with hyphens (e.g. `2026-01-22`) and are not valid
    Python package/module names.
    """
    intergroup_experiments_dir = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../../../services/ml_inference/intergroup/experiments",
        )
    )

    labeled_posts_fp = os.path.join(
        intergroup_experiments_dir, "2026-01-10", "intergroup_eval_dataset.csv"
    )
    df = pd.read_csv(labeled_posts_fp)
    df = cast(pd.DataFrame, df[["uri", "text", "gold_label"]])  # type: ignore[index]
    df = df.reset_index(drop=True)

    if not include_synthetic_records:
        return df

    synthetic_dir = os.path.join(
        intergroup_experiments_dir, "2026-01-22", "synthetic_data"
    )
    if not os.path.isdir(synthetic_dir):
        return df
    synthetic_files = [
        os.path.join(synthetic_dir, fp)
        for fp in os.listdir(synthetic_dir)
        if fp.endswith(".csv")
    ]
    if not synthetic_files:
        return df

    synthetic_dfs = [pd.read_csv(fp) for fp in synthetic_files]
    synthetic_df = pd.concat(synthetic_dfs).reset_index(drop=True)
    synthetic_df = cast(
        pd.DataFrame, synthetic_df[["uri", "text", "gold_label"]]  # type: ignore[index]
    )

    return cast(pd.DataFrame, pd.concat([df, synthetic_df]).reset_index(drop=True))


def _calculate_accuracy(ground_truth_labels: list[int], labels: list[int]) -> float:
    if not ground_truth_labels:
        raise ValueError("ground_truth_labels is empty")
    correct = 0
    for gt, pred in zip(ground_truth_labels, labels):
        if gt == pred:
            correct += 1
    return correct / len(ground_truth_labels)


def _create_batch(
    posts: pd.DataFrame,
    *,
    batch_size: int,
    seed: int,
) -> tuple[list[PostToLabelModel], list[int]]:
    """Create a deterministic sampled batch for fair A/B comparisons."""
    sampled = posts.sample(n=batch_size, replace=True, random_state=seed)
    sampled = sampled.reset_index(drop=True)

    posts_list: list[PostToLabelModel] = []
    ground_truth_labels: list[int] = []
    for post in sampled.to_dict(orient="records"):
        posts_list.append(
            PostToLabelModel(
                uri=post["uri"],
                text=post["text"],
                preprocessing_timestamp="test-timestamp",
                batch_id=1,
                batch_metadata="{}",
            )
        )
        ground_truth_labels.append(int(post["gold_label"]))
    return posts_list, ground_truth_labels


def _run_sync_classifier(
    *,
    batch: list[PostToLabelModel],
    ground_truth_labels: list[int],
    model: str,
    warmup: bool,
) -> ClassifierResultsModel:
    clf = IntergroupClassifier()

    if warmup and batch:
        _ = clf.classify_batch(batch=[batch[0]], llm_model_name=model)

    start = time.perf_counter()
    labels = clf.classify_batch(batch=batch, llm_model_name=model)
    runtime = time.perf_counter() - start

    accuracy = _calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[l.label for l in labels],
    )
    return ClassifierResultsModel(accuracy=accuracy, runtime_seconds=runtime)


async def _run_async_classifier(
    *,
    batch: list[PostToLabelModel],
    ground_truth_labels: list[int],
    model: str,
    max_concurrency: int,
    warmup: bool,
) -> ClassifierResultsModel:
    llm = AsyncOpenAIService()
    clf = AsyncIntergroupClassifier(llm, max_concurrency=max_concurrency)

    if warmup and batch:
        _ = await clf.classify_batch(batch=[batch[0]], llm_model_name=model)

    start = time.perf_counter()
    labels = await clf.classify_batch(batch=batch, llm_model_name=model)
    runtime = time.perf_counter() - start

    accuracy = _calculate_accuracy(
        ground_truth_labels=ground_truth_labels,
        labels=[l.label for l in labels],
    )
    return ClassifierResultsModel(accuracy=accuracy, runtime_seconds=runtime)


def _export_results(results: ComparisonResultsModel) -> str:
    export_dir = os.path.join(
        os.path.dirname(__file__),
        "compare_approaches_results",
        generate_current_datetime_str(),
    )
    os.makedirs(export_dir, exist_ok=True)
    path = os.path.join(export_dir, "results.json")
    with open(path, "w") as f:
        json.dump(results.model_dump(), f, indent=2)
    return path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare LLMService vs AsyncOpenAIService on intergroup eval dataset"
    )
    parser.add_argument("--batch_size", type=int, default=200)
    parser.add_argument("--model", type=str, default=DEFAULT_LLM_MODEL_NAME)
    parser.add_argument("--max_concurrency", type=int, default=50)
    parser.add_argument("--include_synthetic_records", action="store_true")
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--n_runs", type=int, default=1)
    parser.add_argument("--warmup", action="store_true")
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()

    if args.batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {args.batch_size}")
    if args.max_concurrency <= 0:
        raise ValueError(f"max_concurrency must be > 0, got {args.max_concurrency}")
    if args.n_runs <= 0:
        raise ValueError(f"n_runs must be > 0, got {args.n_runs}")

    posts_df = _load_posts_from_experiment_data(
        include_synthetic_records=args.include_synthetic_records
    )

    sync_runtimes: list[float] = []
    sync_accuracies: list[float] = []
    async_runtimes: list[float] = []
    async_accuracies: list[float] = []

    # Deterministic seeds per run to avoid accidental coupling to global RNG state.
    for run_idx in range(args.n_runs):
        seed = args.seed + run_idx
        batch, ground_truth = _create_batch(posts_df, batch_size=args.batch_size, seed=seed)

        sync = _run_sync_classifier(
            batch=batch,
            ground_truth_labels=ground_truth,
            model=args.model,
            warmup=args.warmup,
        )
        async_res = await _run_async_classifier(
            batch=batch,
            ground_truth_labels=ground_truth,
            model=args.model,
            max_concurrency=args.max_concurrency,
            warmup=args.warmup,
        )

        sync_runtimes.append(sync.runtime_seconds)
        sync_accuracies.append(sync.accuracy)
        async_runtimes.append(async_res.runtime_seconds)
        async_accuracies.append(async_res.accuracy)

    # Average across runs (keep it simple and explicit).
    sync_results = ClassifierResultsModel(
        accuracy=sum(sync_accuracies) / len(sync_accuracies),
        runtime_seconds=sum(sync_runtimes) / len(sync_runtimes),
    )
    async_results = ClassifierResultsModel(
        accuracy=sum(async_accuracies) / len(async_accuracies),
        runtime_seconds=sum(async_runtimes) / len(async_runtimes),
    )

    config = ExperimentConfigModel(
        batch_size=args.batch_size,
        model=args.model,
        max_concurrency=args.max_concurrency,
        include_synthetic_records=bool(args.include_synthetic_records),
        seed=args.seed,
        n_runs=args.n_runs,
        warmup=bool(args.warmup),
    )

    results = ComparisonResultsModel(
        timestamp=datetime.now(timezone.utc).strftime(timestamp_format),
        config=config,
        sync_results=sync_results,
        async_results=async_results,
        notes=[
            "Sync path uses IntergroupClassifier -> LLMService.structured_batch_completion (LiteLLM batch_completion).",
            "Async path uses AsyncIntergroupClassifier -> AsyncOpenAIService.structured_batch_completion (N concurrent per-prompt OpenAI SDK calls, semaphore-bounded).",
            "Async path cancels pending tasks on first observed failure (all-or-nothing with early cancellation).",
            "Both paths use the same prompt template (INTERGROUP_PROMPT) and same response schema (LabelChoiceModel).",
            "Both paths use ModelConfigRegistry defaults unless overridden via CLI args.",
        ],
    )

    export_path = _export_results(results)

    print("=== A/B comparison ===")
    print(json.dumps(results.model_dump(), indent=2))
    print(f"Exported results to: {export_path}")


if __name__ == "__main__":
    asyncio.run(main())

