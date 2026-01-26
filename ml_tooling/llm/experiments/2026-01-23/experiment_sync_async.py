"""Compare sync vs async LLM inference for intergroup classification.

This script:
- Loads the intergroup eval dataset CSV used in the 2026-01-10 experiments
- Generates prompts using the production intergroup prompt template
- Runs inference with:
  - `ml_tooling.llm.llm_service.LLMService` (sync)
  - `ml_tooling.llm.async_llm_service.AsyncLLMService` (async)
- Computes accuracy vs `gold_label` and measures runtime for both

Example:
  python ml_tooling/llm/experiments/2026-01-23/experiment_sync_async.py --max-samples 200
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, TypeVar

import pandas as pd

from lib.load_env_vars import EnvVarsContainer
from ml_tooling.llm.async_llm_service import AsyncLLMService
from ml_tooling.llm.llm_service import LLMService
from ml_tooling.llm.prompt_utils import generate_batch_prompts
from services.ml_inference.intergroup.constants import DEFAULT_BATCH_SIZE, DEFAULT_LLM_MODEL_NAME
from services.ml_inference.intergroup.models import LabelChoiceModel
from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT
from services.ml_inference.models import PostToLabelModel


@dataclass(frozen=True)
class ExperimentResult:
    name: str
    model: str
    total: int
    correct: int
    failed: int
    duration_s: float

    @property
    def accuracy(self) -> float:
        return 0.0 if self.total == 0 else self.correct / self.total

    @property
    def throughput_rps(self) -> float:
        return 0.0 if self.duration_s <= 0 else self.total / self.duration_s


def _default_dataset_path() -> Path:
    # experiment_sync_async.py -> 2026-01-23 -> experiments -> llm -> ml_tooling -> repo_root
    repo_root = Path(__file__).resolve().parents[4]
    return (
        repo_root
        / "services"
        / "ml_inference"
        / "intergroup"
        / "experiments"
        / "2026-01-10"
        / "intergroup_eval_dataset.csv"
    )


TItem = TypeVar("TItem")


def _iter_batches(items: list[TItem], batch_size: int) -> Iterable[list[TItem]]:
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def _load_eval_dataset(csv_path: Path, max_samples: int | None = None) -> tuple[list[str], list[int]]:
    df = pd.read_csv(csv_path)

    if "text" not in df.columns:
        raise ValueError(f"Expected CSV to contain column 'text'. Found: {list(df.columns)}")
    if "gold_label" not in df.columns:
        raise ValueError(
            f"Expected CSV to contain column 'gold_label'. Found: {list(df.columns)}"
        )

    if max_samples is not None:
        df = df.head(max_samples)

    texts = [str(x) for x in df["text"].tolist()]
    gold = [int(x) for x in df["gold_label"].tolist()]
    return texts, gold


def _make_posts(texts: list[str]) -> list[PostToLabelModel]:
    # This dataset is for evaluation only; it doesn't carry the full queue metadata.
    # Populate required fields with deterministic placeholders.
    posts: list[PostToLabelModel] = []
    for i, text in enumerate(texts):
        posts.append(
            PostToLabelModel(
                uri=f"eval://intergroup/{i}",
                text=text,
                preprocessing_timestamp="eval",
                batch_id=i,
                batch_metadata="{}",
            )
        )
    return posts


def _make_prompts(posts: list[PostToLabelModel]) -> list[str]:
    return generate_batch_prompts(
        batch=posts,
        prompt_template=INTERGROUP_PROMPT,
        template_variable_to_model_field_mapping={"input": "text"},
    )


def _score_predictions(pred: list[int], gold: list[int]) -> tuple[int, int]:
    if len(pred) != len(gold):
        raise ValueError(f"Length mismatch: pred={len(pred)} gold={len(gold)}")
    correct = sum(int(p == g) for p, g in zip(pred, gold))
    failed = sum(int(p not in (0, 1)) for p in pred)
    return correct, failed


def run_sync(
    *,
    prompts: list[str],
    gold: list[int],
    model: str,
    batch_size: int,
) -> ExperimentResult:
    service = LLMService()
    predictions: list[int] = []

    start = time.perf_counter()
    for prompt_batch in _iter_batches(prompts, batch_size=batch_size):
        responses: list[LabelChoiceModel] = service.structured_batch_completion(
            prompts=prompt_batch,
            response_model=LabelChoiceModel,
            model=model,
            role="user",
        )
        predictions.extend([r.label for r in responses])
    duration_s = time.perf_counter() - start

    correct, failed = _score_predictions(predictions, gold)
    return ExperimentResult(
        name="sync",
        model=model,
        total=len(gold),
        correct=correct,
        failed=failed,
        duration_s=duration_s,
    )


async def run_async(
    *,
    prompts: list[str],
    gold: list[int],
    model: str,
    batch_size: int,
    max_concurrent: int,
) -> ExperimentResult:
    service = AsyncLLMService()
    predictions: list[int] = []

    start = time.perf_counter()
    for prompt_batch in _iter_batches(prompts, batch_size=batch_size):
        responses: list[LabelChoiceModel] = await service.structured_batch_completion_async(
            prompts=prompt_batch,
            response_model=LabelChoiceModel,
            model=model,
            role="user",
            max_concurrent=max_concurrent,
        )
        predictions.extend([r.label for r in responses])
    duration_s = time.perf_counter() - start

    correct, failed = _score_predictions(predictions, gold)
    return ExperimentResult(
        name="async",
        model=model,
        total=len(gold),
        correct=correct,
        failed=failed,
        duration_s=duration_s,
    )


def _print_result(r: ExperimentResult) -> None:
    print("")
    print(f"== {r.name} ==")
    print(f"model: {r.model}")
    print(f"total: {r.total}")
    print(f"correct: {r.correct}")
    print(f"failed_parse_or_other: {r.failed}")
    print(f"accuracy: {r.accuracy:.4f}")
    print(f"runtime_s: {r.duration_s:.2f}")
    print(f"throughput_rps: {r.throughput_rps:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare sync vs async LLM inference on intergroup eval dataset.")
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=_default_dataset_path(),
        help="Path to intergroup_eval_dataset.csv",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=None,
        help="If set, evaluate only the first N rows.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_LLM_MODEL_NAME,
        help="LLM model name to use.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of prompts per batch.",
    )
    parser.add_argument(
        "--async-max-concurrent",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Max concurrent requests inside each async batch.",
    )
    args = parser.parse_args()

    # Ensure env vars are loaded and OpenAI key is available for LiteLLM.
    openai_api_key = EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=True)
    os.environ["OPENAI_API_KEY"] = openai_api_key

    csv_path: Path = args.csv_path
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found at: {csv_path}")

    texts, gold = _load_eval_dataset(csv_path, max_samples=args.max_samples)
    posts = _make_posts(texts)
    prompts = _make_prompts(posts)

    if len(prompts) != len(gold):
        raise ValueError(f"Internal error: prompts={len(prompts)} gold={len(gold)}")

    sync_result = run_sync(
        prompts=prompts,
        gold=gold,
        model=args.model,
        batch_size=args.batch_size,
    )
    async_result = asyncio.run(
        run_async(
            prompts=prompts,
            gold=gold,
            model=args.model,
            batch_size=args.batch_size,
            max_concurrent=args.async_max_concurrent,
        )
    )

    print(f"dataset: {csv_path}")
    print(f"prompt_template: services/ml_inference/intergroup/prompts.py::INTERGROUP_PROMPT")
    _print_result(sync_result)
    _print_result(async_result)


if __name__ == "__main__":
    main()
