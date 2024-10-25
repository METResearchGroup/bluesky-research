"""Inference for the IME classification model."""

import time

import pandas as pd
import numpy as np
import torch
from torch.utils.data import DataLoader
import wandb
from lib.log.logger import get_logger
from lib.constants import current_datetime_str
from ml_tooling.ime_classification.model import (
    get_device,
    load_model_and_tokenizer,
    TextDataset,
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

default_batch_size = 32


default_model = "distilbert"
model, tokenizer = load_model_and_tokenizer(default_model)
device = get_device()

logger = get_logger(__name__)

wandb_project_name = "IME classification inference"


def batch_classify_posts(
    posts: list[FilteredPreprocessedPostModel], batch_size: int = default_batch_size
) -> tuple[np.array, float]:
    """Batch classifies a list of posts."""
    df = pd.DataFrame([post.dict() for post in posts])
    dataset = TextDataset(tokenizer, df, mode="test")
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=False)
    total_batches = len(dataloader)

    all_probabilities: list[float] = []

    model.eval()

    start_time = time.time()
    with torch.no_grad():
        for i, batch in enumerate(dataloader):
            logger.info(f"Processing batch {i}/{total_batches}...")
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            probs = outputs.cpu().numpy()
            all_probabilities.extend(probs)
    end_time = time.time()
    total_seconds = end_time - start_time
    logger.info(f"Batch run finished in {total_seconds} seconds.")
    return all_probabilities, total_seconds


def run_batch_classification(posts: list[FilteredPreprocessedPostModel]):
    """Run batch classification and log results with Wandb."""

    probabilities, batch_times = batch_classify_posts(posts)
    hyperparams = {
        "model_name": default_model,
        "batch_size": default_batch_size,
        "timestamp": current_datetime_str,
    }
    wandb.init(project=wandb_project_name, config=hyperparams)
    wandb.watch(model, log="all")
    wandb.log(
        {
            "total_inference_time": sum(batch_times),
            "average_batch_time": np.mean(batch_times),
            "total_posts": len(posts),
            "probability_distribution": wandb.Histogram(probabilities),
        }
    )

    wandb.finish()

    return probabilities


if __name__ == "__main__":
    texts = [
        {"text": "I can't believe this is happening! #outrageous"},
        {"text": "This is absolutely unacceptable!"},
        {"text": "How can they allow this to happen?"},
        {"text": "I'm so angry right now!"},
        {"text": "This is a complete disaster! #furious"},
    ]
    probabilities = batch_classify_posts(texts)
    breakpoint()
