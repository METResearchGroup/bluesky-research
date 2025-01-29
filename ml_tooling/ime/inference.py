"""Inference for the IME classification model."""

import gc

import numpy as np
import pandas as pd
import torch

from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from ml_tooling.ime.classes import TextDataset
from ml_tooling.ime.constants import default_model
from ml_tooling.ime.helper import load_model_and_tokenizer, get_device

logger = get_logger(__name__)

device = get_device()
model, tokenizer = load_model_and_tokenizer(model_name=default_model, device=device)


def default_to_other(preds):
    """
    defualt labeling 0,0,0,0 output to 0,0,0,1.
    (the last class is "other")
    Inputs:
        preds: numpy array of shape (n_samples, 4)

    Creates a boolean mask that is True for rows where the sum across all
    columns is zero. For those rows, it sets the value of the last column to 1.
    """
    new_preds = preds.copy()
    new_preds[(new_preds.sum(axis=1) == 0), 3] = 1
    return new_preds


def process_ime_minibatch(
    minibatch_df: pd.DataFrame, minibatch_size: int
) -> pd.DataFrame:
    """Processes a minibatch of posts using the IME classification model"""
    dataset = TextDataset(
        tokenizer=tokenizer,
        df=minibatch_df,
        mode="test",
        batch_size=minibatch_size,
    )

    all_probabilities: list[float] = []
    all_preds: list[int] = []

    model.eval()

    with torch.no_grad():
        for i, batch in enumerate(dataset):
            # logger.info(f"Processing minibatch {i}/{total_minibatches}...")
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)

            # probs are of length nx4 (4 classes)
            probs = outputs.cpu().numpy()
            preds = (probs > 0.5).astype(int)
            preds = default_to_other(preds)

            all_probabilities.extend(probs)
            all_preds.extend(preds)

    probabilities = np.array(all_probabilities)
    preds = np.array(all_preds)

    probs_emotion = probabilities[:, 0]
    probs_intergroup = probabilities[:, 1]
    probs_moral = probabilities[:, 2]
    probs_other = probabilities[:, 3]

    labels_emotion = preds[:, 0]
    labels_intergroup = preds[:, 1]
    labels_moral = preds[:, 2]
    labels_other = preds[:, 3]

    uris = minibatch_df["uri"]
    texts = minibatch_df["text"]

    output_df = pd.DataFrame(
        {
            "uri": uris,
            "text": texts,
            "prob_emotion": probs_emotion,
            "prob_intergroup": probs_intergroup,
            "prob_moral": probs_moral,
            "prob_other": probs_other,
            "label_emotion": labels_emotion,
            "label_intergroup": labels_intergroup,
            "label_moral": labels_moral,
            "label_other": labels_other,
            "label_timestamp": current_datetime_str,
        }
    )

    return output_df


def process_ime_batch(
    batch: list[dict],
    minibatch_size: int,
) -> list[dict]:
    """Process a batch of posts using the IME classification model.

    Returns a list of dicts of class labels.
    """
    minibatches: list[list[dict]] = [
        batch[i : i + minibatch_size] for i in range(0, len(batch), minibatch_size)
    ]

    batch_output_dfs: list[pd.DataFrame] = []

    for minibatch in minibatches:
        minibatch_df = pd.DataFrame(minibatch)
        output_df: pd.DataFrame = process_ime_minibatch(
            minibatch_df=minibatch_df, minibatch_size=minibatch_size
        )
        batch_output_dfs.append(output_df)

    joined_output_df = pd.concat(batch_output_dfs)
    output_dicts: list[dict] = joined_output_df.to_dict(orient="records")

    del joined_output_df
    gc.collect()

    return output_dicts
