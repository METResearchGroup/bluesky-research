"""Inference for the IME classification model."""

import gc

import numpy as np
import pandas as pd
import torch

from lib.constants import current_datetime_str
from lib.helper import RUN_MODE
from lib.log.logger import get_logger
from ml_tooling.ime.classes import MultiLabelClassifier
from ml_tooling.ime.constants import default_num_classes, model_to_asset_paths_map
from ml_tooling.ime.helper import get_device
from transformers import AutoTokenizer

from ml_tooling.ime.classes import TextDataset

logger = get_logger(__name__)

device = get_device()


def load_model_and_tokenizer(
    model_name: str, device: torch.device
) -> tuple[MultiLabelClassifier, AutoTokenizer]:
    """Load the model and tokenizer for the given model name."""
    if RUN_MODE == "test":
        # Return dummy versions for testing
        model = MultiLabelClassifier(n_classes=default_num_classes, model=model_name)
        model.eval()

        # Create a minimal tokenizer class for testing
        class DummyTokenizer:
            def __call__(self, texts, **kwargs):
                batch_size = len(texts) if isinstance(texts, list) else 1
                return {
                    "input_ids": torch.zeros((batch_size, 512), dtype=torch.long),
                    "attention_mask": torch.ones((batch_size, 512), dtype=torch.long),
                }

        return model, DummyTokenizer()

    # Normal production behavior
    model = MultiLabelClassifier(n_classes=default_num_classes, model=model_name)

    # Enable multi-GPU if available
    if torch.cuda.device_count() > 1:
        logger.info(f"Using {torch.cuda.device_count()} GPUs!")
        model = torch.nn.DataParallel(model)

    model.to(device)
    model.eval()

    tokenizer_path = model_to_asset_paths_map[model_name]["tokenizer"]
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

    return model, tokenizer


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
    minibatch_df: pd.DataFrame,
    minibatch_size: int,
    model: MultiLabelClassifier,
    tokenizer: AutoTokenizer,
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
        for _, batch in enumerate(dataset):
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
    model: MultiLabelClassifier,
    tokenizer: AutoTokenizer,
) -> pd.DataFrame:
    """Process a batch of posts using the IME classification model."""
    try:
        minibatches: list[list[dict]] = [
            batch[i : i + minibatch_size] for i in range(0, len(batch), minibatch_size)
        ]

        batch_output_dfs: list[pd.DataFrame] = []

        for minibatch in minibatches:
            minibatch_df = pd.DataFrame(minibatch)
            output_df: pd.DataFrame = process_ime_minibatch(
                minibatch_df=minibatch_df,
                minibatch_size=minibatch_size,
                model=model,
                tokenizer=tokenizer,
            )
            batch_output_dfs.append(output_df)

        joined_output_df = pd.concat(batch_output_dfs)

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        joined_output_df = pd.DataFrame()

    finally:
        gc.collect()

    return joined_output_df
