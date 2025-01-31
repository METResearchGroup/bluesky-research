"""Classes for the IME model."""

from typing import Literal

import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset
from transformers import AutoModel, AutoTokenizer

from lib.helper import RUN_MODE
from ml_tooling.ime.constants import (
    default_minibatch_size,
    model_to_asset_paths_map,
)
from ml_tooling.ime.helper import get_device

device = get_device()


class MultiLabelClassifier(nn.Module):
    """
    A multi-label classifier using a pre-trained transformer model.

    Args:
        n_classes (int): Number of output classes.
        model (str): Name or path of the pre-trained model.

    Methods:
        forward(input_ids, attention_mask):
            Performs a forward pass through the model and returns the probabilities.
    """

    def __init__(self, n_classes: int, model: str):
        super().__init__()
        if RUN_MODE == "test":
            # For testing, create a dummy model with minimal functionality
            self.model = nn.Linear(768, 768)  # Dummy layer
            self.classifier = nn.Linear(768, n_classes)
            self.sigmoid = nn.Sigmoid()
            return

        model_name = model_to_asset_paths_map[model]["model_name"]
        self.model = AutoModel.from_pretrained(model_name)
        self.classifier = nn.Linear(self.model.config.hidden_size, n_classes)
        self.sigmoid = nn.Sigmoid()

        pretrained_weights_path = model_to_asset_paths_map[model][
            "pretrained_weights_path"
        ]
        state_dict = torch.load(pretrained_weights_path, map_location=device)
        self.load_state_dict(state_dict)

    def forward(self, input_ids, attention_mask):
        outputs = self.model(input_ids=input_ids, attention_mask=attention_mask)

        if hasattr(outputs, "pooler_output"):
            pooled_output = outputs.pooler_output
        else:
            # Apply mean pooling on the last hidden state
            last_hidden_state = outputs.last_hidden_state
            pooled_output = torch.mean(last_hidden_state, dim=1)

        logits = self.classifier(pooled_output)
        probabilities = self.sigmoid(logits)
        return probabilities


class TextDataset(Dataset):
    """
    Dataset class for handling text data for multi-label classification.

    Args:
        tokenizer: The tokenizer to be used for encoding the text.
        df: A pandas DataFrame containing the text data and labels.
        mode: The mode of the dataset, either "train" or "test".

    Attributes:
        tokenizer: The tokenizer to be used for encoding the text.
        texts: A list of text data.
        labels: A numpy array of labels corresponding to the text data.

    Methods:
        __len__():
            Returns the number of samples in the dataset.
        __iter__():
            Returns a dictionary containing the input_ids, attention_mask, and labels for a given index.
    """

    def __init__(
        self,
        tokenizer: AutoTokenizer,
        df: pd.DataFrame,
        mode: Literal["train", "test"],
        batch_size: int = default_minibatch_size,
    ):
        self.tokenizer = tokenizer
        self.texts = df["text"].tolist()
        self.mode = mode
        self.df = df
        self.texts = df["text"].tolist()
        self.batch_size = batch_size
        if mode == "train":
            self.labels = df[["Emotion", "Intergroup", "Moral", "Other"]].values
        else:
            self.labels = None

    def __len__(self):
        return len(self.texts)

    def __iter__(self):
        """Iterate over the dataset in batches."""
        for i in range(0, len(self.texts), self.batch_size):
            batch_texts = self.texts[i : i + self.batch_size]

            inputs = self.tokenizer(
                batch_texts,
                add_special_tokens=True,
                return_tensors="pt",
                padding="max_length",
                truncation=True,
            )

            batch = {
                "input_ids": inputs["input_ids"],
                "attention_mask": inputs["attention_mask"],
            }

            yield batch

    # def __getitem__(self, idx):
    #     text = self.texts[idx]
    #     if self.mode == "train":
    #         labels = self.labels[idx]
    #     else:
    #         labels = None
    #     inputs = self.tokenizer(
    #         text,
    #         add_special_tokens=True,  # Add '[CLS]' and '[SEP]'
    #         return_tensors="pt",  # Return PyTorch tensors
    #         padding="max_length",  # Pad to a length specified by the max_length argument
    #         truncation=True,
    #     )
    #     input_ids = inputs["input_ids"].squeeze()
    #     attention_mask = inputs["attention_mask"].squeeze()
    #     if self.mode == "train":
    #         return {
    #             "input_ids": input_ids,
    #             "attention_mask": attention_mask,
    #             "labels": torch.tensor(labels, dtype=torch.float),
    #         }
    #     else:
    #         return {"input_ids": input_ids, "attention_mask": attention_mask}
