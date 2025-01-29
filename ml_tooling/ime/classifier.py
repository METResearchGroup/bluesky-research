"""Interface for the IME classification model."""

from typing import Literal

import os
import pandas as pd

import torch
import torch.nn as nn
from torch.optim import AdamW
from transformers import AutoModel, AutoTokenizer, get_linear_schedule_with_warmup
from torch.utils.data import Dataset, DataLoader

current_file_directory = os.path.dirname(os.path.abspath(__file__))
default_num_classes = 4
default_batch_size = 512
default_minibatch_size = 32


def get_device():
    if torch.cuda.is_available():
        print("CUDA backend available.")
        device = torch.device("cuda")
    elif torch.backends.mps.is_available() and torch.backends.mps.is_built():
        print("Arm mac GPU available, using GPU.")
        device = torch.device("mps")  # for Arm Macs
    else:
        print("GPU not available, using CPU")
        device = torch.device("cpu")
        # raise ValueError("GPU not available, using CPU")
    return device


device = get_device()

model_to_asset_paths_map = {
    "distilbert": {
        "model_name": "distilbert-base-uncased",
        "pretrained_weights_path": os.path.join(
            current_file_directory,
            "models",
            "benchmark_distilbert-base-uncased",
            "distilbert-base-uncased",
            "model",
            "distilbert-base-uncased_multilabel_classifier.pth",
        ),
        "tokenizer": os.path.join(
            current_file_directory,
            "models",
            "benchmark_distilbert-base-uncased",
            "distilbert-base-uncased",
            "tokenizer",
        ),
    },
    "roberta": {
        "model_name": "roberta-base",
        "pretrained_weights_path": os.path.join(
            current_file_directory,
            "models",
            "benchmark_roberta",
            "roberta-base",
            "model",
            "roberta-base_multilabel_classifier.pth",
        ),
        "tokenizer": os.path.join(
            current_file_directory,
            "models",
            "benchmark_roberta",
            "roberta-base",
            "tokenizer",
        ),
    },
}


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
        model_name = model_to_asset_paths_map[model]["model_name"]
        self.model = AutoModel.from_pretrained(model_name)
        self.classifier = nn.Linear(self.model.config.hidden_size, n_classes)
        self.sigmoid = nn.Sigmoid()

        pretrained_weights_path = model_name = model_to_asset_paths_map[model][
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
        __getitem__(idx):
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


def train_model(
    model_name: str,
    train: pd.DataFrame,
    eval: pd.DataFrame,
    model_folder: str,
    task_folder: str,
    num_epochs: int,
    num_class: int = 4,
):
    """Trains the model."""

    tokenizer: AutoTokenizer = AutoTokenizer.from_pretrained(model_name)

    dataset: TextDataset = TextDataset(tokenizer=tokenizer, df=train, mode="train")
    loader: DataLoader = DataLoader(dataset, batch_size=16, shuffle=True)
    eval_dataset: TextDataset = TextDataset(tokenizer=tokenizer, df=eval, mode="test")
    eval_loader: DataLoader = DataLoader(eval_dataset, batch_size=16, shuffle=True)

    model = MultiLabelClassifier(n_classes=num_class, model=model_name)

    criterion = nn.BCELoss()
    optimizer = AdamW(model.parameters(), lr=1e-5)

    model.to(device)

    scheduler = get_linear_schedule_with_warmup(
        optimizer, num_warmup_steps=0, num_training_steps=len(loader) * num_epochs
    )

    for epoch in range(num_epochs):
        model.train()
        total_loss = 0
        for batch in loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            model.zero_grad()
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)

            loss = criterion(outputs, labels)
            total_loss += loss.item()

            loss.backward()
            optimizer.step()
            scheduler.step()

        avg_loss = total_loss / len(loader)
        print(f"Epoch {epoch}, Average Loss: {avg_loss}")

        model.eval()
        val_loss = 0
        with torch.no_grad():
            for batch in eval_loader:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)
                labels = batch["labels"].to(device)

                outputs = model(input_ids=input_ids, attention_mask=attention_mask)

                loss = criterion(outputs, labels)
                val_loss += loss.item()

        avg_val_loss = val_loss / len(eval_loader)
        print(f"Epoch {epoch}, Average Validation Loss: {avg_val_loss}")

    # export model and tokenizer.
    model_folder = os.path.join(
        current_file_directory, model_folder, task_folder, f"{model_name}/model"
    )
    if not os.path.exists(model_folder):
        os.makedirs(model_folder)
    model_path = os.path.join(model_folder, f"{model_name}_multilabel_classifier.pth")
    torch.save(model.state_dict(), model_path)

    tokenizer_folder = os.path.join(
        current_file_directory, model_folder, task_folder, f"{model_name}/tokenizer"
    )
    if not os.path.exists(tokenizer_folder):
        os.makedirs(tokenizer_folder)
    tokenizer_path = os.path.join(tokenizer_folder, f"{model_name}_tokenizer.pth")
    tokenizer.save_pretrained(tokenizer_path)


# def test_model(model_name, test,
#                model_folder, task_folder,
#                num_class=4):
#     # Load the model
#     model = MultiLabelClassifier(n_classes=num_class, model=model_name)
#     model_path = os.path.join(PROJECT_PATH, model_folder, task_folder,
#                               f'{model_name}/model', f'{model_name}_multilabel_classifier.pth')
#     model.load_state_dict(torch.load(model_path))
#     model.to(device)
#     model.eval()  # Important to set the model to evaluation mode

#     # ensure the tokenizer is the same
#     tokenizer_path = os.path.join(PROJECT_PATH, model_folder, task_folder, f'{model_name}/tokenizer')
#     tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

#     # tokenizer dependent
#     test_dataset = TextDataset(tokenizer, test)
#     test_loader = DataLoader(test_dataset, batch_size=16, shuffle=False)  # shuffle=False for testing

#     # Collect all predictions and true labels
#     all_preds = []
#     all_probabilities = []
#     all_labels = []

#     with torch.no_grad():
#         for batch in test_loader:
#             input_ids = batch['input_ids'].to(device)
#             attention_mask = batch['attention_mask'].to(device)
#             labels = batch['labels'].to(device)
#             outputs = model(input_ids=input_ids, attention_mask=attention_mask)
#             probs = outputs.cpu().numpy()  # Get probability scores
#             preds = (probs > 0.5).astype(int)  # Apply threshold to get binary predictions
#             preds = default_to_other(preds)
#             all_probabilities.extend(probs)  # Store the probabilities
#             all_preds.extend(preds)  # Store the predictions
#             all_labels.extend(labels.cpu().numpy())

#     # Convert the lists into arrays
#     all_preds = np.array(all_preds)
#     all_probabilities = np.array(all_probabilities)
#     all_labels = np.array(all_labels)

#     return all_probabilities, all_preds, all_labels


# # function for deployment
# def deploy_model(model_name, test,
#                  model_folder, task_folder,
#                  num_class=4):
#     # Load the model
#     model = MultiLabelClassifier(n_classes=num_class, model=model_name)
#     model_path = os.path.join(
#         PROJECT_PATH, model_folder, task_folder,
#                               f'{model_name}/model', f'{model_name}_multilabel_classifier.pth')
#     model.load_state_dict(torch.load(model_path))
#     model.to(device)
#     model.eval()  # Important to set the model to evaluation mode

#     # ensure the tokenizer is the same
#     tokenizer_path = os.path.join(PROJECT_PATH, model_folder, task_folder, f'{model_name}/tokenizer')
#     tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

#     # tokenizer dependent
#     test_dataset = TextDataset_v2(tokenizer, test)
#     test_loader = DataLoader(test_dataset, batch_size=16, shuffle=False)  # shuffle=False for testing

#     # Collect all predictions and true labels
#     all_preds = []
#     all_probabilities = []

#     with torch.no_grad():
#         for batch in test_loader:
#             input_ids = batch['input_ids'].to(device)
#             attention_mask = batch['attention_mask'].to(device)
#             outputs = model(input_ids=input_ids, attention_mask=attention_mask)
#             probs = outputs.cpu().numpy()  # Get probability scores
#             preds = (probs > 0.5).astype(int)
#             preds = default_to_other(preds)
#             all_probabilities.extend(probs)  # Store the probabilities
#             all_preds.extend(preds)  # Store the predictions

#     # Convert the lists into arrays
#     all_preds = np.array(all_preds)
#     all_probabilities = np.array(all_probabilities)

#     return all_probabilities, all_preds


def load_model_and_tokenizer(
    model_name: str,
) -> tuple[MultiLabelClassifier, AutoTokenizer]:
    """Load the model and tokenizer for the given model name, from
    local storage."""
    model = MultiLabelClassifier(n_classes=default_num_classes, model=model_name)
    model.to(device)
    model.eval()

    tokenizer_path = model_to_asset_paths_map[model_name]["tokenizer"]
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

    return model, tokenizer


# TODO: refactor this file (e.g., move some stuff to "train.py")
def process_ime_batch(batch: list[dict]) -> list[dict]:
    """Process a batch of posts using the IME classification model."""
    pass


if __name__ == "__main__":
    model, tokenizer = load_model_and_tokenizer("distilbert")
    breakpoint()
