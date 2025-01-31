"""Train the IME model."""

# import os
# import pandas as pd

# import torch
# import torch.nn as nn
# from torch.optim import AdamW
# from transformers import AutoTokenizer, get_linear_schedule_with_warmup
# from torch.utils.data import DataLoader


# def train_model(
#     model_name: str,
#     train: pd.DataFrame,
#     eval: pd.DataFrame,
#     model_folder: str,
#     task_folder: str,
#     num_epochs: int,
#     num_class: int = 4,
# ):
#     """Trains the model."""

#     tokenizer: AutoTokenizer = AutoTokenizer.from_pretrained(model_name)

#     dataset: TextDataset = TextDataset(tokenizer=tokenizer, df=train, mode="train")
#     loader: DataLoader = DataLoader(dataset, batch_size=16, shuffle=True)
#     eval_dataset: TextDataset = TextDataset(tokenizer=tokenizer, df=eval, mode="test")
#     eval_loader: DataLoader = DataLoader(eval_dataset, batch_size=16, shuffle=True)

#     model = MultiLabelClassifier(n_classes=num_class, model=model_name)

#     criterion = nn.BCELoss()
#     optimizer = AdamW(model.parameters(), lr=1e-5)

#     model.to(device)

#     scheduler = get_linear_schedule_with_warmup(
#         optimizer, num_warmup_steps=0, num_training_steps=len(loader) * num_epochs
#     )

#     for epoch in range(num_epochs):
#         model.train()
#         total_loss = 0
#         for batch in loader:
#             input_ids = batch["input_ids"].to(device)
#             attention_mask = batch["attention_mask"].to(device)
#             labels = batch["labels"].to(device)

#             model.zero_grad()
#             outputs = model(input_ids=input_ids, attention_mask=attention_mask)

#             loss = criterion(outputs, labels)
#             total_loss += loss.item()

#             loss.backward()
#             optimizer.step()
#             scheduler.step()

#         avg_loss = total_loss / len(loader)
#         print(f"Epoch {epoch}, Average Loss: {avg_loss}")

#         model.eval()
#         val_loss = 0
#         with torch.no_grad():
#             for batch in eval_loader:
#                 input_ids = batch["input_ids"].to(device)
#                 attention_mask = batch["attention_mask"].to(device)
#                 labels = batch["labels"].to(device)

#                 outputs = model(input_ids=input_ids, attention_mask=attention_mask)

#                 loss = criterion(outputs, labels)
#                 val_loss += loss.item()

#         avg_val_loss = val_loss / len(eval_loader)
#         print(f"Epoch {epoch}, Average Validation Loss: {avg_val_loss}")

#     # export model and tokenizer.
#     model_folder = os.path.join(
#         current_file_directory, model_folder, task_folder, f"{model_name}/model"
#     )
#     if not os.path.exists(model_folder):
#         os.makedirs(model_folder)
#     model_path = os.path.join(model_folder, f"{model_name}_multilabel_classifier.pth")
#     torch.save(model.state_dict(), model_path)

#     tokenizer_folder = os.path.join(
#         current_file_directory, model_folder, task_folder, f"{model_name}/tokenizer"
#     )
#     if not os.path.exists(tokenizer_folder):
#         os.makedirs(tokenizer_folder)
#     tokenizer_path = os.path.join(tokenizer_folder, f"{model_name}_tokenizer.pth")
#     tokenizer.save_pretrained(tokenizer_path)


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
