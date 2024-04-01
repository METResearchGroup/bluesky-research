import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim


# example recommendation system setup: https://colab.research.google.com/github/google/eng-edu/blob/main/ml/recommendation-systems/recommendation-systems.ipynb
class TwoTowerModel(nn.Module):
    """Two-tower model for recommendation system
    
    Takes two towers, a "user" (query) and a "post" (item) tower, and learns
    a joint representation of the user-post embeddings.
    """
    def __init__(self, user_feature_dim: int=100, item_feature_dim: int=100, embedding_dim: int=100): # noqa
        super(TwoTowerModel, self).__init__()
        # User (query) tower
        self.user_tower = nn.Sequential(
            nn.Linear(user_feature_dim, embedding_dim),
            nn.ReLU(),
            nn.Linear(embedding_dim, embedding_dim)
        )

        # Post (item) tower
        self.post_tower = nn.Sequential(
            nn.Linear(user_feature_dim, embedding_dim),
            nn.ReLU(),
            nn.Linear(embedding_dim, embedding_dim)
        )

        # Final joint layer for binary classification
        self.final_layer = nn.Linear(embedding_dim, 1)

    def forward(self, user_features, item_features):
        user_embedding = self.user_tower(user_features)
        item_embedding = self.post_tower(item_features)
        joint_embedding = user_embedding * item_embedding
        return self.final_layer(joint_embedding)
