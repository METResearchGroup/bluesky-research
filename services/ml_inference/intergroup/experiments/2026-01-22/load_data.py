import os

import pandas as pd

from services.ml_inference.models import PostToLabelModel

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
labeled_posts_filename = "intergroup_eval_dataset.csv"
labeled_posts_fp = os.path.join(
    parent_dir, "2026-01-10", labeled_posts_filename
)

# load in the ground truth labels and randomly sample posts from the
# ground truth labels.
def load_posts():
    df = pd.read_csv(labeled_posts_fp)
    return df

def create_batch(
    posts: pd.DataFrame,
    batch_size: int,
) -> tuple[list[PostToLabelModel], list[int]]:
    """Creates batch of posts and corresponding ground truth labels.
    
    Returns:
        list[PostToLabelModel]: List of posts.
        list[int]: List of ground truth labels.
    """
    sampled_posts = posts.sample(n=batch_size, replace=True)
    print(f"Sampled {len(sampled_posts)} posts")
    posts_list: list[PostToLabelModel] = []
    ground_truth_labels: list[int] = []
    for post in sampled_posts.to_dict(orient="records"):
        posts_list.append(
            PostToLabelModel(
                uri=post["uri"],
                text=post["text"],
                preprocessing_timestamp="test-timestamp",
                batch_id=1,
                batch_metadata="{}",
            )
        )
        ground_truth_labels.append(post["gold_label"])
    return posts_list, ground_truth_labels