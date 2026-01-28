import os

import pandas as pd

from services.ml_inference.models import PostToLabelModel

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
labeled_posts_filename = "intergroup_eval_dataset.csv"
labeled_posts_fp = os.path.join(
    parent_dir, "2026-01-10", labeled_posts_filename
)

synthetic_posts_dir = os.path.join(current_dir, "synthetic_data")
# load in the ground truth labels and randomly sample posts from the
# ground truth labels.

def load_labeled_posts() -> pd.DataFrame:
    df: pd.DataFrame = pd.read_csv(labeled_posts_fp)
    df = df[["uri", "text", "gold_label"]] # type: ignore
    df = df.reset_index(drop=True)
    return df

def load_synthetic_posts() -> pd.DataFrame:
    """Load all the synthetic posts from the synthetic_data directory as
    a single dataframe."""
    synthetic_posts_fps = os.listdir(synthetic_posts_dir)
    synthetic_posts_fps = [
        os.path.join(synthetic_posts_dir, fp) for fp in synthetic_posts_fps
        if fp.endswith(".csv")
    ]
    synthetic_posts_dfs = [pd.read_csv(fp) for fp in synthetic_posts_fps]

    total_synthetic_posts_df: pd.DataFrame = pd.concat(synthetic_posts_dfs)
    total_synthetic_posts_df = total_synthetic_posts_df.reset_index(drop=True)
    total_synthetic_posts_df = (
        total_synthetic_posts_df[["uri", "text", "gold_label"]] # type: ignore
    )
    return total_synthetic_posts_df

def load_posts(include_synthetic_records: bool = True) -> pd.DataFrame:
    """Load all the posts from the labeled_posts and synthetic_posts directories
    as a single dataframe."""
    labeled_posts: pd.DataFrame = load_labeled_posts()
    if include_synthetic_records:
        synthetic_posts: pd.DataFrame = load_synthetic_posts()
        posts: pd.DataFrame = pd.concat([labeled_posts, synthetic_posts])
    else:
        posts = labeled_posts
    return posts

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