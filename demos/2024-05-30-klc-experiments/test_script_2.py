"""Test script 2.

This script tests if we can access the data and files from our project
directory.
"""
from lib.db.sql.preprocessing_database import get_filtered_posts
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

if __name__ == "__main__":
    posts: list[FilteredPreprocessedPostModel] = get_filtered_posts()
    print(f"Number of filtered posts: {len(posts)}")
    print("Posts successfully loaded!")

# Path: demos/2024-05-30-klc-experiments/test_script_2.py
