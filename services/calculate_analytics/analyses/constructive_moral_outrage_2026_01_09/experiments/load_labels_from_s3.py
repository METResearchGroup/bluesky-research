"""Loads the data from S3.

Filters out posts that are both constructive AND have moral outrage. Then
saves the data to a CSV file.

We found through estimates that out of all constructive posts, ~10% also
have moral outrage. We're interested in seeing how it skews our data.
"""

import os

from lib.aws.athena import Athena
import pandas as pd

athena = Athena()

labels_table_name = "archive_ml_inference_perspective_api"
posts_used_in_feeds_table_name = "archive_fetch_posts_used_in_feeds"

fp_labels = os.path.join(
    os.path.dirname(__file__), "constructive_moral_outrage_2026_01_09.csv"
)

query = f"""
    SELECT *
    FROM {labels_table_name}
    WHERE NOT (
        prob_constructive > 0.5
        AND prob_moral_outrage > 0.5
    )
    AND uri IN (
        SELECT
            uri
            FROM {posts_used_in_feeds_table_name}
    )
"""

if __name__ == "__main__":
    df: pd.DataFrame = athena.query_results_as_df(query)
    df.to_csv(fp_labels)
