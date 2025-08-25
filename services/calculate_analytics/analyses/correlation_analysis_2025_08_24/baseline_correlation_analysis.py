"""
Baseline Correlation Analysis for Toxicity-Constructiveness Study

This script implements the baseline correlation analysis across all Bluesky posts (20-30M posts)
to investigate the relationship between toxicity and constructiveness scores. The analysis
aims to determine whether observed correlations are real data patterns that replicate across
a wide sample of Bluesky posts, or if they are artifacts of data selection or processing.

Research Question: "Look at the correlation of toxicity x constructiveness on all posts we have,
to see if this is a trend that replicates across a wide sample of Bluesky posts."

The analysis will:
- Process all available posts using Slurm for large-scale data handling
- Calculate both Pearson and Spearman correlation coefficients
- Implement daily batch processing with garbage collection for memory management
- Generate CSV output with correlation results and statistical summaries
- Provide baseline understanding of toxicity-constructiveness relationships

This baseline analysis serves as Phase 1 of the correlation investigation project and will
be compared against feed selection bias analysis and calculation logic review in subsequent phases.
"""

import os

import pandas as pd

from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_perspective_api_labels,
)
from services.calculate_analytics.shared.analysis.correlations import (
    calculate_correlations,
    write_correlation_results,
)

output_dir = os.path.join(os.path.dirname(__file__), "results")
logger = get_logger(__name__)


def main():
    """
    Main execution function for baseline correlation analysis.
    """
    logger.info("Starting baseline correlation analysis")

    table_columns = ["uri", "partition_date", "prob_toxic", "prob_constructive"]
    table_columns_str = ", ".join(table_columns)
    query = (
        f"SELECT {table_columns_str} "
        f"FROM ml_inference_perspective_api "
        f"WHERE prob_toxic IS NOT NULL "
        f"AND prob_constructive IS NOT NULL "
    ).strip()

    df: pd.DataFrame = get_perspective_api_labels(
        lookback_start_date=STUDY_START_DATE,
        lookback_end_date=STUDY_END_DATE,
        duckdb_query=query,
        query_metadata={
            "tables": [
                {"name": "ml_inference_perspective_api", "columns": table_columns}
            ]
        },
        export_format="duckdb",
    )

    # Generate summary and write results
    results = calculate_correlations(df, "prob_toxic", "prob_constructive")
    write_correlation_results(
        results,
        output_dir,
        f"baseline_correlations_{generate_current_datetime_str()}.json",
    )
    logger.info("Baseline correlation analysis complete")


if __name__ == "__main__":
    main()
