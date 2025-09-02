"""
Script to analyze the prevalence of each content label from baseline measures.

This script reads the total average baseline content label metrics CSV file and
extracts the prevalence (proportion) of each label, displaying them as percentages.

IMPORTANT DISTINCTION:
- "average" columns: Average probability scores (e.g., average toxicity score of 0.26)
- "proportion" columns: Percentage of posts classified as having a characteristic (e.g., 26% of posts classified as toxic)

This script ONLY analyzes "proportion" columns, which represent true prevalence - the actual
percentage of posts that were classified as having each characteristic.
"""

import os
import glob
from typing import Optional, Dict

import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(__file__)


def find_latest_total_average_file() -> Optional[str]:
    """Find the most recent total average baseline content label metrics CSV file.

    Returns:
        Path to the latest total average CSV file, or None if none found
    """
    results_dir = os.path.join(current_dir, "results")
    pattern = os.path.join(
        results_dir, "total_average_baseline_content_label_metrics_*.csv"
    )

    csv_files = glob.glob(pattern)

    if not csv_files:
        logger.warning(
            "No total average baseline content label metrics CSV files found"
        )
        return None

    # Sort by modification time and return the most recent
    latest_file = max(csv_files, key=os.path.getmtime)
    logger.info(f"Found latest total average file: {latest_file}")

    return latest_file


def load_total_average_data(csv_file_path: str) -> pd.DataFrame:
    """Load the total average baseline content label metrics CSV file.

    Args:
        csv_file_path: Path to the total average CSV file

    Returns:
        DataFrame containing the total average metrics
    """
    try:
        df = pd.read_csv(csv_file_path)
        logger.info(
            f"Loaded total average data with {len(df)} rows and {len(df.columns)} columns"
        )
        return df
    except Exception as e:
        logger.error(f"Failed to load total average data from {csv_file_path}: {e}")
        raise


def extract_prevalence_metrics(df: pd.DataFrame) -> Dict[str, float]:
    """Extract prevalence (proportion) metrics from the total average data.

    IMPORTANT: This function specifically looks at "proportion" columns, NOT "average" columns.
    - "average" columns contain the average probability scores (e.g., average toxicity score of 0.26)
    - "proportion" columns contain the proportion of posts that exceed the threshold (e.g., 26% of posts classified as toxic)

    Only the proportion columns represent true prevalence - the percentage of posts that were
    actually classified as having a particular characteristic.

    Args:
        df: DataFrame containing total average metrics

    Returns:
        Dictionary mapping label names to their prevalence percentages
    """
    # Get the first (and only) row of data
    if len(df) == 0:
        raise ValueError("No data found in the CSV file")

    row = df.iloc[0]

    # Extract ONLY proportion metrics (these represent true prevalence)
    # We explicitly exclude "average" columns as they represent average probabilities, not prevalence
    proportion_columns = [
        col for col in df.columns if col.startswith("baseline_proportion_")
    ]

    # Log what we're doing for clarity
    average_columns = [col for col in df.columns if col.startswith("baseline_average_")]
    logger.info(
        f"Found {len(proportion_columns)} proportion columns (prevalence) and {len(average_columns)} average columns (ignored)"
    )

    prevalence_metrics = {}

    for col in proportion_columns:
        # Extract the label name by removing the 'baseline_proportion_' prefix
        label_name = col.replace("baseline_proportion_", "")

        # Get the value and convert to percentage
        value = row[col]
        if pd.isna(value):
            logger.warning(f"No value found for {label_name}")
            prevalence_metrics[label_name] = 0.0
        else:
            # Convert proportion to percentage and round
            # This represents the percentage of posts that were classified as having this characteristic
            percentage = round(value * 100, 1)
            prevalence_metrics[label_name] = percentage

    logger.info(
        f"Extracted prevalence for {len(prevalence_metrics)} labels from proportion columns only"
    )
    return prevalence_metrics


def categorize_labels(
    prevalence_metrics: Dict[str, float],
) -> Dict[str, Dict[str, float]]:
    """Categorize labels by their type for better organization.

    Args:
        prevalence_metrics: Dictionary mapping label names to prevalence percentages

    Returns:
        Dictionary with categorized labels
    """
    categories = {
        "Toxicity & Safety": {},
        "Constructive Content": {},
        "Political & Sociopolitical": {},
        "Emotional Valence": {},
        "IME Categories": {},
        "Other Content": {},
    }

    for label, prevalence in prevalence_metrics.items():
        if any(
            toxic_term in label
            for toxic_term in [
                "toxic",
                "insult",
                "profanity",
                "threat",
                "identity_attack",
                "severe_toxic",
            ]
        ):
            categories["Toxicity & Safety"][label] = prevalence
        elif any(
            constructive_term in label
            for constructive_term in [
                "constructive",
                "affinity",
                "compassion",
                "curiosity",
                "nuance",
                "personal_story",
                "reasoning",
                "respect",
            ]
        ):
            categories["Constructive Content"][label] = prevalence
        elif any(
            political_term in label
            for political_term in ["sociopolitical", "political"]
        ):
            categories["Political & Sociopolitical"][label] = prevalence
        elif any(valence_term in label for valence_term in ["valence"]):
            categories["Emotional Valence"][label] = prevalence
        elif any(
            ime_term in label
            for ime_term in ["intergroup", "moral", "emotion", "other"]
        ):
            categories["IME Categories"][label] = prevalence
        else:
            categories["Other Content"][label] = prevalence

    return categories


def print_prevalence_summary(
    prevalence_metrics: Dict[str, float],
    categorized_labels: Dict[str, Dict[str, float]],
):
    """Print a formatted summary of label prevalence.

    Args:
        prevalence_metrics: Dictionary mapping label names to prevalence percentages
        categorized_labels: Dictionary with categorized labels
    """
    print("\n" + "=" * 80)
    print("CONTENT LABEL PREVALENCE ANALYSIS")
    print("=" * 80)
    print(f"Total labels analyzed: {len(prevalence_metrics)}")
    print(
        "Analysis based on PROPORTION columns (true prevalence) from baseline measures"
    )
    print(
        "NOTE: This shows the percentage of posts classified as having each characteristic"
    )
    print("=" * 80)

    # Print by category
    for category, labels in categorized_labels.items():
        if not labels:
            continue

        print(f"\n{category.upper()}:")
        print("-" * len(category))

        # Sort by prevalence (descending)
        sorted_labels = sorted(labels.items(), key=lambda x: x[1], reverse=True)

        for label, prevalence in sorted_labels:
            # Format label name for better readability
            formatted_label = label.replace("_", " ").title()
            print(f"  {formatted_label:<30} {prevalence:>6.1f}%")

    # Print top 10 most prevalent labels overall
    print("\nTOP 10 MOST PREVALENT LABELS:")
    print("-" * 40)
    sorted_all = sorted(prevalence_metrics.items(), key=lambda x: x[1], reverse=True)

    for i, (label, prevalence) in enumerate(sorted_all[:10], 1):
        formatted_label = label.replace("_", " ").title()
        print(f"  {i:2d}. {formatted_label:<30} {prevalence:>6.1f}%")

    print("\n" + "=" * 80)


def export_prevalence_results(prevalence_metrics: Dict[str, float]) -> str:
    """Export prevalence results to a CSV file.

    Args:
        prevalence_metrics: Dictionary mapping label names to prevalence percentages

    Returns:
        Path to the exported CSV file
    """
    current_datetime_str = generate_current_datetime_str()
    output_filename = f"label_prevalence_analysis_{current_datetime_str}.csv"
    output_path = os.path.join(current_dir, "results", output_filename)

    # Ensure results directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Create DataFrame
    df_data = []
    for label, prevalence in prevalence_metrics.items():
        df_data.append(
            {
                "label": label,
                "prevalence_percentage": prevalence,
                "formatted_label": label.replace("_", " ").title(),
            }
        )

    df = pd.DataFrame(df_data)
    df = df.sort_values("prevalence_percentage", ascending=False)

    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Exported prevalence analysis to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to export prevalence results to {output_path}: {e}")
        raise


def export_prevalence_markdown(
    prevalence_metrics: Dict[str, float],
    categorized_labels: Dict[str, Dict[str, float]],
) -> str:
    """Export prevalence analysis as a human-readable markdown file.

    Args:
        prevalence_metrics: Dictionary mapping label names to prevalence percentages
        categorized_labels: Dictionary with categorized labels

    Returns:
        Path to the exported markdown file
    """
    current_datetime_str = generate_current_datetime_str()
    output_filename = f"label_prevalence_analysis_{current_datetime_str}.md"
    output_path = os.path.join(current_dir, "results", output_filename)

    # Ensure results directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        with open(output_path, "w") as f:
            # Write header
            f.write("# Content Label Prevalence Analysis\n\n")
            f.write(
                "This analysis provides the prevalence (percentage) of each content label across all labeled posts in the dataset.\n\n"
            )
            f.write(
                "**Important Note**: This analysis uses PROPORTION columns only, which represent the percentage of posts that were actually classified as having each characteristic. Average columns (which contain average probability scores) are excluded.\n\n"
            )
            f.write("## Summary\n\n")
            f.write(f"- **Total labels analyzed**: {len(prevalence_metrics)}\n")
            f.write(
                "- **Analysis based on**: Total average baseline measures across study period\n"
            )
            f.write(f"- **Generated on**: {current_datetime_str}\n\n")
            f.write("---\n\n")

            # Write by category
            for category, labels in categorized_labels.items():
                if not labels:
                    continue

                f.write(f"## {category}\n\n")

                # Sort by prevalence (descending)
                sorted_labels = sorted(labels.items(), key=lambda x: x[1], reverse=True)

                for label, prevalence in sorted_labels:
                    # Format label name for better readability
                    formatted_label = label.replace("_", " ").title()
                    f.write(f"- **{formatted_label}**: {prevalence:.1f}%\n")

                f.write("\n")

            # Write top 10 most prevalent labels
            f.write("## Top 10 Most Prevalent Labels\n\n")
            sorted_all = sorted(
                prevalence_metrics.items(), key=lambda x: x[1], reverse=True
            )

            for i, (label, prevalence) in enumerate(sorted_all[:10], 1):
                formatted_label = label.replace("_", " ").title()
                f.write(f"{i}. **{formatted_label}**: {prevalence:.1f}%\n")

            f.write("\n---\n\n")
            f.write("## All Labels (Sorted by Prevalence)\n\n")

            for i, (label, prevalence) in enumerate(sorted_all, 1):
                formatted_label = label.replace("_", " ").title()
                f.write(f"{i:2d}. **{formatted_label}**: {prevalence:.1f}%\n")

        logger.info(f"Exported prevalence analysis markdown to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to export prevalence markdown to {output_path}: {e}")
        raise


def main():
    """Execute the prevalence analysis of baseline content labels."""

    try:
        # Find the latest total average file
        total_average_file = find_latest_total_average_file()
        if total_average_file is None:
            logger.error(
                "No total average file found. Please run the summarization script first."
            )
            return

        # Load the total average data
        df = load_total_average_data(total_average_file)

        # Extract prevalence metrics
        prevalence_metrics = extract_prevalence_metrics(df)

        # Categorize labels
        categorized_labels = categorize_labels(prevalence_metrics)

        # Print summary
        print_prevalence_summary(prevalence_metrics, categorized_labels)

        # Export results
        csv_output_path = export_prevalence_results(prevalence_metrics)
        markdown_output_path = export_prevalence_markdown(
            prevalence_metrics, categorized_labels
        )

        logger.info("Successfully completed prevalence analysis.")
        logger.info(f"CSV results saved to: {csv_output_path}")
        logger.info(f"Markdown results saved to: {markdown_output_path}")

    except Exception as e:
        logger.error(f"Failed to analyze prevalence: {e}")
        raise


if __name__ == "__main__":
    main()
