#!/usr/bin/env python3
"""
Topic Proportion Visualization for Topic Modeling Results

This script generates publication-ready visualizations showing the distribution
of topics across different slices (conditions, time periods, etc.). It creates
both stacked bar charts and grouped bar charts to analyze topic proportions.

Author: AI Agent implementing Scientific Visualization Specialist
Date: 2025-01-17
"""

import argparse
import json
import time
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)


class TopicProportionVisualizer:
    """
    Generates topic proportion visualizations for topic modeling results.

    Creates publication-ready charts showing the relative distribution of topics
    across different slices (conditions, time periods, etc.). Supports both
    stacked bar charts and grouped bar charts for different analytical needs.
    """

    def __init__(self, model_path: str, metadata_path: str, results_path: str):
        """
        Initialize topic proportion visualizer.

        Args:
            model_path: Path to trained BERTopic model directory
            metadata_path: Path to model metadata JSON file
            results_path: Path to inference results directory
        """
        self.model_path = model_path
        self.metadata_path = metadata_path
        self.results_path = results_path

        # Scientific visualization settings
        self.figure_params = {"figsize": (14, 8), "dpi": 300, "facecolor": "white"}

        # Colorblind-friendly color palette
        self.color_palette = [
            "#1f77b4",  # blue
            "#ff7f0e",  # orange
            "#2ca02c",  # green
            "#d62728",  # red
            "#9467bd",  # purple
            "#8c564b",  # brown
            "#e377c2",  # pink
            "#7f7f7f",  # gray
            "#bcbd22",  # olive
            "#17becf",  # cyan
            "#aec7e8",  # light blue
            "#ffbb78",  # light orange
            "#98df8a",  # light green
            "#ff9896",  # light red
            "#c5b0d5",  # light purple
            "#ffcc99",  # light orange 2
            "#c7c7c7",  # light gray
            "#d4a5a5",  # light brown
            "#e1b3e1",  # light purple 2
            "#b3d9b3",  # light green 2
        ]

        self.metadata = None
        self.topic_assignments = None
        self.topic_names = None
        self.documents_df = None
        self.uri_doc_map = None
        self.date_condition_uris_map = None

    def load_model_and_results(self) -> None:
        """Load trained model and inference results."""
        logger.info("📂 Loading trained model and inference results...")

        # Load metadata first
        with open(self.metadata_path, "r") as f:
            self.metadata = json.load(f)
        logger.info("✅ Model metadata loaded successfully")

        # Load inference results
        self._load_inference_results()
        logger.info("✅ Inference results loaded successfully")

        # Load topic names if available
        self._load_topic_names()
        logger.info("✅ Topic names loaded successfully")

        # Load exported data for slicing
        self._load_exported_data()
        logger.info("✅ Exported data loaded successfully")

    def _load_inference_results(self) -> None:
        """Load topic assignments from inference results."""
        # Look for topic assignments file
        assignments_files = list(
            Path(self.results_path).glob("*topic_assignments*.csv")
        )
        if not assignments_files:
            raise FileNotFoundError(
                f"No topic assignments file found in {self.results_path}"
            )

        assignments_file = assignments_files[0]  # Use the first one found
        logger.info(f"📄 Loading topic assignments from: {assignments_file}")

        # Load topic assignments
        self.topic_assignments = pd.read_csv(assignments_file)
        logger.info(f"📊 Loaded {len(self.topic_assignments)} topic assignments")

        # Filter out outliers (topic -1) for cleaner visualization
        original_count = len(self.topic_assignments)
        self.topic_assignments = self.topic_assignments[
            self.topic_assignments["topic_id"] != -1
        ]
        filtered_count = len(self.topic_assignments)
        outliers_count = original_count - filtered_count

        logger.info(f"🔍 Filtered out {outliers_count} outliers (topic -1)")
        logger.info(f"📊 Using {filtered_count} documents for visualization")

    def _load_topic_names(self) -> None:
        """Load generated topic names if available."""
        # Try to find topic names file in the model directory
        model_dir = Path(
            self.model_path
        ).parent  # Go up one level from model file to timestamp dir

        topic_names_file = model_dir / "topic_names.csv"
        if topic_names_file.exists():
            logger.info(f"📄 Loading topic names from: {topic_names_file}")
            topic_names_df = pd.read_csv(topic_names_file)
            self.topic_names = dict(
                zip(topic_names_df["topic_id"], topic_names_df["generated_name"])
            )
            logger.info(f"📊 Loaded {len(self.topic_names)} topic names")
        else:
            logger.info("📄 No topic names file found, using default topic labels")
            self.topic_names = {}

    def _load_exported_data(self) -> None:
        """Load exported data for slicing capabilities."""
        # Look for exported data in the model directory
        model_dir = Path(self.model_path).parent
        exported_data_dir = model_dir / "exported_data"

        if exported_data_dir.exists():
            logger.info(f"📂 Loading exported data from: {exported_data_dir}")

            # Load core data structures
            self.documents_df = pd.read_parquet(
                f"{exported_data_dir}/documents_df.parquet"
            )
            self.uri_doc_map = pd.read_parquet(
                f"{exported_data_dir}/uri_doc_map.parquet"
            )

            with open(f"{exported_data_dir}/date_condition_uris_map.json") as f:
                # Convert lists back to sets for easier manipulation
                raw_map = json.load(f)
                self.date_condition_uris_map = {}
                for date, condition_map in raw_map.items():
                    self.date_condition_uris_map[date] = {}
                    for condition, uris in condition_map.items():
                        self.date_condition_uris_map[date][condition] = set(uris)

            logger.info("✅ Exported data loaded successfully")
        else:
            logger.warning("⚠️ No exported data found - slicing capabilities limited")

    def compute_topic_proportions_by_slice(
        self, slice_configs: List[Dict[str, Union[str, List[str]]]]
    ) -> pd.DataFrame:
        """
        Compute topic proportions for each slice configuration.

        Args:
            slice_configs: List of slice configurations with 'condition', 'date_range', 'title_suffix' keys

        Returns:
            DataFrame with columns: slice_name, topic_id, topic_name, proportion, count, total_documents
        """
        logger.info(
            f"📊 Computing topic proportions for {len(slice_configs)} slices..."
        )

        results = []

        for config in slice_configs:
            slice_name = config.get("title_suffix", "Unknown")
            condition = config.get("condition")
            date_range = config.get("date_range")

            logger.info(f"   📊 Processing slice: {slice_name}")

            # Get URIs for this slice
            slice_uris = self._get_uris_for_slice(condition, date_range)

            if len(slice_uris) == 0:
                logger.warning(f"   ⚠️ No URIs found for slice: {slice_name}")
                continue

            # Get doc_ids for this slice
            slice_doc_ids = self.uri_doc_map[self.uri_doc_map["uri"].isin(slice_uris)][
                "doc_id"
            ].unique()

            # Filter topic assignments for this slice
            slice_assignments = self.topic_assignments[
                self.topic_assignments["doc_id"].isin(slice_doc_ids)
            ]

            if len(slice_assignments) == 0:
                logger.warning(
                    f"   ⚠️ No topic assignments found for slice: {slice_name}"
                )
                continue

            # Compute topic proportions
            topic_counts = slice_assignments["topic_id"].value_counts().sort_index()
            total_documents = len(slice_assignments)

            for topic_id, count in topic_counts.items():
                proportion = count / total_documents

                # Get topic name
                topic_name = self.topic_names.get(topic_id, f"Topic {topic_id}")

                results.append(
                    {
                        "slice_name": slice_name,
                        "topic_id": topic_id,
                        "topic_name": topic_name,
                        "proportion": proportion,
                        "count": count,
                        "total_documents": total_documents,
                    }
                )

            logger.info(f"   ✅ Processed {total_documents} documents for {slice_name}")

        results_df = pd.DataFrame(results)
        logger.info(
            f"📊 Computed proportions for {len(results_df)} topic-slice combinations"
        )

        return results_df

    def _get_uris_for_slice(
        self, condition: Optional[str], date_range: Optional[List[str]]
    ) -> set:
        """Get URIs for a specific condition and date range."""
        if not self.date_condition_uris_map:
            logger.warning("⚠️ No date-condition mapping available, returning all URIs")
            return set(self.uri_doc_map["uri"].unique())

        slice_uris = set()

        if condition and date_range:
            # Both condition and date range specified
            for date in date_range:
                if date in self.date_condition_uris_map:
                    if condition in self.date_condition_uris_map[date]:
                        slice_uris.update(self.date_condition_uris_map[date][condition])
        elif condition:
            # Only condition specified
            for date, condition_map in self.date_condition_uris_map.items():
                if condition in condition_map:
                    slice_uris.update(condition_map[condition])
        elif date_range:
            # Only date range specified
            for date in date_range:
                if date in self.date_condition_uris_map:
                    for condition_uris in self.date_condition_uris_map[date].values():
                        slice_uris.update(condition_uris)
        else:
            # Neither specified - return all URIs
            for date, condition_map in self.date_condition_uris_map.items():
                for condition_uris in condition_map.values():
                    slice_uris.update(condition_uris)

        return slice_uris

    def create_stacked_bar_chart(
        self, proportions_df: pd.DataFrame
    ) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create stacked bar chart showing topic proportions by slice.

        Args:
            proportions_df: DataFrame with topic proportion data

        Returns:
            Tuple of (figure, axes)
        """
        logger.info("🎨 Creating stacked bar chart...")

        # Pivot data for stacked bar chart
        pivot_df = proportions_df.pivot(
            index="slice_name", columns="topic_id", values="proportion"
        ).fillna(0)

        # Sort topics by overall frequency
        topic_frequencies = (
            proportions_df.groupby("topic_id")["count"]
            .sum()
            .sort_values(ascending=False)
        )
        pivot_df = pivot_df[topic_frequencies.index]

        # Create figure
        fig, ax = plt.subplots(
            figsize=self.figure_params["figsize"],
            dpi=self.figure_params["dpi"],
            facecolor=self.figure_params["facecolor"],
        )

        # Create stacked bar chart
        bottom = np.zeros(len(pivot_df))
        colors = self.color_palette[: len(pivot_df.columns)]

        for i, topic_id in enumerate(pivot_df.columns):
            topic_name = self.topic_names.get(topic_id, f"Topic {topic_id}")
            ax.bar(
                pivot_df.index,
                pivot_df[topic_id],
                bottom=bottom,
                label=topic_name,
                color=colors[i % len(colors)],
                alpha=0.8,
                edgecolor="white",
                linewidth=0.5,
            )
            bottom += pivot_df[topic_id]

        # Apply styling
        self._apply_stacked_bar_styling(fig, ax, pivot_df)

        logger.info("✅ Stacked bar chart created successfully")
        return fig, ax

    def create_grouped_bar_chart(
        self, proportions_df: pd.DataFrame
    ) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create grouped bar chart showing topic proportions by slice.

        Args:
            proportions_df: DataFrame with topic proportion data

        Returns:
            Tuple of (figure, axes)
        """
        logger.info("🎨 Creating grouped bar chart...")

        # Pivot data for grouped bar chart
        pivot_df = proportions_df.pivot(
            index="topic_id", columns="slice_name", values="proportion"
        ).fillna(0)

        # Sort topics by overall frequency
        topic_frequencies = (
            proportions_df.groupby("topic_id")["count"]
            .sum()
            .sort_values(ascending=False)
        )
        pivot_df = pivot_df.reindex(topic_frequencies.index)

        # Create figure
        fig, ax = plt.subplots(
            figsize=self.figure_params["figsize"],
            dpi=self.figure_params["dpi"],
            facecolor=self.figure_params["facecolor"],
        )

        # Create grouped bar chart
        x = np.arange(len(pivot_df))
        width = 0.8 / len(pivot_df.columns)
        colors = self.color_palette[: len(pivot_df.columns)]

        for i, slice_name in enumerate(pivot_df.columns):
            ax.bar(
                x + i * width,
                pivot_df[slice_name],
                width,
                label=slice_name,
                color=colors[i % len(colors)],
                alpha=0.8,
                edgecolor="white",
                linewidth=0.5,
            )

        # Apply styling
        self._apply_grouped_bar_styling(fig, ax, pivot_df)

        logger.info("✅ Grouped bar chart created successfully")
        return fig, ax

    def _apply_stacked_bar_styling(
        self, fig: plt.Figure, ax: plt.Axes, pivot_df: pd.DataFrame
    ) -> None:
        """Apply scientific styling to stacked bar chart."""
        # Set title
        ax.set_title(
            "Topic Distribution by Condition/Time Period\n(Stacked Bar Chart)",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )

        # Set axis labels
        ax.set_xlabel("Slice Category", fontsize=12, fontweight="bold")
        ax.set_ylabel("Proportion", fontsize=12, fontweight="bold")

        # Set y-axis to percentage
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x*100:.0f}%"))

        # Set axis properties
        ax.tick_params(axis="both", which="major", labelsize=10)
        ax.grid(True, alpha=0.3, linestyle="-", linewidth=0.5, axis="y")

        # Set legend
        handles, labels = ax.get_legend_handles_labels()
        n_topics = len(labels)
        if n_topics <= 15:
            ax.legend(
                handles,
                labels,
                bbox_to_anchor=(1.05, 1),
                loc="upper left",
                fontsize=9,
                frameon=True,
                fancybox=True,
                shadow=True,
                title="Topics",
            )
        else:
            # Show only top 10 topics
            top_topics = pivot_df.sum().nlargest(10).index
            top_handles = [
                handles[labels.index(f"Topic {tid}")]
                for tid in top_topics
                if f"Topic {tid}" in labels
            ]
            top_labels = [
                f"Topic {tid}" for tid in top_topics if f"Topic {tid}" in labels
            ]
            ax.legend(
                top_handles,
                top_labels,
                bbox_to_anchor=(1.05, 1),
                loc="upper left",
                fontsize=9,
                frameon=True,
                fancybox=True,
                shadow=True,
                title="Top 10 Topics",
            )

        # Rotate x-axis labels if needed
        if len(pivot_df.index) > 5:
            plt.xticks(rotation=45, ha="right")

        # Tight layout
        plt.tight_layout()

    def _apply_grouped_bar_styling(
        self, fig: plt.Figure, ax: plt.Axes, pivot_df: pd.DataFrame
    ) -> None:
        """Apply scientific styling to grouped bar chart."""
        # Set title
        ax.set_title(
            "Topic Distribution by Condition/Time Period\n(Grouped Bar Chart)",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )

        # Set axis labels
        ax.set_xlabel("Topic", fontsize=12, fontweight="bold")
        ax.set_ylabel("Proportion", fontsize=12, fontweight="bold")

        # Set y-axis to percentage
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x*100:.0f}%"))

        # Set axis properties
        ax.tick_params(axis="both", which="major", labelsize=10)
        ax.grid(True, alpha=0.3, linestyle="-", linewidth=0.5, axis="y")

        # Set x-axis labels
        topic_labels = [self.topic_names.get(tid, f"T{tid}") for tid in pivot_df.index]
        ax.set_xticks(range(len(pivot_df)))
        ax.set_xticklabels(topic_labels, rotation=45, ha="right")

        # Set legend
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            fontsize=10,
            frameon=True,
            fancybox=True,
            shadow=True,
            title="Slice Category",
        )

        # Tight layout
        plt.tight_layout()

    def save_results(
        self,
        stacked_fig: plt.Figure,
        grouped_fig: plt.Figure,
        proportions_df: pd.DataFrame,
    ) -> str:
        """Save visualization results and metadata."""
        logger.info("💾 Saving visualization results...")

        # Create output directory
        mode = "local" if "local" in self.results_path else "prod"
        timestamp = generate_current_datetime_str()
        output_dir = Path("visualization/results/topic_proportions") / mode / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save figures
        stacked_path = output_dir / "topic_proportions_stacked.png"
        stacked_fig.savefig(
            stacked_path, dpi=300, bbox_inches="tight", facecolor="white"
        )
        logger.info(f"📊 Stacked bar chart saved to: {stacked_path}")

        grouped_path = output_dir / "topic_proportions_grouped.png"
        grouped_fig.savefig(
            grouped_path, dpi=300, bbox_inches="tight", facecolor="white"
        )
        logger.info(f"📊 Grouped bar chart saved to: {grouped_path}")

        # Save high-resolution versions
        stacked_path_hires = output_dir / "topic_proportions_stacked_hires.png"
        stacked_fig.savefig(
            stacked_path_hires, dpi=600, bbox_inches="tight", facecolor="white"
        )
        logger.info(f"📊 High-res stacked chart saved to: {stacked_path_hires}")

        grouped_path_hires = output_dir / "topic_proportions_grouped_hires.png"
        grouped_fig.savefig(
            grouped_path_hires, dpi=600, bbox_inches="tight", facecolor="white"
        )
        logger.info(f"📊 High-res grouped chart saved to: {grouped_path_hires}")

        # Save data
        data_path = output_dir / "topic_proportions_data.csv"
        proportions_df.to_csv(data_path, index=False)
        logger.info(f"📊 Data saved to: {data_path}")

        # Save summary statistics
        summary_stats = (
            proportions_df.groupby("slice_name")
            .agg({"total_documents": "first", "count": "sum"})
            .reset_index()
        )
        summary_stats["unique_topics"] = (
            proportions_df.groupby("slice_name")["topic_id"].nunique().values
        )

        summary_path = output_dir / "slice_summary_stats.csv"
        summary_stats.to_csv(summary_path, index=False)
        logger.info(f"📊 Summary statistics saved to: {summary_path}")

        # Save metadata
        metadata = {
            "visualization_type": "topic_proportions",
            "model_path": self.model_path,
            "metadata_path": self.metadata_path,
            "results_path": self.results_path,
            "figure_parameters": self.figure_params,
            "n_slices": len(proportions_df["slice_name"].unique()),
            "n_topics": len(proportions_df["topic_id"].unique()),
            "total_documents": proportions_df["total_documents"].sum(),
            "slice_summary": proportions_df.groupby("slice_name")["total_documents"]
            .first()
            .to_dict(),
            "topic_summary": proportions_df.groupby("topic_id")["count"]
            .sum()
            .to_dict(),
            "creation_timestamp": timestamp,
            "color_palette": self.color_palette,
            "outliers_excluded": True,
            "note": "Outliers (topic -1) are excluded from visualization for clarity",
        }

        metadata_path = output_dir / "visualization_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
        logger.info(f"📋 Metadata saved to: {metadata_path}")

        return str(output_dir)

    def run_visualization(
        self, slice_configs: List[Dict[str, Union[str, List[str]]]]
    ) -> str:
        """Run the complete topic proportion visualization pipeline."""
        start_time = time.time()

        logger.info("🚀 Starting topic proportion visualization pipeline...")

        # Load model and results
        self.load_model_and_results()

        # Compute topic proportions
        proportions_df = self.compute_topic_proportions_by_slice(slice_configs)

        if len(proportions_df) == 0:
            raise ValueError(
                "No data found for visualization. Check slice configurations."
            )

        # Create visualizations
        stacked_fig, stacked_ax = self.create_stacked_bar_chart(proportions_df)
        grouped_fig, grouped_ax = self.create_grouped_bar_chart(proportions_df)

        # Save results
        output_dir = self.save_results(stacked_fig, grouped_fig, proportions_df)

        # Close figures to free memory
        plt.close(stacked_fig)
        plt.close(grouped_fig)

        elapsed_time = time.time() - start_time
        logger.info(
            f"🎉 Topic proportion visualization completed in {elapsed_time:.2f} seconds"
        )
        logger.info(f"📁 Results saved to: {output_dir}")

        return output_dir


def create_default_slice_configs() -> List[Dict[str, Union[str, List[str]]]]:
    """Create default slice configurations for visualization."""
    return [
        # Overall visualization
        {"condition": None, "date_range": None, "title_suffix": "Overall"},
        # By condition
        {
            "condition": "reverse_chronological",
            "date_range": None,
            "title_suffix": "Reverse Chronological",
        },
        {
            "condition": "representative_diversification",
            "date_range": None,
            "title_suffix": "Representative Diversification",
        },
        {"condition": "engagement", "date_range": None, "title_suffix": "Engagement"},
        # By time period (assuming 4-week study)
        {
            "condition": None,
            "date_range": ["2024-10-01", "2024-10-07"],
            "title_suffix": "Week 1",
        },
        {
            "condition": None,
            "date_range": ["2024-10-08", "2024-10-14"],
            "title_suffix": "Week 2",
        },
        {
            "condition": None,
            "date_range": ["2024-10-15", "2024-10-21"],
            "title_suffix": "Week 3",
        },
        {
            "condition": None,
            "date_range": ["2024-10-22", "2024-10-28"],
            "title_suffix": "Week 4",
        },
        # Pre/post election
        {
            "condition": None,
            "date_range": ["2024-10-01", "2024-11-04"],
            "title_suffix": "Pre-Election",
        },
        {
            "condition": None,
            "date_range": ["2024-11-05", "2024-11-28"],
            "title_suffix": "Post-Election",
        },
        # Condition × Time combinations
        {
            "condition": "reverse_chronological",
            "date_range": ["2024-10-01", "2024-10-07"],
            "title_suffix": "Reverse Chronological Week 1",
        },
        {
            "condition": "representative_diversification",
            "date_range": ["2024-10-01", "2024-10-07"],
            "title_suffix": "Representative Diversification Week 1",
        },
        {
            "condition": "reverse_chronological",
            "date_range": ["2024-11-05", "2024-11-28"],
            "title_suffix": "Reverse Chronological Post-Election",
        },
        {
            "condition": "representative_diversification",
            "date_range": ["2024-11-05", "2024-11-28"],
            "title_suffix": "Representative Diversification Post-Election",
        },
    ]


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate topic proportion visualizations for topic modeling results"
    )
    parser.add_argument(
        "--model-path",
        type=str,
        required=True,
        help="Path to trained BERTopic model directory",
    )
    parser.add_argument(
        "--metadata-path",
        type=str,
        required=True,
        help="Path to model metadata JSON file",
    )
    parser.add_argument(
        "--results-path",
        type=str,
        required=True,
        help="Path to inference results directory",
    )
    parser.add_argument(
        "--slice-configs",
        type=str,
        help="Path to JSON file with custom slice configurations",
    )

    args = parser.parse_args()

    # Load slice configurations
    if args.slice_configs:
        with open(args.slice_configs, "r") as f:
            slice_configs = json.load(f)
    else:
        slice_configs = create_default_slice_configs()

    # Create visualizer
    visualizer = TopicProportionVisualizer(
        model_path=args.model_path,
        metadata_path=args.metadata_path,
        results_path=args.results_path,
    )

    # Run visualization
    try:
        output_dir = visualizer.run_visualization(slice_configs)
        print("✅ Topic proportion visualization completed successfully!")
        print(f"📁 Results saved to: {output_dir}")
    except Exception as e:
        logger.error(f"❌ Topic proportion visualization failed: {e}")
        raise


if __name__ == "__main__":
    main()
