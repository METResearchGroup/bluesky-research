#!/usr/bin/env python3
"""
Topic Proportion Visualization for Topic Modeling Results

This script generates structured topic proportion visualizations with:
- Consistent top 10 topics across all visualizations
- Separate subfolders for each visualization type
- Pre/post election analysis with correct date (2024-11-05)
- Condition-specific analysis

Author: AI Agent implementing Scientific Visualization Specialist
Date: 2025-09-17
"""

import argparse
import json
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)


class TopicProportionVisualizer:
    """
    Generates structured topic proportion visualizations for topic modeling results.

    Creates publication-ready charts with consistent top 10 topics across all
    visualization types, organized in separate subfolders.
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
        self.election_date = "2024-11-05"  # Correct election date

        # Scientific visualization settings
        self.figure_params = {"figsize": (12, 8), "dpi": 300, "facecolor": "white"}

        # Colorblind-friendly color palette for top 10 topics
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
        ]

        self.metadata = None
        self.topic_assignments = None
        self.topic_names = None
        self.documents_df = None
        self.uri_doc_map = None
        self.date_condition_uris_map = None
        self.top_10_topics = None

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

        # Determine top 10 topics overall
        self._determine_top_10_topics()
        logger.info("✅ Top 10 topics determined")

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

    def _determine_top_10_topics(self) -> None:
        """Determine the top 10 topics overall for consistent visualization."""
        logger.info("🔍 Determining top 10 topics overall...")

        # Count topics across all documents
        if self.topic_assignments is None:
            raise ValueError("Topic assignments not loaded")

        topic_counts = self.topic_assignments["topic_id"].value_counts()

        # Get top 10 topics
        self.top_10_topics = topic_counts.head(10).index.tolist()

        logger.info(f"📊 Top 10 topics: {self.top_10_topics}")

        # Log topic names if available
        for i, topic_id in enumerate(self.top_10_topics, 1):
            topic_name = (
                self.topic_names.get(topic_id, f"Topic {topic_id}")
                if self.topic_names
                else f"Topic {topic_id}"
            )
            count = topic_counts[topic_id]
            logger.info(
                f"   {i:2d}. Topic {topic_id}: {topic_name} ({count} documents)"
            )

    def _get_uris_for_slice(
        self, condition: Optional[str], date_range: Optional[List[str]]
    ) -> set:
        """Get URIs for a specific condition and date range."""
        if not self.date_condition_uris_map:
            logger.warning("⚠️ No date-condition mapping available, returning all URIs")
            return (
                set(self.uri_doc_map["uri"].unique())
                if self.uri_doc_map is not None
                else set()
            )

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

    def _get_uris_for_election_period(self, period: str) -> set:
        """Get URIs for pre/post election period."""
        if not self.date_condition_uris_map:
            logger.warning("⚠️ No date-condition mapping available, returning all URIs")
            return (
                set(self.uri_doc_map["uri"].unique())
                if self.uri_doc_map is not None
                else set()
            )

        slice_uris = set()

        for date, condition_map in self.date_condition_uris_map.items():
            if period == "pre" and date <= self.election_date:
                for condition_uris in condition_map.values():
                    slice_uris.update(condition_uris)
            elif period == "post" and date > self.election_date:
                for condition_uris in condition_map.values():
                    slice_uris.update(condition_uris)

        return slice_uris

    def compute_topic_proportions_for_slice(
        self, slice_name: str, slice_uris: set
    ) -> Tuple[Dict[int, float], int]:
        """
        Compute topic proportions for a specific slice using top 10 topics.

        Args:
            slice_name: Name of the slice
            slice_uris: URIs for this slice

        Returns:
            Tuple of (proportions dict, sample size)
        """
        logger.info(f"   📊 Computing proportions for: {slice_name}")

        if len(slice_uris) == 0:
            logger.warning(f"   ⚠️ No URIs found for slice: {slice_name}")
            return {topic_id: 0.0 for topic_id in (self.top_10_topics or [])}, 0

        # Get doc_ids for this slice
        if self.uri_doc_map is None or self.topic_assignments is None:
            raise ValueError("Required data not loaded")

        slice_doc_ids = self.uri_doc_map[self.uri_doc_map["uri"].isin(slice_uris)][
            "doc_id"
        ].unique()

        # Filter topic assignments for this slice
        slice_assignments = self.topic_assignments[
            self.topic_assignments["doc_id"].isin(slice_doc_ids)
        ]

        if len(slice_assignments) == 0:
            logger.warning(f"   ⚠️ No topic assignments found for slice: {slice_name}")
            return {topic_id: 0.0 for topic_id in (self.top_10_topics or [])}, 0

        # Compute topic proportions
        topic_counts = slice_assignments["topic_id"].value_counts()
        total_documents = len(slice_assignments)

        # Create proportions dict for top 10 topics
        proportions = {}
        for topic_id in self.top_10_topics or []:
            count = topic_counts.get(topic_id, 0)
            proportions[topic_id] = count / total_documents

        logger.info(f"   ✅ Processed {total_documents} documents for {slice_name}")
        return proportions, total_documents

    def create_topic_proportion_chart(
        self,
        proportions_data: Dict[str, Dict[int, float]],
        title: str,
        output_path: str,
        sample_sizes: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Create a topic proportion chart.

        Args:
            proportions_data: Dictionary mapping slice names to topic proportions
            title: Chart title
            output_path: Path to save the chart
            sample_sizes: Optional dictionary mapping slice names to sample sizes
        """
        logger.info(f"🎨 Creating chart: {title}")

        # Prepare data for plotting
        slice_names = list(proportions_data.keys())
        n_slices = len(slice_names)

        # Create figure
        fig, ax = plt.subplots(
            figsize=self.figure_params["figsize"],
            dpi=self.figure_params["dpi"],
            facecolor=self.figure_params["facecolor"],
        )

        # Create stacked bar chart
        bottom = np.zeros(n_slices)

        if self.top_10_topics is None:
            raise ValueError("Top 10 topics not determined")

        for i, topic_id in enumerate(self.top_10_topics):
            topic_name = (
                self.topic_names.get(topic_id, f"Topic {topic_id}")
                if self.topic_names
                else f"Topic {topic_id}"
            )
            proportions = [
                proportions_data[slice_name][topic_id] for slice_name in slice_names
            ]

            ax.bar(
                slice_names,
                proportions,
                bottom=bottom,
                label=topic_name,
                color=self.color_palette[i],
                alpha=0.8,
                edgecolor="white",
                linewidth=0.5,
            )
            bottom += proportions

        # Add percentage labels for "Political Opinions and Perspectives" (first topic)
        if self.top_10_topics:
            political_topic_id = self.top_10_topics[
                0
            ]  # Assuming this is the political topic
            political_topic_name = (
                self.topic_names.get(political_topic_id, f"Topic {political_topic_id}")
                if self.topic_names
                else f"Topic {political_topic_id}"
            )

            # Check if this is the political topic
            if (
                "political" in political_topic_name.lower()
                or "opinion" in political_topic_name.lower()
            ):
                political_proportions = [
                    proportions_data[slice_name][political_topic_id]
                    for slice_name in slice_names
                ]

                # Add percentage labels on top of the political topic bars
                for i, (slice_name, proportion) in enumerate(
                    zip(slice_names, political_proportions)
                ):
                    if proportion > 0.05:  # Only show label if proportion is > 5%
                        ax.text(
                            i,
                            proportion
                            / 2,  # Position at middle of the political topic bar
                            f"{proportion*100:.1f}%",
                            ha="center",
                            va="center",
                            fontsize=10,
                            fontweight="bold",
                            color="white",
                        )

        # Apply styling
        self._apply_chart_styling(fig, ax, title, slice_names, sample_sizes)

        # Save chart
        fig.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
        logger.info(f"💾 Chart saved to: {output_path}")

        # Close figure to free memory
        plt.close(fig)

    def _apply_chart_styling(
        self,
        fig: plt.Figure,
        ax: plt.Axes,
        title: str,
        slice_names: List[str],
        sample_sizes: Optional[Dict[str, int]] = None,
    ) -> None:
        """Apply scientific styling to chart."""
        # Fix title capitalization
        title = self._fix_title_capitalization(title)

        # Add sample sizes to title if provided
        if sample_sizes:
            sample_info = " | ".join(
                [f"{name}: n={size:,}" for name, size in sample_sizes.items()]
            )
            title = f"{title}\n({sample_info})"

        # Set title
        ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Set axis labels
        ax.set_xlabel("Category", fontsize=12, fontweight="bold")
        ax.set_ylabel("Proportion", fontsize=12, fontweight="bold")

        # Set y-axis to percentage
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x*100:.0f}%"))

        # Set axis properties
        ax.tick_params(axis="both", which="major", labelsize=10)
        ax.grid(True, alpha=0.3, linestyle="-", linewidth=0.5, axis="y")

        # Set legend
        ax.legend(
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            fontsize=9,
            frameon=True,
            fancybox=True,
            shadow=True,
            title="Top 10 Topics",
        )

        # Rotate x-axis labels if needed
        if len(slice_names) > 5:
            plt.xticks(rotation=45, ha="right")

        # Tight layout
        plt.tight_layout()

    def _fix_title_capitalization(self, title: str) -> str:
        """Fix title capitalization for presentation."""
        # Common words that should be capitalized
        words_to_capitalize = {
            "engagement": "Engagement",
            "condition": "Condition",
            "election": "Election",
            "date": "Date",
            "topic": "Topic",
            "distribution": "Distribution",
            "overall": "Overall",
            "political": "Political",
            "opinions": "Opinions",
            "perspectives": "Perspectives",
        }

        # Split title and fix capitalization
        words = title.split()
        fixed_words = []

        for word in words:
            # Remove punctuation for checking
            clean_word = word.strip("()[]{}.,!?;:")
            if clean_word.lower() in words_to_capitalize:
                # Replace the word while preserving punctuation
                if word == clean_word:
                    fixed_words.append(words_to_capitalize[clean_word.lower()])
                else:
                    # Handle punctuation
                    punctuation = word[len(clean_word) :]
                    fixed_words.append(
                        words_to_capitalize[clean_word.lower()] + punctuation
                    )
            else:
                fixed_words.append(word)

        return " ".join(fixed_words)

    def create_binary_topic_chart(
        self,
        proportions_data: Dict[str, Dict[int, float]],
        title: str,
        output_path: str,
        sample_sizes: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Create a binary chart showing Political Opinions vs Other Categories.

        Args:
            proportions_data: Dictionary mapping slice names to topic proportions
            title: Chart title
            output_path: Path to save the chart
            sample_sizes: Optional dictionary mapping slice names to sample sizes
        """
        logger.info(f"🎨 Creating binary chart: {title}")

        # Prepare data for plotting
        slice_names = list(proportions_data.keys())

        # Create figure
        fig, ax = plt.subplots(
            figsize=self.figure_params["figsize"],
            dpi=self.figure_params["dpi"],
            facecolor=self.figure_params["facecolor"],
        )

        if self.top_10_topics is None:
            raise ValueError("Top 10 topics not determined")

        # Find political topic (assuming it's the first topic or contains "political")
        political_topic_id = None
        for topic_id in self.top_10_topics:
            topic_name = (
                self.topic_names.get(topic_id, f"Topic {topic_id}")
                if self.topic_names
                else f"Topic {topic_id}"
            )
            if "political" in topic_name.lower() or "opinion" in topic_name.lower():
                political_topic_id = topic_id
                break

        if political_topic_id is None:
            # Fallback to first topic if no political topic found
            political_topic_id = self.top_10_topics[0]

        # Calculate binary proportions
        political_proportions = []
        other_proportions = []

        for slice_name in slice_names:
            political_prop = proportions_data[slice_name].get(political_topic_id, 0.0)
            other_prop = 1.0 - political_prop  # All other topics combined

            political_proportions.append(political_prop)
            other_proportions.append(other_prop)

        # Create stacked bar chart
        ax.bar(
            slice_names,
            political_proportions,
            label="Political Opinions and Perspectives",
            color=self.color_palette[0],  # Blue
            alpha=0.8,
            edgecolor="white",
            linewidth=0.5,
        )

        ax.bar(
            slice_names,
            other_proportions,
            bottom=political_proportions,
            label="Other Categories",
            color="#cccccc",  # Light gray
            alpha=0.8,
            edgecolor="white",
            linewidth=0.5,
        )

        # Add percentage labels for political topic
        for i, (slice_name, proportion) in enumerate(
            zip(slice_names, political_proportions)
        ):
            if proportion > 0.05:  # Only show label if proportion is > 5%
                ax.text(
                    i,
                    proportion / 2,  # Position at middle of the political topic bar
                    f"{proportion*100:.1f}%",
                    ha="center",
                    va="center",
                    fontsize=12,
                    fontweight="bold",
                    color="white",
                )

        # Apply styling
        self._apply_chart_styling(fig, ax, title, slice_names, sample_sizes)

        # Save chart
        fig.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="white")
        logger.info(f"💾 Binary chart saved to: {output_path}")

        # Close figure to free memory
        plt.close(fig)

    def create_overall_visualization(self, output_dir: Path) -> None:
        """Create overall topic proportion visualization."""
        logger.info("📊 Creating overall visualization...")

        # Get all URIs
        all_uris = self._get_uris_for_slice(None, None)

        # Compute proportions and sample size
        proportions, sample_size = self.compute_topic_proportions_for_slice(
            "Overall", all_uris
        )

        # Create chart
        proportions_data = {"Overall": proportions}
        sample_sizes = {"Overall": sample_size}
        title = "Topic Distribution - Overall"
        output_path = output_dir / "topic_proportions.png"

        self.create_topic_proportion_chart(
            proportions_data, title, str(output_path), sample_sizes
        )

        # Create binary chart
        binary_title = "Political Opinions vs Other Categories - Overall"
        binary_output_path = output_dir / "binary_topic_proportions.png"
        self.create_binary_topic_chart(
            proportions_data, binary_title, str(binary_output_path), sample_sizes
        )

    def create_condition_visualization(self, output_dir: Path) -> None:
        """Create condition-based topic proportion visualization."""
        logger.info("📊 Creating condition visualization...")

        # Get conditions from the data
        if self.date_condition_uris_map is None:
            raise ValueError("Date condition mapping not loaded")

        conditions = set()
        for date, condition_map in self.date_condition_uris_map.items():
            conditions.update(condition_map.keys())

        conditions = sorted(list(conditions))
        logger.info(f"📊 Found conditions: {conditions}")

        # Compute proportions for each condition
        proportions_data = {}
        sample_sizes = {}
        for condition in conditions:
            condition_uris = self._get_uris_for_slice(condition, None)
            proportions, sample_size = self.compute_topic_proportions_for_slice(
                condition, condition_uris
            )
            proportions_data[condition] = proportions
            sample_sizes[condition] = sample_size

        # Create chart
        title = "Topic Distribution by Condition"
        output_path = output_dir / "topic_proportions.png"

        self.create_topic_proportion_chart(
            proportions_data, title, str(output_path), sample_sizes
        )

        # Create binary chart
        binary_title = "Political Opinions vs Other Categories by Condition"
        binary_output_path = output_dir / "binary_topic_proportions.png"
        self.create_binary_topic_chart(
            proportions_data, binary_title, str(binary_output_path), sample_sizes
        )

    def create_election_date_visualization(self, output_dir: Path) -> None:
        """Create pre/post election topic proportion visualization."""
        logger.info("📊 Creating election date visualization...")

        # Compute proportions for pre/post election
        proportions_data = {}
        sample_sizes = {}

        # Pre-election
        pre_uris = self._get_uris_for_election_period("pre")
        pre_proportions, pre_sample_size = self.compute_topic_proportions_for_slice(
            "Before Election", pre_uris
        )
        proportions_data["Before Election"] = pre_proportions
        sample_sizes["Before Election"] = pre_sample_size

        # Post-election
        post_uris = self._get_uris_for_election_period("post")
        post_proportions, post_sample_size = self.compute_topic_proportions_for_slice(
            "After Election", post_uris
        )
        proportions_data["After Election"] = post_proportions
        sample_sizes["After Election"] = post_sample_size

        # Create chart
        title = f"Topic Distribution by Election Date (Election: {self.election_date})"
        output_path = output_dir / "topic_proportions.png"

        self.create_topic_proportion_chart(
            proportions_data, title, str(output_path), sample_sizes
        )

        # Create binary chart
        binary_title = f"Political Opinions vs Other Categories by Election Date (Election: {self.election_date})"
        binary_output_path = output_dir / "binary_topic_proportions.png"
        self.create_binary_topic_chart(
            proportions_data, binary_title, str(binary_output_path), sample_sizes
        )

    def create_election_date_by_condition_visualization(self, output_dir: Path) -> None:
        """Create pre/post election topic proportion visualization by condition."""
        logger.info("📊 Creating election date by condition visualization...")

        # Get conditions from the data
        if self.date_condition_uris_map is None:
            raise ValueError("Date condition mapping not loaded")

        conditions = set()
        for date, condition_map in self.date_condition_uris_map.items():
            conditions.update(condition_map.keys())

        conditions = sorted(list(conditions))

        # Create by_condition subdirectory
        by_condition_dir = output_dir / "by_condition"
        by_condition_dir.mkdir(exist_ok=True)

        # Create visualization for each condition
        for condition in conditions:
            logger.info(
                f"📊 Creating election date visualization for condition: {condition}"
            )

            # Compute proportions for pre/post election for this condition
            proportions_data = {}
            sample_sizes = {}

            # Pre-election for this condition
            pre_uris = set()
            for date, condition_map in self.date_condition_uris_map.items():
                if date <= self.election_date and condition in condition_map:
                    pre_uris.update(condition_map[condition])

            pre_proportions, pre_sample_size = self.compute_topic_proportions_for_slice(
                "Before Election", pre_uris
            )
            proportions_data["Before Election"] = pre_proportions
            sample_sizes["Before Election"] = pre_sample_size

            # Post-election for this condition
            post_uris = set()
            for date, condition_map in self.date_condition_uris_map.items():
                if date > self.election_date and condition in condition_map:
                    post_uris.update(condition_map[condition])

            post_proportions, post_sample_size = (
                self.compute_topic_proportions_for_slice("After Election", post_uris)
            )
            proportions_data["After Election"] = post_proportions
            sample_sizes["After Election"] = post_sample_size

            # Create chart
            title = f"Topic Distribution by Election Date - {condition}"
            output_path = by_condition_dir / f"{condition}.png"

            self.create_topic_proportion_chart(
                proportions_data, title, str(output_path), sample_sizes
            )

            # Create binary chart
            binary_title = (
                f"Political Opinions vs Other Categories by Election Date - {condition}"
            )
            binary_output_path = by_condition_dir / f"binary_{condition}.png"
            self.create_binary_topic_chart(
                proportions_data, binary_title, str(binary_output_path), sample_sizes
            )

    def save_metadata(self, output_dir: Path) -> None:
        """Save visualization metadata."""
        logger.info("💾 Saving metadata...")

        metadata = {
            "visualization_type": "topic_proportions",
            "model_path": self.model_path,
            "metadata_path": self.metadata_path,
            "results_path": self.results_path,
            "election_date": self.election_date,
            "top_10_topics": self.top_10_topics,
            "topic_names": {
                str(k): v
                for k, v in (self.topic_names.items() if self.topic_names else {})
            },
            "figure_parameters": self.figure_params,
            "total_documents": len(self.topic_assignments)
            if self.topic_assignments is not None
            else 0,
            "creation_timestamp": generate_current_datetime_str(),
            "color_palette": self.color_palette,
            "outliers_excluded": True,
            "note": "Outliers (topic -1) are excluded from visualization for clarity",
        }

        metadata_path = output_dir / "visualization_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
        logger.info(f"📋 Metadata saved to: {metadata_path}")

    def run_visualization(self) -> str:
        """Run the complete topic proportion visualization pipeline."""
        start_time = time.time()

        logger.info("🚀 Starting topic proportion visualization pipeline...")

        # Load model and results
        self.load_model_and_results()

        # Create output directory structure
        mode = "local" if "local" in self.results_path else "prod"
        timestamp = generate_current_datetime_str()
        base_output_dir = (
            Path("visualization/results/topic_proportions") / mode / timestamp
        )
        base_output_dir.mkdir(parents=True, exist_ok=True)

        # Create visualizations
        logger.info("📊 Creating structured visualizations...")

        # 1. Overall visualization
        overall_dir = base_output_dir / "overall"
        overall_dir.mkdir(exist_ok=True)
        self.create_overall_visualization(overall_dir)

        # 2. Condition visualization
        condition_dir = base_output_dir / "condition"
        condition_dir.mkdir(exist_ok=True)
        self.create_condition_visualization(condition_dir)

        # 3. Election date visualization
        election_dir = base_output_dir / "election_date"
        election_dir.mkdir(exist_ok=True)
        self.create_election_date_visualization(election_dir)

        # 4. Election date by condition visualization
        self.create_election_date_by_condition_visualization(election_dir)

        # Save metadata
        self.save_metadata(base_output_dir)

        elapsed_time = time.time() - start_time
        logger.info(
            f"🎉 Topic proportion visualization completed in {elapsed_time:.2f} seconds"
        )
        logger.info(f"📁 Results saved to: {base_output_dir}")

        return str(base_output_dir)


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate structured topic proportion visualizations for topic modeling results"
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

    args = parser.parse_args()

    # Create visualizer
    visualizer = TopicProportionVisualizer(
        model_path=args.model_path,
        metadata_path=args.metadata_path,
        results_path=args.results_path,
    )

    # Run visualization
    try:
        output_dir = visualizer.run_visualization()
        print("✅ Topic proportion visualization completed successfully!")
        print(f"📁 Results saved to: {output_dir}")
    except Exception as e:
        logger.error(f"❌ Topic proportion visualization failed: {e}")
        raise


if __name__ == "__main__":
    main()
