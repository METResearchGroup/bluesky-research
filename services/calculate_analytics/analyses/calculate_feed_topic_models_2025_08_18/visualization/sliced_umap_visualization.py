#!/usr/bin/env python3
"""
Sliced UMAP Visualization for Topic Modeling Results

This script generates publication-ready UMAP scatter plots for specific slices of data
(e.g., by condition, date range, or combinations thereof). It uses pre-exported data
structures to enable fast, flexible visualization of topic model results.

Author: AI Agent implementing Scientific Visualization Specialist
Date: 2025-09-16
"""

import json
import os
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from umap import UMAP

from lib.log.logger import get_logger

logger = get_logger(__name__)


class SlicedUMAPVisualizer:
    """
    Generates sliced UMAP visualizations for topic modeling results.

    Uses pre-exported data structures to create targeted visualizations
    for specific conditions, date ranges, or combinations thereof.
    """

    def __init__(self, model_path: str, metadata_path: str, exported_data_path: str):
        """
        Initialize sliced UMAP visualizer.

        Args:
            model_path: Path to trained BERTopic model directory
            metadata_path: Path to model metadata JSON file
            exported_data_path: Path to exported data directory
        """
        self.model_path = model_path
        self.metadata_path = metadata_path
        self.exported_data_path = exported_data_path

        # UMAP parameters optimized for topic modeling visualization
        self.umap_params = {
            "n_neighbors": 15,
            "min_dist": 0.1,
            "n_components": 2,
            "metric": "cosine",
            "random_state": 42,
        }

        # Scientific visualization settings
        self.figure_params = {"figsize": (12, 10), "dpi": 300, "facecolor": "white"}

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
        ]

        # Data structures
        self.documents_df = None
        self.uri_doc_map = None
        self.date_condition_uris_map = None
        self.doc_topic_assignments = None
        self.metadata = None
        self.bertopic = None
        self.embedding_model = None
        self.topic_names = None

    def load_exported_data(self) -> None:
        """Load the exported data structures."""
        logger.info("📂 Loading exported data structures...")

        # Load core data structures
        self.documents_df = pd.read_parquet(
            f"{self.exported_data_path}/documents_df.parquet"
        )
        self.uri_doc_map = pd.read_parquet(
            f"{self.exported_data_path}/uri_doc_map.parquet"
        )

        with open(f"{self.exported_data_path}/date_condition_uris_map.json") as f:
            # Convert lists back to sets for easier manipulation
            raw_map = json.load(f)
            self.date_condition_uris_map = {}
            for date, condition_map in raw_map.items():
                self.date_condition_uris_map[date] = {}
                for condition, uris in condition_map.items():
                    self.date_condition_uris_map[date][condition] = set(uris)

        with open(f"{self.exported_data_path}/metadata.json") as f:
            self.metadata = json.load(f)

        logger.info("✅ Loaded exported data:")
        logger.info(f"   📄 Documents: {len(self.documents_df)}")
        logger.info(f"   🔗 URI mappings: {len(self.uri_doc_map)}")
        logger.info(f"   📅 Dates: {len(self.date_condition_uris_map)}")
        logger.info(f"   🏷️ Conditions: {self.metadata.get('conditions', [])}")

    def load_model_and_assignments(self) -> None:
        """Load trained model and topic assignments."""
        logger.info("🤖 Loading trained model and topic assignments...")

        # Load metadata first
        with open(self.metadata_path, "r") as f:
            self.metadata = json.load(f)

        # Load BERTopic model
        from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper

        self.bertopic = BERTopicWrapper.load_model(self.model_path)

        # Initialize embedding model
        self._initialize_embedding_model()

        # Load topic assignments if available
        self._load_topic_assignments()

        # Load topic names if available
        self._load_topic_names()

        logger.info("✅ Model and assignments loaded successfully")

    def _initialize_embedding_model(self) -> None:
        """Initialize embedding model from metadata."""
        from sentence_transformers import SentenceTransformer

        # Get embedding model configuration from metadata
        embedding_config = self.metadata.get("embedding_model", {})
        model_name = embedding_config.get("name", "all-MiniLM-L6-v2")
        device = embedding_config.get("device", "mps")

        logger.info(f"🔮 Initializing embedding model: {model_name} on {device}")
        self.embedding_model = SentenceTransformer(model_name, device=device)

    def _load_topic_assignments(self) -> None:
        """Load topic assignments from inference results."""
        # Look for topic assignments in the exported data or inference results
        assignments_file = f"{self.exported_data_path}/doc_topic_assignments.csv"

        if os.path.exists(assignments_file):
            logger.info(f"📊 Loading topic assignments from: {assignments_file}")
            self.doc_topic_assignments = pd.read_csv(assignments_file)
        else:
            logger.warning(
                "⚠️ No topic assignments found. Run inference first to generate assignments."
            )
            self.doc_topic_assignments = None

    def _load_topic_names(self) -> None:
        """Load generated topic names if available."""
        # Try to find topic names file in the model directory
        model_dir = Path(self.model_path).parent
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

    def create_sliced_visualization(
        self,
        condition: Optional[str] = None,
        date_range: Optional[List[str]] = None,
        output_path: Optional[str] = None,
        title_suffix: str = "",
    ) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create UMAP visualization for a specific slice of data.

        Args:
            condition: Specific condition to filter by (e.g., "control", "treatment")
            date_range: List of dates to include (e.g., ["2024-01-15", "2024-01-21"])
            output_path: Path to save the visualization
            title_suffix: Additional text to append to the title

        Returns:
            tuple: (figure, axes) for the created visualization
        """
        logger.info("🎨 Creating sliced UMAP visualization...")
        logger.info(f"   🏷️ Condition: {condition or 'all'}")
        logger.info(f"   📅 Date range: {date_range or 'all'}")

        # Get URIs for the slice
        slice_uris = self._get_uris_for_slice(condition, date_range)
        logger.info(f"   🔗 Found {len(slice_uris)} URIs for slice")

        if len(slice_uris) == 0:
            raise ValueError("No URIs found for the specified slice")

        # Get doc_ids for the slice
        slice_doc_ids = self.uri_doc_map[self.uri_doc_map["uri"].isin(slice_uris)][
            "doc_id"
        ].unique()

        # Filter data for the slice
        slice_documents_df = self.documents_df[
            self.documents_df["doc_id"].isin(slice_doc_ids)
        ]

        if self.doc_topic_assignments is not None:
            slice_topic_assignments = self.doc_topic_assignments[
                self.doc_topic_assignments["doc_id"].isin(slice_doc_ids)
            ]
        else:
            logger.warning(
                "⚠️ No topic assignments available, will generate embeddings only"
            )
            slice_topic_assignments = None

        logger.info(f"   📄 Processing {len(slice_documents_df)} documents")

        # Generate embeddings
        embeddings = self.embedding_model.encode(
            slice_documents_df["text"].tolist(), batch_size=32, show_progress_bar=True
        )

        # Compute UMAP
        umap_embeddings = self._compute_umap_embeddings(embeddings)

        # Create visualization
        if slice_topic_assignments is not None:
            fig, ax = self._create_topic_colored_visualization(
                umap_embeddings,
                slice_topic_assignments,
                condition,
                date_range,
                title_suffix,
            )
        else:
            fig, ax = self._create_simple_visualization(
                umap_embeddings, condition, date_range, title_suffix
            )

        # Save if output path provided
        if output_path:
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            logger.info(f"💾 Visualization saved to: {output_path}")

        return fig, ax

    def _get_uris_for_slice(
        self, condition: Optional[str], date_range: Optional[List[str]]
    ) -> set:
        """Get URIs for a specific condition and date range."""
        slice_uris = set()

        # Determine which dates to process
        if date_range:
            dates_to_process = [
                d for d in date_range if d in self.date_condition_uris_map
            ]
        else:
            dates_to_process = list(self.date_condition_uris_map.keys())

        # Collect URIs
        for date in dates_to_process:
            if condition:
                # Specific condition
                if condition in self.date_condition_uris_map[date]:
                    slice_uris.update(self.date_condition_uris_map[date][condition])
            else:
                # All conditions
                for condition_uris in self.date_condition_uris_map[date].values():
                    slice_uris.update(condition_uris)

        return slice_uris

    def _compute_umap_embeddings(self, embeddings: np.ndarray) -> np.ndarray:
        """Compute 2D UMAP embeddings from document embeddings."""
        logger.info("🗺️ Computing UMAP embeddings...")

        # Create UMAP reducer
        reducer = UMAP(**self.umap_params)

        # Fit and transform embeddings
        umap_embeddings = reducer.fit_transform(embeddings)

        logger.info(f"✅ UMAP embeddings computed: {umap_embeddings.shape}")
        return umap_embeddings

    def _create_topic_colored_visualization(
        self,
        umap_embeddings: np.ndarray,
        topic_assignments: pd.DataFrame,
        condition: Optional[str],
        date_range: Optional[List[str]],
        title_suffix: str,
    ) -> Tuple[plt.Figure, plt.Axes]:
        """Create UMAP visualization colored by topics."""
        logger.info("🎨 Creating topic-colored UMAP visualization...")

        # Get unique topics (exclude outliers)
        unique_topics = sorted(topic_assignments["topic_id"].unique())
        unique_topics = [t for t in unique_topics if t != -1]  # Exclude outliers

        n_topics = len(unique_topics)
        logger.info(f"📊 Visualizing {n_topics} topics (outliers excluded)")

        # Create color map
        if n_topics <= len(self.color_palette):
            colors = self.color_palette[:n_topics]
        else:
            colors = plt.cm.tab20(np.linspace(0, 1, n_topics))

        # Create figure and axis
        fig, ax = plt.subplots(
            figsize=self.figure_params["figsize"],
            dpi=self.figure_params["dpi"],
            facecolor=self.figure_params["facecolor"],
        )

        # Plot each topic with different colors
        for i, topic_id in enumerate(unique_topics):
            # Get indices for this topic
            topic_mask = topic_assignments["topic_id"] == topic_id

            # Get topic label
            if self.topic_names and topic_id in self.topic_names:
                topic_label = self.topic_names[topic_id]
            else:
                topic_label = f"Topic {topic_id}"

            # Plot points for this topic
            ax.scatter(
                umap_embeddings[topic_mask, 0],
                umap_embeddings[topic_mask, 1],
                c=[colors[i]],
                label=topic_label,
                alpha=0.7,
                s=20,
                edgecolors="none",
            )

        # Apply styling and title
        self._apply_scientific_styling(fig, ax, unique_topics)
        self._set_slice_title(ax, condition, date_range, title_suffix)

        logger.info("✅ Topic-colored UMAP visualization created")
        return fig, ax

    def _create_simple_visualization(
        self,
        umap_embeddings: np.ndarray,
        condition: Optional[str],
        date_range: Optional[List[str]],
        title_suffix: str,
    ) -> Tuple[plt.Figure, plt.Axes]:
        """Create simple UMAP visualization without topic coloring."""
        logger.info("🎨 Creating simple UMAP visualization...")

        # Create figure and axis
        fig, ax = plt.subplots(
            figsize=self.figure_params["figsize"],
            dpi=self.figure_params["dpi"],
            facecolor=self.figure_params["facecolor"],
        )

        # Plot all points in a single color
        ax.scatter(
            umap_embeddings[:, 0],
            umap_embeddings[:, 1],
            c=self.color_palette[0],
            alpha=0.7,
            s=20,
            edgecolors="none",
        )

        # Apply styling and title
        self._apply_scientific_styling(fig, ax, [])
        self._set_slice_title(ax, condition, date_range, title_suffix)

        logger.info("✅ Simple UMAP visualization created")
        return fig, ax

    def _apply_scientific_styling(
        self, fig: plt.Figure, ax: plt.Axes, topics: List[int]
    ) -> None:
        """Apply scientific visualization styling."""
        # Set labels
        ax.set_xlabel("UMAP Dimension 1", fontsize=12, fontweight="bold")
        ax.set_ylabel("UMAP Dimension 2", fontsize=12, fontweight="bold")

        # Add legend if we have topics
        if topics:
            ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=10)

        # Grid
        ax.grid(True, alpha=0.3)

        # Remove top and right spines
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Adjust layout
        fig.tight_layout()

    def _set_slice_title(
        self,
        ax: plt.Axes,
        condition: Optional[str],
        date_range: Optional[List[str]],
        title_suffix: str,
    ) -> None:
        """Set title for the slice visualization."""
        title_parts = ["UMAP Visualization"]

        if condition:
            title_parts.append(f"{condition.title()} Condition")

        if date_range:
            if len(date_range) == 1:
                title_parts.append(f"({date_range[0]})")
            else:
                title_parts.append(f"({date_range[0]} to {date_range[1]})")

        if title_suffix:
            title_parts.append(title_suffix)

        title = " - ".join(title_parts)
        ax.set_title(title, fontsize=14, fontweight="bold", pad=20)

    def create_multiple_slice_visualizations(
        self,
        slice_configs: List[Dict[str, Union[str, List[str]]]],
        output_dir: str,
    ) -> List[Tuple[plt.Figure, plt.Axes]]:
        """
        Create multiple slice visualizations.

        Args:
            slice_configs: List of dictionaries with 'condition', 'date_range', 'title_suffix' keys
            output_dir: Directory to save visualizations

        Returns:
            List of (figure, axes) tuples
        """
        os.makedirs(output_dir, exist_ok=True)
        visualizations = []

        for i, config in enumerate(slice_configs):
            logger.info(
                f"🎨 Creating visualization {i+1}/{len(slice_configs)}: {config}"
            )

            condition = config.get("condition")
            date_range = config.get("date_range")
            title_suffix = config.get("title_suffix", "")

            # Create visualization
            fig, ax = self.create_sliced_visualization(
                condition=condition,
                date_range=date_range,
                title_suffix=title_suffix,
            )

            # Save with descriptive filename
            filename_parts = []
            if condition:
                filename_parts.append(condition)
            if date_range:
                if len(date_range) == 1:
                    filename_parts.append(date_range[0])
                else:
                    filename_parts.append(f"{date_range[0]}_to_{date_range[1]}")

            filename = "_".join(filename_parts) if filename_parts else f"slice_{i+1}"
            output_path = f"{output_dir}/{filename}_umap.png"

            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            logger.info(f"💾 Saved: {output_path}")

            visualizations.append((fig, ax))
            plt.close(fig)  # Close to free memory

        return visualizations


def main():
    """Example usage of SlicedUMAPVisualizer."""
    import argparse

    parser = argparse.ArgumentParser(description="Create sliced UMAP visualizations")
    parser.add_argument(
        "--model_path", required=True, help="Path to trained BERTopic model"
    )
    parser.add_argument("--metadata_path", required=True, help="Path to model metadata")
    parser.add_argument(
        "--exported_data_path", required=True, help="Path to exported data"
    )
    parser.add_argument("--output_dir", help="Output directory for visualizations")
    parser.add_argument("--condition", help="Specific condition to visualize")
    parser.add_argument("--date_range", nargs="+", help="Date range to visualize")

    args = parser.parse_args()

    # Initialize visualizer
    visualizer = SlicedUMAPVisualizer(
        model_path=args.model_path,
        metadata_path=args.metadata_path,
        exported_data_path=args.exported_data_path,
    )

    # Load data
    visualizer.load_exported_data()
    visualizer.load_model_and_assignments()

    # Create visualization
    fig, ax = visualizer.create_sliced_visualization(
        condition=args.condition,
        date_range=args.date_range,
        output_path=args.output_dir,
    )

    plt.show()


if __name__ == "__main__":
    main()
