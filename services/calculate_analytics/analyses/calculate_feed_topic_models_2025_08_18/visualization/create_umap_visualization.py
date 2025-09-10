#!/usr/bin/env python3
"""
UMAP Visualization for Topic Modeling Results

This script generates publication-ready UMAP scatter plots showing documents
in semantic space, colored by their assigned topics. It processes inference
results and creates visualizations similar to standard topic modeling papers.

Author: AI Agent implementing Scientific Visualization Specialist
Date: 2025-09-10
"""

import argparse
import json
import time
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from umap import UMAP

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)


class UMAPVisualizer:
    """
    Generates UMAP visualizations for topic modeling results.

    Creates publication-ready scatter plots showing documents in 2D semantic space,
    colored by their assigned topics. Follows scientific visualization best practices
    including accessibility, typography, and layout standards.
    """

    def __init__(self, model_path: str, metadata_path: str, results_path: str):
        """
        Initialize UMAP visualizer.

        Args:
            model_path: Path to trained BERTopic model directory
            metadata_path: Path to model metadata JSON file
            results_path: Path to inference results directory
        """
        self.model_path = model_path
        self.metadata_path = metadata_path
        self.results_path = results_path

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

        self.bertopic = None
        self.metadata = None
        self.documents = None
        self.topic_assignments = None

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

        # Initialize embedding model separately to avoid UMAP conflicts
        self._initialize_embedding_model()
        logger.info("✅ Embedding model initialized successfully")

    def _load_inference_results(self) -> None:
        """Load documents and topic assignments from inference results."""
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

        # Load documents from the original data
        # We need to get the documents that correspond to these assignments
        self._load_documents_for_assignments()

    def _load_documents_for_assignments(self) -> None:
        """Load documents corresponding to the topic assignments."""
        # For now, we'll need to regenerate embeddings from the model
        # In a full implementation, we might store embeddings during inference
        logger.info("📝 Loading documents for embedding generation...")

        # Get document IDs from assignments
        doc_ids = self.topic_assignments["doc_id"].tolist()

        # Load the full dataset to get documents
        # This is a simplified approach - in practice, you might want to store
        # the documents during inference to avoid reloading
        from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.load.load_data import (
            DataLoader,
        )

        # Determine mode from results path
        mode = "local" if "local" in self.results_path else "prod"
        dataloader = DataLoader(mode, run_mode="inference")
        data_result = dataloader.load_data()

        # Extract documents_df from the tuple returned by load_data
        if mode == "local":
            # For local mode: (date_condition_uris_map, documents_df, uri_doc_map)
            _, documents_df, _ = data_result
        else:
            # For prod mode: (date_condition_uris_map, documents_df, uri_doc_map)
            _, documents_df, _ = data_result

        # Filter to only the documents we have assignments for
        documents_df = documents_df[documents_df["doc_id"].isin(doc_ids)]

        # Sort to match the order of topic assignments
        documents_df = documents_df.set_index("doc_id").reindex(doc_ids).reset_index()

        self.documents = documents_df["text"].tolist()
        logger.info(f"📝 Loaded {len(self.documents)} documents for visualization")

    def _initialize_embedding_model(self) -> None:
        """Initialize embedding model from metadata to avoid UMAP conflicts."""
        from sentence_transformers import SentenceTransformer

        # Get embedding model configuration from metadata
        embedding_config = self.metadata.get("embedding_model", {})
        model_name = embedding_config.get("name", "all-MiniLM-L6-v2")
        device = embedding_config.get("device", "mps")

        # Initialize SentenceTransformer directly
        self.embedding_model = SentenceTransformer(model_name, device=device)
        logger.info(f"🔮 Initialized embedding model: {model_name} on {device}")

    def generate_embeddings(self) -> np.ndarray:
        """Generate embeddings for documents using the trained model."""
        logger.info("🔮 Generating embeddings for documents...")

        # Use the initialized embedding model
        embeddings = self.embedding_model.encode(
            self.documents, batch_size=32, show_progress_bar=True
        )

        logger.info(f"✅ Generated embeddings: {embeddings.shape}")
        return embeddings

    def compute_umap_embeddings(self, embeddings: np.ndarray) -> np.ndarray:
        """Compute 2D UMAP embeddings from document embeddings."""
        logger.info("🗺️ Computing UMAP embeddings...")

        # Create UMAP reducer
        reducer = UMAP(**self.umap_params)

        # Fit and transform embeddings
        umap_embeddings = reducer.fit_transform(embeddings)

        logger.info(f"✅ UMAP embeddings computed: {umap_embeddings.shape}")
        return umap_embeddings

    def create_visualization(
        self, umap_embeddings: np.ndarray
    ) -> Tuple[plt.Figure, plt.Axes]:
        """Create UMAP scatter plot with topic-based coloring."""
        logger.info("🎨 Creating UMAP visualization...")

        # Get unique topics and create color mapping
        unique_topics = sorted(self.topic_assignments["topic_id"].unique())
        n_topics = len(unique_topics)

        # Create color map
        if n_topics <= len(self.color_palette):
            colors = self.color_palette[:n_topics]
        else:
            # If more topics than colors, use a continuous colormap
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
            topic_mask = self.topic_assignments["topic_id"] == topic_id

            # Plot points for this topic
            ax.scatter(
                umap_embeddings[topic_mask, 0],
                umap_embeddings[topic_mask, 1],
                c=[colors[i]],
                label=f"Topic {topic_id}" if topic_id != -1 else "Outliers",
                alpha=0.7,
                s=20,
                edgecolors="none",
            )

        # Apply scientific styling
        self._apply_scientific_styling(fig, ax, unique_topics)

        logger.info("✅ UMAP visualization created successfully")
        return fig, ax

    def _apply_scientific_styling(
        self, fig: plt.Figure, ax: plt.Axes, topics: List[int]
    ) -> None:
        """Apply scientific visualization standards to the plot."""
        # Set title
        ax.set_title(
            "Responses in Semantic Space, Classified by Their Topic",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )

        # Set axis labels
        ax.set_xlabel("UMAP Dimension 1", fontsize=12, fontweight="bold")
        ax.set_ylabel("UMAP Dimension 2", fontsize=12, fontweight="bold")

        # Set axis properties
        ax.tick_params(axis="both", which="major", labelsize=10)
        ax.grid(True, alpha=0.3, linestyle="-", linewidth=0.5)

        # Set legend
        n_topics = len(topics)
        if n_topics <= 10:
            # Show all topics in legend
            ax.legend(
                bbox_to_anchor=(1.05, 1),
                loc="upper left",
                fontsize=10,
                frameon=True,
                fancybox=True,
                shadow=True,
            )
        else:
            # For many topics, show only a subset in legend
            handles, labels = ax.get_legend_handles_labels()
            # Show first 5 and last 5 topics
            n_show = min(10, n_topics)
            ax.legend(
                handles[:n_show],
                labels[:n_show],
                bbox_to_anchor=(1.05, 1),
                loc="upper left",
                fontsize=10,
                frameon=True,
                fancybox=True,
                shadow=True,
                title=f"Topics (showing {n_show}/{n_topics})",
            )

        # Set aspect ratio to be equal
        ax.set_aspect("equal", adjustable="box")

        # Tight layout
        plt.tight_layout()

    def save_results(self, fig: plt.Figure, umap_embeddings: np.ndarray) -> str:
        """Save visualization results and metadata."""
        logger.info("💾 Saving visualization results...")

        # Create output directory
        mode = "local" if "local" in self.results_path else "prod"
        timestamp = generate_current_datetime_str()
        output_dir = Path("visualization/results/umap") / mode / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save figure
        fig_path = output_dir / "umap_visualization.png"
        fig.savefig(fig_path, dpi=300, bbox_inches="tight", facecolor="white")
        logger.info(f"📊 Figure saved to: {fig_path}")

        # Save high-resolution version
        fig_path_hires = output_dir / "umap_visualization_hires.png"
        fig.savefig(fig_path_hires, dpi=600, bbox_inches="tight", facecolor="white")
        logger.info(f"📊 High-res figure saved to: {fig_path_hires}")

        # Save UMAP embeddings
        embeddings_df = pd.DataFrame(umap_embeddings, columns=["umap_x", "umap_y"])
        embeddings_df["doc_id"] = self.topic_assignments["doc_id"]
        embeddings_df["topic_id"] = self.topic_assignments["topic_id"]

        embeddings_path = output_dir / "umap_embeddings.csv"
        embeddings_df.to_csv(embeddings_path, index=False)
        logger.info(f"📊 UMAP embeddings saved to: {embeddings_path}")

        # Save metadata
        metadata = {
            "visualization_type": "umap",
            "model_path": self.model_path,
            "metadata_path": self.metadata_path,
            "results_path": self.results_path,
            "umap_parameters": self.umap_params,
            "figure_parameters": self.figure_params,
            "n_documents": len(self.documents),
            "n_topics": len(self.topic_assignments["topic_id"].unique()),
            "topic_distribution": self.topic_assignments["topic_id"]
            .value_counts()
            .to_dict(),
            "creation_timestamp": timestamp,
            "color_palette": self.color_palette[
                : len(self.topic_assignments["topic_id"].unique())
            ],
        }

        metadata_path = output_dir / "visualization_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
        logger.info(f"📋 Metadata saved to: {metadata_path}")

        return str(output_dir)

    def run_visualization(self) -> str:
        """Run the complete UMAP visualization pipeline."""
        start_time = time.time()

        logger.info("🚀 Starting UMAP visualization pipeline...")

        # Load model and results
        self.load_model_and_results()

        # Generate embeddings
        embeddings = self.generate_embeddings()

        # Compute UMAP embeddings
        umap_embeddings = self.compute_umap_embeddings(embeddings)

        # Create visualization
        fig, ax = self.create_visualization(umap_embeddings)

        # Save results
        output_dir = self.save_results(fig, umap_embeddings)

        # Close figure to free memory
        plt.close(fig)

        elapsed_time = time.time() - start_time
        logger.info(f"🎉 UMAP visualization completed in {elapsed_time:.2f} seconds")
        logger.info(f"📁 Results saved to: {output_dir}")

        return output_dir


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate UMAP visualization for topic modeling results"
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
        "--umap-neighbors",
        type=int,
        default=15,
        help="Number of neighbors for UMAP (default: 15)",
    )
    parser.add_argument(
        "--umap-min-dist",
        type=float,
        default=0.1,
        help="Minimum distance for UMAP (default: 0.1)",
    )

    args = parser.parse_args()

    # Create visualizer
    visualizer = UMAPVisualizer(
        model_path=args.model_path,
        metadata_path=args.metadata_path,
        results_path=args.results_path,
    )

    # Update UMAP parameters if provided
    if args.umap_neighbors != 15:
        visualizer.umap_params["n_neighbors"] = args.umap_neighbors
    if args.umap_min_dist != 0.1:
        visualizer.umap_params["min_dist"] = args.umap_min_dist

    # Run visualization
    try:
        output_dir = visualizer.run_visualization()
        print("✅ UMAP visualization completed successfully!")
        print(f"📁 Results saved to: {output_dir}")
    except Exception as e:
        logger.error(f"❌ UMAP visualization failed: {e}")
        raise


if __name__ == "__main__":
    main()
