import pytest
import os
import pandas as pd
from unittest.mock import patch

class TestSampleNetworkFileGeneration:
    """
    Tests for sample network file generation (ticket #006).
    Verifies node/edge CSVs, engagement bias, and data quality.
    """
    def test_node_and_edge_csv_schema_and_counts(self):
        """
        Should generate node and edge CSVs with correct columns and row counts (100 nodes/day for 14 days).
        """
        from search_engine.app_v2 import generate_sample_network_data
        node_csv, edge_csv = generate_sample_network_data.generate_sample_network_files()
        node_df = pd.read_csv(node_csv)
        edge_df = pd.read_csv(edge_csv)
        assert set(node_df.columns) == {"id", "date", "valence", "toxic", "political", "slant"}
        assert set(edge_df.columns) == {"source", "target", "type", "date"}
        assert len(node_df) == 1400
        assert len(edge_df) > 0

    def test_engagement_bias(self):
        """
        Should bias engagement: positive valence/low toxicity nodes have more edges than negative/high toxicity nodes.
        """
        from search_engine.app_v2 import generate_sample_network_data
        node_csv, edge_csv = generate_sample_network_data.generate_sample_network_files()
        node_df = pd.read_csv(node_csv)
        edge_df = pd.read_csv(edge_csv)
        # Compute mean degree by valence/toxic
        edge_counts = edge_df.groupby("source").size().to_dict()
        node_df["degree"] = node_df["id"].map(edge_counts).fillna(0)
        pos_low = node_df[(node_df["valence"] == "positive") & (node_df["toxic"] == False)]["degree"].mean()
        neg_high = node_df[(node_df["valence"] == "negative") & (node_df["toxic"] == True)]["degree"].mean()
        assert pos_low > neg_high

    def test_data_quality(self):
        """
        Should have no missing values and correct types in node and edge CSVs.
        """
        from search_engine.app_v2 import generate_sample_network_data
        node_csv, edge_csv = generate_sample_network_data.generate_sample_network_files()
        node_df = pd.read_csv(node_csv)
        edge_df = pd.read_csv(edge_csv)
        assert not node_df.isnull().any().any()
        assert not edge_df.isnull().any().any()
        assert node_df["id"].dtype == int
        assert edge_df["source"].dtype == int
        assert edge_df["target"].dtype == int 