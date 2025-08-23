#!/usr/bin/env python3
"""
Simple verification script to test configuration loading.

This script verifies that:
1. The config.yaml file loads without errors
2. No invalid parameters are passed to BERTopic
3. The configuration structure is correct

Author: AI Agent implementing debugging fixes
Date: 2025-08-22
"""

import yaml
from pathlib import Path


def verify_config():
    """Verify the configuration file loads correctly."""
    print("🔍 Verifying BERTopic configuration...")

    try:
        # Load the config file
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        print("✅ Configuration file loaded successfully")

        # Check BERTopic configuration
        bertopic_config = config.get("bertopic", {})
        print(f"📊 BERTopic config keys: {list(bertopic_config.keys())}")

        # Check for invalid top-level parameters
        invalid_params = ["n_jobs"]  # These should not be at the top level
        for param in invalid_params:
            if param in bertopic_config:
                print(f"❌ Invalid parameter '{param}' found at top level")
                return False
            else:
                print(f"✅ No invalid parameter '{param}' at top level")

        # Check UMAP configuration
        umap_config = bertopic_config.get("umap_model", {})
        if "n_jobs" in umap_config:
            print(f"✅ UMAP n_jobs parameter: {umap_config['n_jobs']}")
        else:
            print("❌ UMAP n_jobs parameter missing")
            return False

        # Check HDBSCAN configuration
        hdbscan_config = bertopic_config.get("hdbscan_model", {})
        if "core_dist_n_jobs" in hdbscan_config:
            print(
                f"✅ HDBSCAN core_dist_n_jobs parameter: {hdbscan_config['core_dist_n_jobs']}"
            )
        else:
            print("❌ HDBSCAN core_dist_n_jobs parameter missing")
            return False

        # Check performance configuration
        performance_config = config.get("performance", {})
        if performance_config:
            print(f"✅ Performance config keys: {list(performance_config.keys())}")
        else:
            print("❌ Performance configuration missing")
            return False

        print("\n🎉 Configuration verification completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Configuration verification failed: {e}")
        return False


if __name__ == "__main__":
    success = verify_config()
    exit(0 if success else 1)
