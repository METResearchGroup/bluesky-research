"""
Tests for the DataLoaderConfig configuration management.

This module tests the configuration management functionality for data loaders
in the topic modeling pipeline.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open

from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.config import DataLoaderConfig
from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.base import DataLoader


class MockDataLoader(DataLoader):
    """Mock implementation of DataLoader for testing."""
    
    def load_text_data(self, start_date: str, end_date: str):
        """Mock implementation."""
        return None


class TestDataLoaderConfig:
    """Test the DataLoaderConfig class."""
    
    def test_config_initialization_with_default_path(self):
        """Test config initialization with default path."""
        # Mock the default config path
        with patch('src.data_loading.config.Path') as mock_path:
            mock_path.return_value.parent.parent.parent = Path("/mock/path")
            mock_path.return_value.exists.return_value = False
            
            config = DataLoaderConfig()
            
            assert config.config_path is not None
            assert config.config_data == {}
            assert config.available_loaders == {}
    
    def test_config_initialization_with_custom_path(self):
        """Test config initialization with custom path."""
        custom_path = "/custom/config.yaml"
        config = DataLoaderConfig(custom_path)
        
        assert str(config.config_path) == custom_path
    
    def test_create_default_config(self):
        """Test creation of default configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "data_loader.yaml"
            config = DataLoaderConfig(str(config_path))
            
            # Check that default config was created
            assert config_path.exists()
            
            # Load and verify the created config
            with open(config_path, 'r') as f:
                created_config = yaml.safe_load(f)
            
            expected_keys = ["data_loader", "validation", "performance"]
            for key in expected_keys:
                assert key in created_config
            
            # Check specific values
            assert created_config["data_loader"]["type"] == "local"
            assert created_config["data_loader"]["local"]["enabled"] is True
            assert created_config["data_loader"]["local"]["service"] == "preprocessed_posts"
    
    def test_load_existing_config(self):
        """Test loading existing configuration file."""
        test_config = {
            "data_loader": {
                "type": "custom",
                "custom": {
                    "enabled": True,
                    "endpoint": "http://test.com"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(test_config, f)
            config_path = f.name
        
        try:
            config = DataLoaderConfig(config_path)
            
            assert config.config_data["data_loader"]["type"] == "custom"
            assert config.config_data["data_loader"]["custom"]["endpoint"] == "http://test.com"
        finally:
            Path(config_path).unlink()
    
    def test_register_loader(self):
        """Test registering a data loader class."""
        config = DataLoaderConfig()
        
        # Register a mock loader
        config.register_loader("mock", MockDataLoader)
        
        assert "mock" in config.available_loaders
        assert config.available_loaders["mock"] == MockDataLoader
    
    def test_register_loader_invalid_class(self):
        """Test registering an invalid loader class."""
        config = DataLoaderConfig()
        
        class InvalidLoader:
            pass
        
        with pytest.raises(ValueError, match="Loader class must inherit from DataLoader"):
            config.register_loader("invalid", InvalidLoader)
    
    def test_get_loader_config_default(self):
        """Test getting default loader configuration."""
        config = DataLoaderConfig()
        
        # Create a minimal config
        config.config_data = {
            "data_loader": {
                "type": "local",
                "local": {
                    "enabled": True,
                    "service": "test_service"
                }
            }
        }
        
        loader_config = config.get_loader_config()
        assert loader_config["service"] == "test_service"
    
    def test_get_loader_config_specific(self):
        """Test getting configuration for a specific loader type."""
        config = DataLoaderConfig()
        
        config.config_data = {
            "data_loader": {
                "local": {
                    "enabled": True,
                    "service": "local_service"
                },
                "production": {
                    "enabled": False,
                    "endpoint": "http://prod.com"
                }
            }
        }
        
        local_config = config.get_loader_config("local")
        assert local_config["service"] == "local_service"
        
        prod_config = config.get_loader_config("production")
        assert prod_config["endpoint"] == "http://prod.com"
    
    def test_get_loader_config_not_found(self):
        """Test getting configuration for non-existent loader type."""
        config = DataLoaderConfig()
        
        config.config_data = {
            "data_loader": {
                "local": {"enabled": True}
            }
        }
        
        with pytest.raises(KeyError, match="Configuration not found for loader type: nonexistent"):
            config.get_loader_config("nonexistent")
    
    def test_is_loader_enabled(self):
        """Test checking if a loader is enabled."""
        config = DataLoaderConfig()
        
        config.config_data = {
            "data_loader": {
                "local": {"enabled": True},
                "production": {"enabled": False}
            }
        }
        
        assert config.is_loader_enabled("local") is True
        assert config.is_loader_enabled("production") is False
        assert config.is_loader_enabled("nonexistent") is False
    
    def test_get_validation_config(self):
        """Test getting validation configuration."""
        config = DataLoaderConfig()
        
        config.config_data = {
            "validation": {
                "max_date_range_days": 100,
                "min_text_length": 5
            }
        }
        
        validation_config = config.get_validation_config()
        assert validation_config["max_date_range_days"] == 100
        assert validation_config["min_text_length"] == 5
    
    def test_get_performance_config(self):
        """Test getting performance configuration."""
        config = DataLoaderConfig()
        
        config.config_data = {
            "performance": {
                "batch_size": 500,
                "memory_limit_gb": 4
            }
        }
        
        performance_config = config.get_performance_config()
        assert performance_config["batch_size"] == 500
        assert performance_config["memory_limit_gb"] == 4
    
    def test_update_config(self):
        """Test updating configuration."""
        config = DataLoaderConfig()
        
        # Initial config
        config.config_data = {
            "data_loader": {
                "local": {"enabled": True}
            }
        }
        
        # Update config
        updates = {
            "data_loader": {
                "local": {"enabled": False, "new_option": "value"}
            }
        }
        
        config.update_config(updates)
        
        assert config.config_data["data_loader"]["local"]["enabled"] is False
        assert config.config_data["data_loader"]["local"]["new_option"] == "value"
    
    def test_deep_merge(self):
        """Test deep merging of configuration updates."""
        config = DataLoaderConfig()
        
        # Initial config with nested structure
        config.config_data = {
            "level1": {
                "level2": {
                    "value1": "original",
                    "value2": "keep"
                }
            }
        }
        
        # Updates with nested structure
        updates = {
            "level1": {
                "level2": {
                    "value1": "updated",
                    "value3": "new"
                }
            }
        }
        
        config.update_config(updates)
        
        # Check that values were properly merged
        assert config.config_data["level1"]["level2"]["value1"] == "updated"
        assert config.config_data["level1"]["level2"]["value2"] == "keep"
        assert config.config_data["level1"]["level2"]["value3"] == "new"
    
    def test_get_info(self):
        """Test getting configuration information."""
        config = DataLoaderConfig()
        
        # Set up some test data
        config.config_data = {
            "data_loader": {
                "type": "local",
                "local": {"enabled": True}
            }
        }
        config.register_loader("local", MockDataLoader)
        config.register_loader("production", MockDataLoader)
        
        info = config.get_info()
        
        assert "config_path" in info
        assert "available_loaders" in info
        assert "enabled_loaders" in info
        assert "default_loader" in info
        assert "validation_config" in info
        assert "performance_config" in info
        
        assert "local" in info["available_loaders"]
        assert "production" in info["available_loaders"]
        assert info["default_loader"] == "local"
    
    def test_string_representations(self):
        """Test string representation methods."""
        config = DataLoaderConfig()
        config.register_loader("test", MockDataLoader)
        
        str_repr = str(config)
        repr_repr = repr(config)
        
        assert "DataLoaderConfig" in str_repr
        assert "DataLoaderConfig" in repr_repr
        assert "test" in repr_repr
