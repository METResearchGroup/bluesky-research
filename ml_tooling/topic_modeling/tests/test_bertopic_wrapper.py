"""Tests for bertopic_wrapper.py.

This test suite verifies the functionality of BERTopicWrapper text preprocessing:
- Text preprocessing with stopwords removal
- Custom stopwords handling
- URL and emoji removal
- Edge cases and error handling
"""

import pytest
from unittest.mock import patch

from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper


class TestBERTopicWrapperTextPreprocessing:
    """Tests for BERTopicWrapper text preprocessing functionality."""

    def test_preprocess_text_with_stopwords_enabled(self):
        """Test text preprocessing with stopwords removal enabled.

        This test verifies that:
        1. Common English stopwords are removed from text
        2. Custom stopwords are also removed
        3. URLs and emojis are removed when enabled
        4. Text is converted to lowercase
        5. Minimum word length filtering works correctly
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": ["rt", "via", "amp", "u", "ur", "im", "ill", "ive", "id"]
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "This is a test post with common words like the and is RT @user this is a retweet with amp and via"
        expected = "test post common words like user retweet"

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_with_stopwords_disabled(self):
        """Test text preprocessing with stopwords removal disabled.

        This test verifies that:
        1. Stopwords are not removed when disabled
        2. Other preprocessing steps still work (lowercase, URL removal)
        3. The configuration controls preprocessing behavior correctly
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": False,  # Disabled
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": ["rt", "via", "amp"]
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "This is a test post with common words like the and is RT @user this is a retweet"
        expected = "this is a test post with common words like the and is rt user this is a retweet"

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_removes_urls(self):
        """Test that URLs are properly removed from text.

        This test verifies that:
        1. HTTP and HTTPS URLs are removed
        2. URL removal works with other preprocessing steps
        3. The regex pattern correctly identifies URLs
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "Check out this link https://example.com and also http://test.org for more info"
        expected = "check link also info"

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected
    
    def test_preprocess_text_removes_emojis(self):
        """Test that emojis and special characters are properly removed.

        This test verifies that:
        1. Emojis are removed from text
        2. Special characters are removed
        3. Only alphanumeric characters and spaces remain
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "This is great! 🎉 Check out this link @user #hashtag $money"
        expected = "great check link user hashtag money"

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_filters_by_minimum_word_length(self):
        """Test that words shorter than minimum length are filtered out.

        This test verifies that:
        1. Words shorter than min_word_length are removed
        2. The minimum length filtering works with other preprocessing steps
        3. The configuration parameter controls filtering correctly
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 3,  # Minimum 3 characters
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "This is a test with short words like it and we"
        expected = "test short words like"

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_handles_custom_stopwords(self):
        """Test that custom stopwords are properly removed.

        This test verifies that:
        1. Custom stopwords from configuration are removed
        2. Custom stopwords work alongside standard English stopwords
        3. Social media specific terms are handled correctly
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": ["rt", "via", "amp", "u", "ur", "im", "ill", "ive", "id", "dont", "cant", "wont"]
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "RT @user this is a retweet via amp u should check this out im sure youll like it"
        expected = "user retweet check sure youll like"

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_handles_empty_string(self):
        """Test handling of empty string input.

        This test verifies that:
        1. Empty strings are handled gracefully
        2. The function returns empty string for empty input
        3. Edge case doesn't cause crashes
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = ""
        expected = ""

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_handles_none_input(self):
        """Test handling of None input.

        This test verifies that:
        1. None input is handled gracefully
        2. The function returns empty string for None input
        3. Edge case doesn't cause crashes
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = None
        expected = ""

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_handles_non_string_input(self):
        """Test handling of non-string input.

        This test verifies that:
        1. Non-string input is handled gracefully
        2. The function returns empty string for non-string input
        3. Edge case doesn't cause crashes
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = 123  # Non-string input
        expected = ""

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    @patch('ml_tooling.topic_modeling.bertopic_wrapper.NLTK_AVAILABLE', False)
    def test_preprocess_text_handles_nltk_unavailable(self):
        """Test handling when NLTK is not available.

        This test verifies that:
        1. When NLTK is not available, stopwords removal is skipped
        2. Other preprocessing steps still work
        3. The function doesn't crash when NLTK is missing
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 2,
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "This is a test post with common words like the and is"
        expected = "this is a test post with common words like the and is"  # No stopwords removed

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected

    def test_preprocess_text_removes_all_words_becoming_empty(self):
        """Test handling when all words are removed during preprocessing.

        This test verifies that:
        1. When all words are filtered out, empty string is returned
        2. The function handles extreme filtering gracefully
        3. Edge case doesn't cause crashes
        """
        # Arrange
        config = {
            "embedding_model": {"name": "all-MiniLM-L6-v2", "device": "cpu", "batch_size": 32},
            "bertopic": {"top_n_words": 10, "min_topic_size": 2, "nr_topics": 2, "calculate_probabilities": True, "verbose": False},
            "quality_thresholds": {"c_v_min": 0.1, "c_npmi_min": 0.05},
            "text_preprocessing": {
                "enable": True,
                "remove_stopwords": True,
                "language": "english",
                "min_word_length": 10,  # Very high minimum length
                "remove_urls": True,
                "remove_emojis": True,
                "custom_stopwords": []
            },
            "random_seed": 42
        }
        
        wrapper = BERTopicWrapper(config_dict=config)
        input_text = "This is a test post with short words"
        expected = ""  # All words filtered out

        # Act
        result = wrapper._preprocess_text(input_text)

        # Assert
        assert result == expected
