"""Tests for utils.py.

This test suite verifies the functionality of utility functions:
- check_if_string_is_empty: Tests various cases of empty string detection
- filter_posts_df: Tests filtering of DataFrame posts based on text and timestamp validity
"""

import numpy as np
import pandas as pd
import pytest

from lib.utils import check_if_string_is_empty, filter_posts_df, MIN_POST_TEXT_LENGTH


class TestCheckIfStringIsEmpty:
    """Tests for check_if_string_is_empty function."""

    @pytest.mark.parametrize("input_string,expected", [
        # None and empty string cases
        (None, True),
        ("", True),
        (" ", True),
        ("  \t  \n  ", True),  # Whitespace with tabs and newlines
        
        # Common string representations of null
        ("null", True),
        ("NULL", True),
        ("Null", True),
        ("none", True),
        ("NONE", True),
        ("None", True),
        ("nan", True),
        ("NaN", True),
        ("NAN", True),
        
        # Valid strings
        ("text", False),
        (" text ", False),
        ("0", False),
        ("false", False),
        
        # Pandas null types
        (pd.NA, True),
        (pd.NaT, True),
        (np.nan, True),
    ])
    def test_various_empty_strings(self, input_string, expected):
        """Test different types of empty/non-empty strings."""
        result = check_if_string_is_empty(input_string)
        assert result == expected

    def test_pandas_series_null(self):
        """Test handling of pandas Series null values."""
        series = pd.Series([
            "text",             # Valid text
            None,              # None
            pd.NA,             # Pandas NA
            "",               # Empty string
            np.nan,           # Numpy nan
            "  \t  \n  ",    # Whitespace
            "NULL",           # Common null string
            "None",           # Common none string
            "NaN"            # Common nan string
        ])
        results = [check_if_string_is_empty(val) for val in series]
        expected = [False, True, True, True, True, True, True, True, True]
        assert results == expected

    def test_non_string_input(self):
        """Test handling of non-string inputs."""
        # Non-string inputs should raise AttributeError
        with pytest.raises(AttributeError):
            check_if_string_is_empty(123)
        with pytest.raises(AttributeError):
            check_if_string_is_empty(0)
        with pytest.raises(AttributeError):
            check_if_string_is_empty(True)
        with pytest.raises(AttributeError):
            check_if_string_is_empty(False)
        with pytest.raises(AttributeError):
            check_if_string_is_empty({})
        with pytest.raises(AttributeError):
            check_if_string_is_empty([])
        with pytest.raises(AttributeError):
            check_if_string_is_empty(object())

    def test_whitespace_handling(self):
        """Test handling of various whitespace cases."""
        whitespace_cases = [
            " ",              # Single space
            "   ",           # Multiple spaces
            "\t",            # Tab
            "\n",            # Newline
            "\r",            # Carriage return
            " \t \n \r ",    # Mixed whitespace
            "\u2000",        # Unicode space
            "\u200b",        # Zero-width space
        ]
        for case in whitespace_cases:
            assert check_if_string_is_empty(case) == True


class TestFilterPostsDf:
    """Tests for filter_posts_df function."""

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        result = filter_posts_df(df)
        assert len(result) == 0
        
        result_strict = filter_posts_df(df, strict=True)
        assert len(result_strict) == 0

    def test_missing_columns(self):
        """Test handling of DataFrame without required columns."""
        df = pd.DataFrame({
            'other_column': ['text1', 'text2']
        })
        result = filter_posts_df(df)
        assert len(result) == 2  # Should pass through unchanged
        
        result_strict = filter_posts_df(df, strict=True)
        assert len(result_strict) == 2  # Should pass through unchanged

    def test_basic_filtering(self):
        """Test basic text and timestamp filtering."""
        df = pd.DataFrame({
            'text': [
                'valid text',                    # Valid
                'a',                             # Too short
                None,                            # None
                '',                              # Empty
                'also valid but no timestamp'    # Valid text but missing timestamp
            ],
            'preprocessing_timestamp': [
                '2024-01-01',
                '2024-01-01',
                '2024-01-01',
                '2024-01-01',
                None
            ]
        })
        result = filter_posts_df(df)
        assert len(result) == 1
        assert result.iloc[0]['text'] == 'valid text'

    def test_strict_filtering(self):
        """Test strict filtering with check_if_string_is_empty."""
        df = pd.DataFrame({
            'text': [
                'valid text',            # Valid
                'NULL',                  # Common null string
                'None',                  # Common none string
                'nan',                   # Common nan string
                '   valid text   ',      # Valid with whitespace
                'proper text'            # Valid
            ],
            'preprocessing_timestamp': ['2024-01-01'] * 6
        })
        
        # Normal filtering - should only remove empty strings and too-short texts
        result = filter_posts_df(df)
        assert len(result) == 3  # Only valid texts remain (valid text, valid text with whitespace, proper text)
        
        # Strict filtering - should also remove null-like strings
        result_strict = filter_posts_df(df, strict=True)
        assert len(result_strict) == 3  # Only valid texts remain
        assert all(text.strip() in ['valid text', 'proper text'] for text in result_strict['text'])

    def test_text_length_requirement(self):
        """Test enforcement of minimum text length."""
        df = pd.DataFrame({
            'text': [
                'a' * (MIN_POST_TEXT_LENGTH - 1),  # Too short
                'a' * MIN_POST_TEXT_LENGTH,        # Exactly minimum
                'a' * (MIN_POST_TEXT_LENGTH + 1),  # Above minimum
            ],
            'preprocessing_timestamp': ['2024-01-01'] * 3
        })
        result = filter_posts_df(df)
        assert len(result) == 2
        assert all(len(text) >= MIN_POST_TEXT_LENGTH for text in result['text'])

    def test_timestamp_filtering(self):
        """Test preprocessing_timestamp filtering."""
        df = pd.DataFrame({
            'text': ['valid text'] * 4,
            'preprocessing_timestamp': [
                '2024-01-01',    # Valid
                None,            # None
                pd.NA,          # Pandas NA
                np.nan          # Numpy nan
            ]
        })
        result = filter_posts_df(df)
        assert len(result) == 1
        assert result.iloc[0]['preprocessing_timestamp'] == '2024-01-01' 