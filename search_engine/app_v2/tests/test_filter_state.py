import pytest
from typing import Dict, Any, List
from search_engine.app_v2.filter_state import FilterState

# Assume the existence of a FilterState class to be implemented

class TestFilterState:
    """
    Unit tests for the FilterState logic that underpins the Filter Builder Panel.
    This covers adding, removing, and listing active filters as chips.
    """

    def test_add_filter(self):
        """
        Test that adding a filter updates the state correctly.
        Input: Add a filter {category: 'Temporal', key: 'date_range', value: '2024-01-01 to 2024-01-31'}
        Expected: The filter state contains this filter under the correct category.
        """
        state = FilterState()
        state.add_filter('Temporal', 'date_range', '2024-01-01 to 2024-01-31')
        expected = {'Temporal': {'date_range': '2024-01-01 to 2024-01-31'}}
        assert state.filters == expected

    def test_remove_filter(self):
        """
        Test that removing a filter updates the state correctly.
        Input: Add then remove a filter {category: 'Content', key: 'keyword', value: 'climate change'}
        Expected: The filter is no longer present in the state.
        """
        state = FilterState()
        state.add_filter('Content', 'keyword', 'climate change')
        state.remove_filter('Content', 'keyword')
        assert 'keyword' not in state.filters.get('Content', {})

    def test_active_chips(self):
        """
        Test that active filters are represented as chips.
        Input: Add multiple filters across categories.
        Expected: The chips list contains all active filters as dicts with category, key, and value.
        """
        state = FilterState()
        state.add_filter('Sentiment', 'valence', 'positive')
        state.add_filter('Political', 'slant', 'left')
        chips = state.active_chips()
        expected = [
            {'category': 'Sentiment', 'key': 'valence', 'value': 'positive'},
            {'category': 'Political', 'key': 'slant', 'value': 'left'}
        ]
        assert all(chip in chips for chip in expected)

    def test_clear_filters(self):
        """
        Test that clearing all filters resets the state.
        Input: Add several filters, then clear.
        Expected: The filter state is empty.
        """
        state = FilterState()
        state.add_filter('User', 'verified', True)
        state.add_filter('Engagement', 'type', 'like')
        state.clear_filters()
        assert state.filters == {} 