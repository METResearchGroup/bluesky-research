import pytest
from datetime import date, timedelta
from typing import List, Dict, Any
from unittest import mock

# Assume the function to be tested will be called generate_daily_counts
# and will be implemented in components.visualization_quicklook_panel
from search_engine.app_v2.sample_data import get_sample_posts

class TestGenerateDailyCounts:
    """
    Tests for the generate_daily_counts function, which produces a daily time series
    of post counts based on filters and sample data.
    """
    @classmethod
    def setup_class(cls):
        cls.data = get_sample_posts()

    def test_full_range_counts(self):
        """
        With no date filter, returns counts for all dates in the data range.
        """
        filters = {}
        # This will fail until generate_daily_counts is implemented
        from search_engine.app_v2.components.visualization_quicklook_panel import generate_daily_counts
        result = generate_daily_counts(filters, self.data)
        # Should cover all dates in the sample data
        dates = sorted({row['date'] for row in self.data})
        assert all(any(r['date'] == d for r in result) for d in dates)
        # Each count should match the number of posts for that date
        for d in dates:
            expected = sum(1 for row in self.data if row['date'] == d)
            actual = next(r['count'] for r in result if r['date'] == d)
            assert actual == expected

    def test_date_range_filter(self):
        """
        With a date range filter, only counts for dates in the range are returned.
        """
        filters = {"Temporal": {"date_range": "2024-06-10 to 2024-06-15"}}
        from search_engine.app_v2.components.visualization_quicklook_panel import generate_daily_counts
        result = generate_daily_counts(filters, self.data)
        expected_dates = [
            (date(2024, 6, 10) + timedelta(days=i)).isoformat() for i in range(6)
        ]
        assert [r['date'] for r in result] == expected_dates
        for r in result:
            assert r['date'] in expected_dates
            expected = sum(1 for row in self.data if row['date'] == r['date'])
            assert r['count'] == expected

    def test_no_matching_posts(self):
        """
        If no posts match the filter, returns all zeros for the date range.
        """
        filters = {"Content": {"hashtags": ["#notarealhashtag"]}, "Temporal": {"date_range": "2024-06-10 to 2024-06-12"}}
        from search_engine.app_v2.components.visualization_quicklook_panel import generate_daily_counts
        result = generate_daily_counts(filters, self.data)
        expected_dates = ["2024-06-10", "2024-06-11", "2024-06-12"]
        assert [r['date'] for r in result] == expected_dates
        assert all(r['count'] == 0 for r in result)

class TestVisualizationQuickLookUI:
    """
    Tests that the visualization panel calls st.altair_chart with the correct data.
    """
    def test_altair_chart_called_with_correct_data(self):
        """
        When render_visualization_quicklook_panel is called, it should call st.altair_chart
        with the daily counts generated from the filters and data.
        """
        filters = {"Temporal": {"date_range": "2024-06-10 to 2024-06-12"}}
        data = [
            {"date": "2024-06-10"},
            {"date": "2024-06-11"},
            {"date": "2024-06-11"},
            {"date": "2024-06-12"},
        ]
        # Patch st.altair_chart and generate_daily_counts
        with mock.patch("search_engine.app_v2.components.visualization_quicklook_panel.st.altair_chart") as mock_chart, \
             mock.patch("search_engine.app_v2.components.visualization_quicklook_panel.generate_daily_counts") as mock_counts:
            mock_counts.return_value = [
                {"date": "2024-06-10", "count": 1},
                {"date": "2024-06-11", "count": 2},
                {"date": "2024-06-12", "count": 1},
            ]
            from search_engine.app_v2.components.visualization_quicklook_panel import render_visualization_quicklook_panel
            render_visualization_quicklook_panel(filters, data)
            # Should call st.altair_chart once with the correct data
            mock_chart.assert_called_once()
            called_arg = mock_chart.call_args[0][0]
            import altair as alt
            assert isinstance(called_arg, alt.Chart) 