"""Comprehensive test suite for hashtag analysis module.

This module contains tests for all hashtag analysis functionality including
hashtag extraction, analysis, visualization, and data loading.
"""

import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

# Import the modules to test
from hashtag_analysis import (
    extract_hashtags_from_text,
    get_hashtag_counts_for_posts,
    filter_rare_hashtags,
    get_election_period,
    create_stratified_hashtag_analysis,
    create_hashtag_dataframe,
    get_top_hashtags_by_condition,
    get_hashtag_summary_stats,
    validate_hashtag_data,
    ELECTION_DATE,
    MIN_HASHTAG_FREQUENCY,
)
from visualization import (
    create_top_hashtags_by_condition_chart,
    create_pre_post_election_comparison_chart,
    create_hashtag_frequency_distribution_chart,
    create_condition_election_period_heatmap,
    create_all_visualizations,
)


class TestHashtagExtraction(unittest.TestCase):
    """Test hashtag extraction functionality."""
    
    def test_extract_hashtags_from_text(self):
        """Test hashtag extraction from text."""
        # Test basic hashtag extraction
        text = "This is a #test post with #multiple #hashtags"
        result = extract_hashtags_from_text(text)
        expected = ["test", "multiple", "hashtags"]
        self.assertEqual(sorted(result), sorted(expected))
        
        # Test case normalization
        text = "This has #UPPERCASE and #MixedCase hashtags"
        result = extract_hashtags_from_text(text)
        expected = ["uppercase", "mixedcase"]
        self.assertEqual(sorted(result), sorted(expected))
        
        # Test empty text
        self.assertEqual(extract_hashtags_from_text(""), [])
        self.assertEqual(extract_hashtags_from_text(None), [])
        
        # Test text without hashtags
        text = "This text has no hashtags"
        result = extract_hashtags_from_text(text)
        self.assertEqual(result, [])
        
        # Test special characters in hashtags
        text = "This has #hashtag_with_underscores and #hashtag-with-dashes"
        result = extract_hashtags_from_text(text)
        expected = ["hashtag_with_underscores", "hashtag-with-dashes"]
        self.assertEqual(sorted(result), sorted(expected))


class TestHashtagAnalysis(unittest.TestCase):
    """Test hashtag analysis functionality."""
    
    def setUp(self):
        """Set up test data."""
        self.sample_posts = pd.DataFrame({
            'uri': ['uri1', 'uri2', 'uri3'],
            'text': [
                'This is a #test post with #hashtags',
                'Another post with #test and #different hashtags',
                'Third post with #hashtags and #more content'
            ]
        })
    
    def test_get_hashtag_counts_for_posts(self):
        """Test hashtag counting for posts."""
        result = get_hashtag_counts_for_posts(self.sample_posts)
        expected = {"test": 2, "hashtags": 2, "different": 1, "more": 1}
        self.assertEqual(result, expected)
    
    def test_filter_rare_hashtags(self):
        """Test filtering of rare hashtags."""
        hashtag_counts = {"common": 10, "rare1": 2, "rare2": 1, "common2": 8}
        result = filter_rare_hashtags(hashtag_counts, min_frequency=5)
        expected = {"common": 10, "common2": 8}
        self.assertEqual(result, expected)
    
    def test_get_election_period(self):
        """Test election period determination."""
        # Pre-election date
        self.assertEqual(get_election_period("2024-11-04"), "pre_election")
        self.assertEqual(get_election_period("2024-10-01"), "pre_election")
        
        # Post-election date
        self.assertEqual(get_election_period("2024-11-06"), "post_election")
        self.assertEqual(get_election_period("2024-12-01"), "post_election")
        
        # Election day (should be post)
        self.assertEqual(get_election_period(ELECTION_DATE), "post_election")


class TestStratifiedAnalysis(unittest.TestCase):
    """Test stratified hashtag analysis."""
    
    def setUp(self):
        """Set up test data."""
        self.user_df = pd.DataFrame({
            'bluesky_user_did': ['user1', 'user2'],
            'condition': ['control', 'treatment']
        })
        
        self.user_to_content_in_feeds = {
            'user1': {
                '2024-11-01': {'uri1', 'uri2'},  # pre-election
                '2024-11-10': {'uri3'}  # post-election
            },
            'user2': {
                '2024-11-01': {'uri4'},  # pre-election
                '2024-11-10': {'uri5', 'uri6'}  # post-election
            }
        }
        
        self.posts_data = pd.DataFrame({
            'uri': ['uri1', 'uri2', 'uri3', 'uri4', 'uri5', 'uri6'],
            'text': [
                'Pre-election #politics post',
                'Another #politics post',
                'Post-election #election post',
                'Pre-election #campaign post',
                'Post-election #results post',
                'Another #results post'
            ]
        })
    
    def test_create_stratified_hashtag_analysis(self):
        """Test stratified hashtag analysis."""
        result = create_stratified_hashtag_analysis(
            self.user_df,
            self.user_to_content_in_feeds,
            self.posts_data
        )
        
        # Check structure
        self.assertIn('control', result)
        self.assertIn('treatment', result)
        self.assertIn('pre_election', result['control'])
        self.assertIn('post_election', result['control'])
        
        # Check that hashtags are filtered by frequency
        for condition in result.values():
            for period in condition.values():
                for hashtag, count in period.items():
                    self.assertGreaterEqual(count, MIN_HASHTAG_FREQUENCY)


class TestDataFrameCreation(unittest.TestCase):
    """Test DataFrame creation and validation."""
    
    def setUp(self):
        """Set up test data."""
        self.stratified_results = {
            'control': {
                'pre_election': {'politics': 10, 'campaign': 8},
                'post_election': {'election': 12, 'results': 6}
            },
            'treatment': {
                'pre_election': {'politics': 15, 'campaign': 12},
                'post_election': {'election': 18, 'results': 9}
            }
        }
    
    def test_create_hashtag_dataframe(self):
        """Test DataFrame creation from stratified results."""
        df = create_hashtag_dataframe(self.stratified_results)
        
        # Check columns
        expected_columns = ['condition', 'pre_post_election', 'hashtag', 'count', 'proportion']
        self.assertEqual(list(df.columns), expected_columns)
        
        # Check data
        self.assertEqual(len(df), 8)  # 2 conditions × 2 periods × 2 hashtags each
        
        # Check proportions sum to 1 for each condition-period combination
        for condition in df['condition'].unique():
            for period in df['pre_post_election'].unique():
                subset = df[(df['condition'] == condition) & (df['pre_post_election'] == period)]
                if not subset.empty:
                    self.assertAlmostEqual(subset['proportion'].sum(), 1.0, places=5)
    
    def test_validate_hashtag_data(self):
        """Test hashtag data validation."""
        # Valid data
        valid_df = pd.DataFrame({
            'condition': ['control', 'control'],
            'pre_post_election': ['pre_election', 'post_election'],
            'hashtag': ['politics', 'election'],
            'count': [10, 12],
            'proportion': [0.5, 0.6]
        })
        self.assertTrue(validate_hashtag_data(valid_df))
        
        # Invalid data - negative counts
        invalid_df = valid_df.copy()
        invalid_df.loc[0, 'count'] = -1
        self.assertFalse(validate_hashtag_data(invalid_df))
        
        # Invalid data - invalid proportions
        invalid_df = valid_df.copy()
        invalid_df.loc[0, 'proportion'] = 1.5
        self.assertFalse(validate_hashtag_data(invalid_df))


class TestVisualization(unittest.TestCase):
    """Test visualization functionality."""
    
    def setUp(self):
        """Set up test data."""
        self.test_df = pd.DataFrame({
            'condition': ['control', 'control', 'treatment', 'treatment'] * 2,
            'pre_post_election': ['pre_election', 'post_election'] * 4,
            'hashtag': ['politics', 'election', 'politics', 'election'] * 2,
            'count': [10, 12, 15, 18, 8, 6, 12, 9],
            'proportion': [0.5, 0.6, 0.75, 0.9, 0.4, 0.3, 0.6, 0.45]
        })
    
    def test_create_top_hashtags_by_condition_chart(self):
        """Test top hashtags by condition chart creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test_chart.png')
            create_top_hashtags_by_condition_chart(
                self.test_df, output_path, top_n=5
            )
            self.assertTrue(os.path.exists(output_path))
    
    def test_create_pre_post_election_comparison_chart(self):
        """Test pre/post election comparison chart creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test_comparison.png')
            create_pre_post_election_comparison_chart(
                self.test_df, output_path, top_n=5
            )
            self.assertTrue(os.path.exists(output_path))
    
    def test_create_hashtag_frequency_distribution_chart(self):
        """Test frequency distribution chart creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test_distribution.png')
            create_hashtag_frequency_distribution_chart(
                self.test_df, output_path
            )
            self.assertTrue(os.path.exists(output_path))
    
    def test_create_all_visualizations(self):
        """Test creation of all visualizations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            create_all_visualizations(
                self.test_df, temp_dir, "test_timestamp", top_n=5
            )
            
            # Check that all expected files were created
            expected_files = [
                'top_hashtags_by_condition_test_timestamp.png',
                'pre_post_election_comparison_test_timestamp.png',
                'hashtag_frequency_distribution_test_timestamp.png',
                'condition_election_heatmap_test_timestamp.png'
            ]
            
            for filename in expected_files:
                filepath = os.path.join(temp_dir, filename)
                self.assertTrue(os.path.exists(filepath), f"File {filename} not created")


class TestIntegration(unittest.TestCase):
    """Test integration between modules."""
    
    @patch('hashtag_analysis.load_user_data')
    @patch('hashtag_analysis.get_post_uris_used_in_feeds_per_user_per_day')
    @patch('hashtag_analysis.get_all_post_uris_used_in_feeds')
    @patch('hashtag_analysis.load_preprocessed_posts_by_uris')
    def test_end_to_end_analysis(self, mock_load_posts, mock_get_all_uris, 
                                 mock_get_feeds, mock_load_users):
        """Test end-to-end analysis workflow."""
        # Mock data
        mock_load_users.return_value = (
            pd.DataFrame({'bluesky_user_did': ['user1'], 'condition': ['control']}),
            pd.DataFrame(),
            {'user1'}
        )
        mock_get_feeds.return_value = {'user1': {'2024-11-01': {'uri1'}}}
        mock_get_all_uris.return_value = {'uri1'}
        mock_load_posts.return_value = pd.DataFrame({
            'uri': ['uri1'],
            'text': ['This is a #test post']
        })
        
        # Test that the analysis can run without errors
        from main import do_setup, create_stratified_hashtag_analysis, create_hashtag_dataframe
        
        setup_objs = do_setup()
        stratified_results = create_stratified_hashtag_analysis(
            setup_objs['user_df'],
            setup_objs['user_to_content_in_feeds'],
            setup_objs['posts_data']
        )
        df = create_hashtag_dataframe(stratified_results)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestHashtagExtraction,
        TestHashtagAnalysis,
        TestStratifiedAnalysis,
        TestDataFrameCreation,
        TestVisualization,
        TestIntegration
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
