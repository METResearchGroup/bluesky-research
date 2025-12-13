# Post Length Calculator

A simple example demonstrating how to calculate the average length of social media posts using pandas DataFrames.

## High-level Overview

This service provides functionality to create mock social media posts and calculate statistical metrics about their content, specifically the average character length. It serves as an example of basic data analysis patterns used throughout the social media platform for content analytics.

## Detailed Explanation

The code is organized into composable functions that can be used independently or together:

### Functions

1. **`create_mock_posts() -> pd.DataFrame`**
   - Creates a pandas DataFrame with 10 sample social media posts
   - Each post contains an `id` and `text` field
   - Posts vary in length and content to simulate realistic social media data
   - Returns a structured DataFrame ready for analysis

2. **`calculate_average_post_length(posts_df: pd.DataFrame) -> float`**
   - Takes a DataFrame with a 'text' column as input
   - Calculates the character length of each post using pandas string operations
   - Returns the mean length as a float value
   - Includes error handling for empty DataFrames and missing columns

3. **`main() -> None`**
   - Demonstrates the complete workflow from data creation to analysis
   - Prints detailed output showing each post and its character count
   - Displays the final average calculation

The functions are designed to be composable, allowing you to follow the logic by examining how `main()` orchestrates the calls to `create_mock_posts()` and `calculate_average_post_length()`.

## Testing Details

Tests are located in the `tests/` directory. The test suite covers both unit tests for individual functions and integration tests for the complete workflow.

### Test Files

- **`tests/test_post_length_calculator.py`** - Tests for `post_length_calculator.py`
  - `TestCreateMockPosts` - Tests for the mock data creation function
    - `test_creates_correct_number_of_posts` - Verifies exactly 10 posts are created
    - `test_has_required_columns` - Ensures DataFrame has 'text' and 'id' columns
    - `test_posts_have_unique_ids` - Validates all post IDs are unique
    - `test_all_posts_have_text` - Confirms all posts have non-empty text content
    - `test_posts_are_strings` - Verifies text content is properly typed as strings
  - `TestCalculateAveragePostLength` - Tests for the length calculation function
    - `test_calculates_correct_average_with_mock_data` - Validates calculation with sample data
    - `test_calculates_correct_average_with_simple_data` - Tests with known simple inputs
    - `test_single_post_returns_its_length` - Edge case with single post
    - `test_raises_error_on_empty_dataframe` - Error handling for empty input
    - `test_raises_error_on_missing_text_column` - Error handling for malformed data
    - `test_handles_unicode_characters` - Unicode and emoji support
    - `test_handles_very_long_posts` - Performance with large text content
  - `TestIntegration` - End-to-end workflow tests
    - `test_end_to_end_workflow` - Complete data creation to calculation pipeline
    - `test_mock_posts_content_quality` - Validates realistic post characteristics

## Usage

Run the example:
```bash
python3 post_length_calculator.py
```

Run the tests:
```bash
python3 -m pytest tests/test_post_length_calculator.py -v
```

## Example Output

```
Mock Social Media Posts:
==================================================
Post 1: Just had the best coffee ever! ☕
Length: 32 characters
------------------------------
...
Total posts: 10
Average post length: 58.70 characters
```