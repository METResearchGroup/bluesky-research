# Testing Plan for Engagement Aggregation Logic

This document outlines the test plan for the engagement aggregation logic in `get_agg_labels_for_engagements.py`, based on the existing test implementation in `test_get_agg_labels_for_engagements.py`.

---

## 1. Unit Tests for Individual Functions

### 1.1 TestGetContentEngagedWithPerUser
- Tests the function that maps users to their engaged content by date and type
- Verifies correct grouping of engagements by user, date, and record type
- Ensures empty dictionaries for users with no engagements
- Tests handling of multiple users engaging with the same content

### 1.2 TestGetLabelsForPartitionDate
- Tests loading and deduplication of label data from storage
- Verifies that duplicate URIs are properly handled by keeping only unique entries

### 1.3 TestGetRelevantProbsForLabel
- Tests extraction of relevant probability values from label data
- Verifies correct handling of different integration types (perspective_api, sociopolitical, ime)
- Ensures proper handling of missing label data

### 1.4 TestGetLabelsForEngagedContent
- Tests aggregation of labels for a list of URIs
- Verifies correct handling of URIs with missing integration data
- Tests tracking and reporting of missing integration data

### 1.5 TestGetColumnPrefixForRecordType
- Tests mapping of engagement record types to column prefixes
- Verifies correct prefix generation for likes, posts, reposts, and replies

### 1.6 TestGetPerUserPerDayContentLabelProportions
- Tests calculation of daily content label proportions for each user
- Verifies correct proportion calculations for different engagement types
- Tests handling of missing label data
- Ensures proper handling of edge cases (no engagements, all None values)

### 1.7 TestGetPerUserToWeeklyContentLabelProportions
- Tests aggregation of daily proportions to weekly averages
- Verifies correct weekly mapping and averaging logic
- Tests handling of None values in the averaging process
- Ensures all users and weeks are properly represented in the output

## 2. Integration Tests

### 2.1 Complex Multi-User Scenario
- Tests the full aggregation pipeline with multiple users, engagement types, and weeks
- Includes users with:
  - Multiple engagement types (likes, posts, reposts, replies)
  - Engagements spread across multiple weeks
  - Varying engagement patterns (some with many, some with few)
  - Some with no engagements for certain types
- Verifies correct proportion calculations for each metric:
  - Toxicity (prob_toxic)
  - Constructiveness (prob_constructive)
  - Sociopolitical content (prob_sociopolitical)
  - Political leaning (left, right, moderate, unclear)
  - Content themes (intergroup, moral, emotion, other)
- Tests weekly aggregation logic with specific expected values for each user/week/metric

### 2.2 Edge Cases
- Tests handling of None values when no engagements exist for a type
- Verifies that users with no engagements still appear in the output
- Tests handling of missing label data for certain posts
- Ensures proper handling of sparse engagement patterns

## 3. Assertion Patterns

The tests use several common assertion patterns:
- Verifying dictionary structure (keys match expected keys)
- Checking for presence of specific users and metrics
- Comparing calculated proportions against expected values
- Handling None values appropriately in comparisons
- Verifying correct aggregation across days to weeks

## 4. Test Data Structure

The tests use carefully constructed test data:
- Mock engagement records with user DIDs, dates, and record types
- Mock label data with various probability values
- Week mapping data to test weekly aggregation
- Expected output structures for verification

---

This testing plan reflects the actual implementation in `test_get_agg_labels_for_engagements.py`, covering unit tests for individual functions as well as integration tests for the complete aggregation pipeline. The tests ensure correct handling of various engagement patterns, label data, and aggregation logic.
