# Testing Plan for Engagement Aggregation Logic

This document outlines the comprehensive test plan for the engagement aggregation logic in `get_agg_labels_for_engagements.py`. Each test case is designed to cover a specific scenario or edge case, ensuring correctness and robustness of the implementation.

---

## 1. No Engagements for a User
- **Scenario:**
  - A user exists in the study but has no engagement records (e.g., User 5 in the experiment).
- **Test:**
  - The output for this user should be present and filled with `None` or `0` as appropriate for all metrics.
  - The user should not be omitted from the results.

## 2. Multiple Engagement Types for a User
- **Scenario:**
  - A user engages with content in multiple ways (like, post, repost, reply) across multiple posts and weeks (e.g., User 1 and User 3).
- **Test:**
  - All engagement types are correctly aggregated and mapped to the right user, date, and week.
  - Each engagement type's metrics are computed independently.

## 3. Engagements Across Multiple Weeks
- **Scenario:**
  - Engagements are distributed across several weeks, and the week mapping is non-trivial.
- **Test:**
  - Weekly aggregation logic correctly groups and averages by week, not just by date.
  - Each user's weekly metrics reflect the correct aggregation of their daily metrics.

## 4. Overlapping Engagements (Same Post, Multiple Types)
- **Scenario:**
  - A user both likes, replies, and reposts the same post (e.g., User 3).
- **Test:**
  - The same post can appear in multiple engagement type aggregations for the same user and date.
  - Each engagement type is counted independently for the same post.

## 5. Sparse Engagements (Single Engagement)
- **Scenario:**
  - A user has only a single engagement (e.g., User 7).
- **Test:**
  - The code handles this without error and produces the correct output for that user and engagement type.

## 6. Posts with No Labels
- **Scenario:**
  - Some engaged posts have no associated labels (missing from the label dict).
- **Test:**
  - The code handles missing label data gracefully, outputting `None` for those aggregations.
  - No errors are raised due to missing label data.

## 7. All Engagements of a Type are None
- **Scenario:**
  - For a given user/date/type, all posts have no valid label for a given metric.
- **Test:**
  - The output is strictly `None` for that metric.
  - The user and metric are still present in the output.

## 8. Multiple Users, Same Post
- **Scenario:**
  - Multiple users engage with the same post.
- **Test:**
  - The engagement is correctly attributed to each user independently.
  - Each user's metrics are computed based on their own engagements, even if the post is the same.

---

## 9. Integrated Multi-User, Multi-Post, Multi-Engagement-Type Scenarios
- **Rationale:**
  - To ensure the aggregation logic works correctly in realistic, complex scenarios with multiple users, posts, engagement types, and weeks, as described in `experiment_get_agg_labels_for_engagement.py`.
- **Test Structure:**
  - Construct a scenario with at least 7 users, 9 posts, and a mix of engagement types (like, post, repost, reply) distributed across multiple weeks.
  - Users should have overlapping and non-overlapping engagements, including:
    - Users with no engagement (e.g., User 5)
    - Users with only one engagement (e.g., User 7)
    - Users with multiple engagement types on the same post (e.g., User 3)
    - Multiple users engaging with the same post (e.g., Users 1 and 4 both like post 4)
    - Engagements spread across at least 2-3 weeks
    - Some posts missing label data
- **Example User/Post Matrix:**
  - **Users:**
    - User 1: Likes and reposts several posts, posts some content
    - User 2: Likes and replies, but no posts
    - User 3: Likes, replies, reposts, and posts, sometimes all on the same post
    - User 4: Sparse likes
    - User 5: No engagement
    - User 6: Only reposts and likes
    - User 7: Only one like
  - **Posts:**
    - 9 posts (uri_1 to uri_9), distributed across 3 weeks
    - Some posts have no label data
- **Expected Outcomes:**
  - All users are present in the output, including those with no engagement
  - Each user's metrics are correct for each engagement type and week
  - Overlapping and sparse engagement patterns are handled correctly
  - Missing label data results in `None` for those metrics
  - Weekly aggregation is correct for all users

---

## 10. Integration-Style Tests for Each Major Function

### 10.1 get_content_engaged_with_per_user (Complex Scenario)
- **Input:**
  - Dictionary mapping each post URI to a list of engagement dicts (user, date, type) for all users, posts, and engagement types as described above.
- **Expected Behavior:**
  - All users are present in the output, including users with no engagement (should have empty dicts).
  - Each user's engagements are correctly grouped by date and type.
  - Posts with multiple engagement types and users are correctly mapped.
- **Key Assertions:**
  - `result.keys()` matches all user IDs
  - Users with no engagement have empty dicts
  - Users with multiple types/dates have correct nested structure

### 10.2 get_labels_for_engaged_content (Complex Scenario)
- **Input:**
  - List of all post URIs, with a label dict for each (some missing labels).
- **Expected Behavior:**
  - All posts with labels have correct label data in the output.
  - Posts without labels have empty dicts.
- **Key Assertions:**
  - For posts with labels, output matches label dict
  - For posts without labels, output is empty dict

### 10.3 get_per_user_per_day_content_label_proportions (Complex Scenario)
- **Input:**
  - ENGAGEMENTS dict (user/date/type -> uris) and LABELS dict (some posts missing labels).
- **Expected Behavior:**
  - Proportions are computed for each user/date/type, with None for missing labels.
  - Users with no engagement have empty dicts.
- **Key Assertions:**
  - Users with no engagement: `{}`
  - Users with missing label data: None for those metrics
  - Users with valid data: correct proportions

### 10.4 get_per_user_to_weekly_content_label_proportions (Complex Scenario)
- **Input:**
  - Daily proportions from previous function, and a DataFrame mapping user/date to week.
- **Expected Behavior:**
  - Weekly averages are computed, ignoring None values.
  - Users with no engagement have all None for metrics.
- **Key Assertions:**
  - Users with no engagement: all None for metrics
  - Users with valid data: correct weekly averages

### 10.5 transform_per_user_to_weekly_content_label_proportions (Complex Scenario)
- **Input:**
  - Weekly proportions from previous function, and user metadata.
- **Expected Behavior:**
  - Output DataFrame includes all users, correct columns, and correct values for each user/metric.
- **Key Assertions:**
  - All users present in DataFrame
  - Metrics columns present
  - Users with no engagement: None or 0 for metrics

---

## 11. Full Integration Flow Test
- **Input:**
  - The full set of users, posts, engagements, and labels as above.
- **Flow:**
  1. Run `get_content_engaged_with_per_user` to map all engagements
  2. Run `get_labels_for_engaged_content` to aggregate label data
  3. Run `get_per_user_per_day_content_label_proportions` to compute daily proportions
  4. Run `get_per_user_to_weekly_content_label_proportions` to aggregate to weekly
  5. Run `transform_per_user_to_weekly_content_label_proportions` to produce the final DataFrame
- **Expected Behavior:**
  - All users are present in the final DataFrame
  - Users with no engagement have None or 0 for all metrics
  - Users with valid engagement have correct metrics
  - Posts with missing labels result in None for those metrics
- **Key Assertions:**
  - DataFrame includes all users
  - User 5 (no engagement) has None/0 for all metrics
  - User 1 (active) has non-None for at least one metric

---

## Additional Notes
- The tests for "no engagement" and "all None" are strict: the user must be present in the output, and the relevant metrics must be explicitly `None` or `0`.
- It is acceptable for the post author to not be in the study; only the user performing the engagement (e.g., the liker) must be a study user. This is reflected in the test data and logic.
- The plan does not include a test for filtering out posts authored by non-study users, as this is not required.

---

This plan is intended to ensure that all relevant edge cases and scenarios from the experimental integration file are covered in the unit tests, providing confidence in the correctness and robustness of the engagement aggregation logic. 