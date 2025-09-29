# Implement toxicity vs join date analysis pipeline

## Context & Motivation
Billy needs exploratory analysis of whether users who joined Bluesky toward the end of the study period (2024-09-30 to 2024-12-01) were more likely to post toxic/outrage content compared to users who joined earlier. This analysis is required by Wednesday afternoon for an upcoming research deliverable. The analysis will help understand platform evolution and user behavior dynamics by correlating user join dates with their content toxicity patterns.

## Detailed Description & Requirements

### Functional Requirements:
- Process Perspective API labeled posts to count user frequency per day
- Sample users based on post frequency threshold (≥K posts)
- Call Bluesky API getProfile endpoint to retrieve user join dates
- Calculate average `prob_toxic` and `prob_moral_outrage` per user
- Generate histogram: joined month/date vs average probability for toxicity/outrage
- Generate scatterplot: average toxicity/outrage vs +/- number of months joined relative to study start (2024-09-30)
- Store results in structured format with metadata

### Non-Functional Requirements:
- Handle Bluesky API rate limits using existing integration code
- Implement error handling and retry logic for API failures
- Process data efficiently with multithreading where appropriate
- Generate publication-ready visualizations

### Validation & Error Handling:
- Handle missing join dates from API calls
- Handle users with insufficient posts
- Handle API rate limit exhaustion
- Validate data quality throughout the pipeline

## Success Criteria
- User frequency analysis completed across all study days
- Sample of users selected based on post frequency threshold
- Bluesky API calls completed for join date collection
- Average toxicity/outrage calculated per user
- Histogram generated: joined month/date vs average probability
- Scatterplot generated: average toxicity/outrage vs months relative to study start
- Data and visualizations ready for presentation by Wednesday afternoon
- Analysis results stored in structured format

## Test Plan
- `test_user_frequency_analysis`: Process Perspective API data → User frequency counts per day
- `test_user_sampling`: Apply frequency threshold → Sampled user list
- `test_api_profile_calls`: Call getProfile API → User join dates retrieved
- `test_toxicity_calculation`: Calculate averages → User toxicity/outrage scores
- `test_visualization_generation`: Generate plots → Histogram and scatterplot created
- `test_data_storage`: Store results → Structured data files created

## Dependencies
- Depends on: Existing Bluesky API integration code
- Requires: Perspective API labeled posts data
- Requires: Data storage access (bluesky_research_data)
- Requires: Visualization libraries (matplotlib, seaborn)

## Suggested Implementation Plan
- Phase 1: Implement user frequency analysis with multithreading
- Phase 2: Implement user sampling based on frequency threshold
- Phase 3: Implement Bluesky API calls with rate limiting and error handling
- Phase 4: Implement analysis and data structure creation
- Phase 5: Implement visualization generation with publication standards
- Phase 6: Implement data storage and metadata tracking

## Effort Estimate
- Estimated effort: **6 hours**
- Assumes existing Bluesky API integration code is available
- Assumes Perspective API data is accessible
- Assumes visualization libraries are installed

## Priority & Impact
- Priority: **High**
- Rationale: Required by Wednesday afternoon deadline for research deliverable

## Acceptance Checklist
- [x] User frequency analysis implemented and tested
- [x] User sampling logic implemented and tested
- [x] Bluesky API integration implemented with error handling
- [x] Toxicity/outrage calculation implemented and tested
- [x] Histogram visualization generated and saved
- [x] Scatterplot visualization generated and saved
- [x] Data storage structure implemented
- [x] Results validated and ready for presentation
- [x] Code reviewed and merged
- [x] Documentation updated

## Links & References
- Specification: `/projects/toxicity-join-date-analysis/spec.md`
- Brain dump: `/projects/toxicity-join-date-analysis/braindump.md`
- Study constants: `/services/calculate_analytics/shared/constants.py`
- Perspective API models: `/services/ml_inference/models.py`
