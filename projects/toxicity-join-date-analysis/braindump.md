# Toxicity vs Join Date Analysis - Brain Dump

## Project Overview
Analysis to investigate whether users who joined Bluesky toward the end of the study period were more likely to post toxic/outrage content compared to users who joined earlier.

## Core Research Question
Are users who joined Bluesky later in the study period more likely to post toxic/outrage content than users who joined earlier?

## Context from Slack Conversation

### Billy's Original Request
- Sample users who appear in firehose at end of study
- Scatter plot: user first post/reshare date (join date proxy) vs mean toxicity
- Random sample of N users, repeat sampling 5 times for reliability
- Timeline: Wednesday afternoon deadline

### Mark's Technical Constraints
- Can't use first post date as join date proxy (users may have posted before study)
- Need Bluesky API calls to get actual join dates
- API rate limits will be a major constraint
- Even with sampling, likely to hit daily API limits quickly

### Proposed Solution (Streamlined Shortcut)
1. Find ~1,000 random users with ≥K posts in existing database
2. Poll Bluesky API for join dates
3. Correlate join date with average toxicity/outrage
4. Hope for decent results despite sampling bias

## Implementation Plan (Mark's Notes)

### Phase 1: User Frequency Analysis
- Iterate through daily Perspective API posts
- Count how often each user appears per day
- Save output: "authors_to_labeled_posts_per_day"
- Make multithreaded for performance
- Store in bluesky_research_data

### Phase 2: User Sampling
- Collapse across all days for master list
- Analyze distribution of post frequencies
- Set threshold (≥K posts or ith percentile)
- Randomly sample users from subsample
- Call getProfile API for join dates

### Phase 3: Data Storage Structure
```
frequent_posters_author_metadata/
    metadata.json          # runtime metadata
    sample.json           # sample of users by DID
    dids_to_profiles.json # user DIDs to full profiles
```

### Phase 4: Analysis
- Calculate average `prob_toxic` and `prob_moral_outrage` per user
- Create data structure:
```python
class AuthorToAverageToxicityOutrage:
    author_did: str
    join_date: str
    average_toxicity_of_posts: float  # prob_toxic (0-1)
    average_outrage_of_posts: float   # prob_moral_outrage (0-1)
```

### Phase 5: Visualization
- **Histogram**: Joined month/date vs average probability for toxicity/outrage
- **Scatterplot**: Average toxicity/outrage vs +/- number of months joined relative to study start (2024-09-30)
- Convert join date to months relative to study start for correlation analysis

## Key Questions Needing Answers

### Data Scope - RESOLVED
- **Study time range**: Wave 1: 2024-09-30 to 2024-11-24, Wave 2: 2024-10-07 to 2024-12-01
- **Total study period**: 2024-09-30 to 2024-12-01 (inclusive)
- **Content lookback**: 2024-09-15 (for posts that may have appeared in feeds)
- How many total users are in the Perspective API dataset? (TBD during implementation)
- What's the distribution of posts per user? (TBD during implementation)

### Technical Implementation - RESOLVED
- **Bluesky API integration**: Existing code available for API calls and rate limit handling
- How many API calls can we realistically make in the timeframe? (TBD based on existing code)
- What's the optimal sample size given constraints? (Exploratory - no minimum requirements)
- **Existing infrastructure**: Bluesky API integration code is available

### Metrics and Analysis - RESOLVED
- **Toxicity metrics**: `prob_toxic` and `prob_moral_outrage` from Perspective API
- **Scoring scale**: Probability floats, 0-1 range
- **Statistical significance**: Exploratory analysis - no significance requirements (but note for future exploration)
- **User post threshold**: To be determined during implementation based on data distribution

## Remaining Questions for Implementation
1. **Sample Size**: What's a reasonable target sample size for the exploratory analysis?
2. **Data Storage Location**: Where should intermediate and final results be stored?
3. **User Post Threshold**: Minimum number of posts per user (will be determined by examining data distribution)

## Visualization Requirements - RESOLVED
1. **Histogram**: Joined month/date vs average probability for toxicity/outrage
2. **Scatterplot**: Average toxicity/outrage vs +/- number of months joined relative to study start

### Success Criteria - RESOLVED
- **Analysis type**: Exploratory analysis
- **No minimum sample size requirements**
- **No statistical significance thresholds**
- **Visualization requirements**: To be determined later

### Fallback Plans - RESOLVED
- **No fallback plans needed** - proceed with streamlined approach

## Potential Risks and Challenges

### API Limitations
- Bluesky API rate limits may severely constrain sample size
- No guarantee that sampled users will have join dates in relevant range
- API calls may fail or timeout

### Sampling Bias
- Users with more posts in database are more likely to be followed by study participants
- May not represent general Bluesky user population
- Could skew results toward more active/visible users

### Data Quality
- Join dates may not be accurate or complete
- Toxicity scores may have measurement error
- Small sample sizes may lead to unreliable correlations

### Timeline Constraints
- Wednesday afternoon deadline is tight
- API rate limits may require multiple days
- No time for extensive iteration or refinement

## Technical Dependencies

### Existing Infrastructure
- Bluesky API integration
- Perspective API data processing
- Data storage systems (bluesky_research_data)
- Analysis and visualization tools

### Required Tools
- Python environment with required packages
- Multithreading capabilities for performance
- Data visualization libraries (matplotlib, seaborn, plotly?)
- JSON handling for API responses

## Potential Extensions
- Multiple sampling iterations for reliability
- Different time period comparisons
- Additional user metadata analysis
- Cross-validation with other data sources

## Next Steps
1. Clarify study time range and data scope
2. Assess current API integration capabilities
3. Determine realistic sample size given constraints
4. Set up data processing pipeline
5. Implement user frequency analysis
6. Execute sampling and API calls
7. Perform correlation analysis
8. Create visualizations
9. Document results and limitations
