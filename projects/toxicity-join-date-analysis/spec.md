# 🧾 Spec: Toxicity vs Join Date Analysis

## 1. Problem Statement
**Who is affected?** Billy (research stakeholder) needs analysis of whether users who joined Bluesky toward the end of the study period were more likely to post toxic/outrage content compared to users who joined earlier.

**What's the pain point?** Missing understanding of relationship between user join timing and content toxicity patterns, with Wednesday afternoon deadline approaching.

**Why now?** Billy remembered discussing this analysis and needs it for upcoming research deliverable.

**Strategic link?** Part of ongoing research into Bluesky user behavior and content patterns to understand platform evolution and user dynamics.

## 2. Desired Outcomes & Metrics
**What should be true once this is done?**
- Exploratory analysis showing relationship between join dates and toxicity/outrage levels
- Clear visualizations demonstrating any patterns or correlations
- Data available for presentation by Wednesday afternoon

**How do we measure it?**
- Number of users successfully sampled
- Number of API calls made vs. rate limits
- Completion of both visualization types (histogram and scatterplot)
- Data quality metrics (users with sufficient posts, valid join dates)

**Success criteria:**
- Analysis completed with visualizations generated
- Both histogram and scatterplot created
- Data ready for research presentation

## 3. In Scope / Out of Scope
**In Scope:**
- Analysis of users with posts in Perspective API dataset
- Sampling users based on post frequency (≥K posts threshold)
- Bluesky API calls to get join dates for sampled users
- Calculation of average `prob_toxic` and `prob_moral_outrage` per user
- Generation of histogram: joined month/date vs average probability for toxicity/outrage
- Generation of scatterplot: average toxicity/outrage vs +/- number of months joined relative to study start

**Out of Scope:**
- Statistical significance testing
- Minimum sample size requirements
- Fallback plans if API limits are hit
- Comprehensive user behavior analysis beyond toxicity/outrage
- User-facing UI changes

## 4. Stakeholders & Dependencies
**Stakeholders:**
- Billy (primary stakeholder, needs results by Wednesday afternoon)
- Research team analyzing Bluesky user behavior patterns

**Dependencies:**
- Bluesky API (getProfile endpoint) for user join dates
- Existing Perspective API data
- Existing Bluesky API integration code with rate limit handling
- Data storage systems (bluesky_research_data)
- Data processing and visualization infrastructure

**Cross-functional requirements:**
- No legal, privacy, or compliance review needed
- No launch coordination or support training required

## 5. Risks / Unknowns
**Technical Risks:**
- Bluesky API rate limits may severely constrain sample size
- Potential sampling bias toward users with more posts in database
- Data quality issues (missing join dates, insufficient posts per user)

**Edge Cases:**
- API rate limit exhaustion before completing analysis
- Users with very few posts (threshold to be determined during implementation)
- Missing or invalid join dates from API
- Empty or insufficient samples

**Unknowns requiring discovery:**
- Optimal sample size given API constraints
- Distribution of post frequencies per user
- Actual API call limits and timing
- Minimum posts threshold for meaningful analysis

## 6. UX Notes & Accessibility
**User Impact:**
- No user-facing UI changes
- Research analysis project with no direct user impact

**Visualization Requirements:**
- Histogram: Joined month/date vs average probability for toxicity/outrage
- Scatterplot: Average toxicity/outrage vs +/- number of months joined relative to study start (2024-09-30)
- Standard data visualization practices (no accessibility requirements)

**Design System:**
- Use existing data processing and visualization infrastructure
- Standard plotting libraries (matplotlib/seaborn)

## 7. Technical Notes
**Implementation Approach:**
- Multi-phase implementation following detailed plan in brain dump
- Phase 1: User frequency analysis (multithreaded processing)
- Phase 2: User sampling based on post frequency threshold
- Phase 3: Bluesky API calls for join dates
- Phase 4: Analysis and data structure creation
- Phase 5: Visualization generation

**Architecture:**
- Leverage existing Bluesky API integration code
- Use existing data storage patterns
- Build on current data processing infrastructure

**Data Structure:**
```python
class AuthorToAverageToxicityOutrage:
    author_did: str
    join_date: str
    average_toxicity_of_posts: float  # prob_toxic (0-1)
    average_outrage_of_posts: float   # prob_moral_outrage (0-1)
```

**Storage Structure:**
```
frequent_posters_author_metadata/
    metadata.json          # runtime metadata
    sample.json           # sample of users by DID
    dids_to_profiles.json # user DIDs to full profiles
```

## 8. Compliance, Cost, GTM
**Legal/Privacy:**
- No special compliance requirements
- Using existing data infrastructure and API access

**Cost:**
- No additional infrastructure costs
- API usage within existing limits
- Development time only

**GTM:**
- No go-to-market implications
- Internal research analysis only

## 9. Success Criteria
**Measurable definition of "done":**
- ✅ User frequency analysis completed across all study days
- ✅ Sample of users selected based on post frequency threshold
- ✅ Bluesky API calls completed for join date collection
- ✅ Average toxicity/outrage calculated per user
- ✅ Histogram generated: joined month/date vs average probability
- ✅ Scatterplot generated: average toxicity/outrage vs months relative to study start
- ✅ Data and visualizations ready for presentation by Wednesday afternoon
- ✅ Analysis results stored in structured format for future reference

**Acceptance criteria:**
- Analysis pipeline runs successfully end-to-end
- Both required visualizations are generated
- Data quality is sufficient for exploratory analysis
- Results are available by Wednesday afternoon deadline
