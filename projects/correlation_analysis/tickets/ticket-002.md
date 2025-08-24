# Ticket 002: Feed Selection Bias Analysis

**Linear Issue**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)

## Research Question
"Assuming the above comes out clean, look at the correlation of toxicity x constructiveness on all posts used in feeds, to see if there's anything in the algorithmic selection that causes this bias."

## Objective
Analyze correlations in posts used in feeds to detect algorithmic selection biases that could create artificial correlations between toxicity and constructiveness.

## Tasks
- [ ] Load posts used in feeds data locally (manageable volume)
- [ ] Implement local processing for feed data analysis
- [ ] Calculate correlations between toxicity and constructiveness for feed posts
- [ ] Compare correlation patterns between baseline and feed-selected posts
- [ ] Implement bias detection metrics and analysis
- [ ] Generate comparison reports and visualizations

## Acceptance Criteria
- [ ] Feed selection bias analysis implemented
- [ ] Local processing for feed data working correctly
- [ ] Comparison between baseline and feed correlations working
- [ ] Bias detection metrics implemented
- [ ] Clear analysis of whether algorithmic bias exists

## Dependencies
- Completion of baseline correlation analysis (Ticket 001)
- Access to feed data via existing utilities

## Effort Estimate
1 week

## Implementation Notes
- This can be done locally since feed data volume is manageable
- Focus on comparing patterns with baseline results
- Look for systematic differences that could indicate algorithmic bias
- Document findings clearly for research team
