# Toxicity vs Join Date Analysis

## Project Overview
Exploratory analysis to investigate whether users who joined Bluesky toward the end of the study period (2024-09-30 to 2024-12-01) were more likely to post toxic/outrage content compared to users who joined earlier.

## Research Question
Are users who joined Bluesky later in the study period more likely to post toxic/outrage content than users who joined earlier?

## Key Deliverables
- Histogram: joined month/date vs average probability for toxicity/outrage
- Scatterplot: average toxicity/outrage vs +/- number of months joined relative to study start
- Structured data analysis results

## Timeline
- **Deadline**: Wednesday afternoon
- **Status**: Ready for implementation

## Project Structure
```
projects/toxicity-join-date-analysis/
├── spec.md              # Complete specification
├── braindump.md         # Initial brainstorming session
├── tickets/
│   └── ticket-001.md    # Implementation ticket
└── README.md            # This file
```

## Tickets
- **Ticket 1**: [Implement toxicity vs join date analysis pipeline](./tickets/ticket-001.md)

## Key Files
- **Specification**: [spec.md](./spec.md)
- **Brain Dump**: [braindump.md](./braindump.md)
- **Study Constants**: `/services/calculate_analytics/shared/constants.py`
- **Perspective API Models**: `/services/ml_inference/models.py`

## Success Criteria
- ✅ User frequency analysis completed across all study days
- ✅ Sample of users selected based on post frequency threshold
- ✅ Bluesky API calls completed for join date collection
- ✅ Average toxicity/outrage calculated per user
- ✅ Histogram generated: joined month/date vs average probability
- ✅ Scatterplot generated: average toxicity/outrage vs months relative to study start
- ✅ Data and visualizations ready for presentation by Wednesday afternoon
- ✅ Analysis results stored in structured format
