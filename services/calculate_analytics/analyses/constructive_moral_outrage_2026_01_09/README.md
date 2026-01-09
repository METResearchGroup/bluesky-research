# Investigating the relationship between constructive and moral outrage content

We have this thesis that there is a subset of posts...

(put examples of content with both constructive + moral outrage, from Excel sheet)

(put results from Athena queries that I went over in the meeting)

Steps:

1. Load labels from S3.
2. Load feeds from S3.
3. Calculate the daily constructiveness averages. Looks like "services/calculate_analytics/shared/analysis/content_analysis.py" might be helpful in terms of the logic? (though obviously my use case is much simpler).
4. Rerun the visualizations from Billy.
