# PII Files Removed from calculate_analytics

This document lists all files containing Personally Identifiable Information (PII) that were removed from the `services/calculate_analytics` directory on 2025-10-20.

**Reason for removal**: These files contain user handles and/or DIDs (Decentralized Identifiers) which are considered PII under privacy guidelines.

**Fields identified as PII**:
- `bluesky_user_handle`
- `user_handle`
- `bluesky_user_did`
- `user_did`
- `handle`
- `did`
- `author_did`

**Note**: These files can be restored from git history if needed for legitimate research purposes with appropriate IRB approval.

## CSV Files Removed (31 files)

- ./shared/static/daily_content_label_proportions_per_user_2025-06-16-02:24:41.csv
- ./shared/static/content_classifications_engaged_content_per_user_per_week.csv
- ./shared/static/condition_aggregated.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-09:34:13.csv
- ./analyses/user_feed_analysis_2025_04_08/results/weekly_feed_content_aggregated_results_per_user.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-04:01:47.csv
- ./analyses/user_feed_analysis_2025_04_08/results/weekly_feed_content_aggregated_results_per_user_2025-09-01-09:34:13.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-05:50:25.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-04:07:52.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv
- ./analyses/user_feed_analysis_2025_04_08/results/weekly_feed_content_aggregated_results_per_user_2025-09-01-07:13:40.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-05:31:01.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-08:50:15.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-07:13:40.csv
- ./analyses/user_feed_analysis_2025_04_08/results/weekly_feed_content_aggregated_results_per_user_2025-09-01-08:50:15.csv
- ./analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user_2025-09-01-03:56:12.csv
- ./analyses/prop_in_network_feed_content_2025_09_02/results/weekly_in_network_feed_content_proportions_2025-09-02-01:31:43.csv
- ./analyses/prop_in_network_feed_content_2025_09_02/results/daily_in_network_feed_content_proportions_2025-09-02-01:31:43.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/daily_content_label_proportions_per_user_2025-09-01-12:14:31.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/daily_content_label_proportions_per_user_2025-09-01-09:12:39.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/daily_content_label_proportions_per_user.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/daily_content_label_proportions_per_user_2025-09-01-09:34:13.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/weekly_content_label_proportions_per_user.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/weekly_content_label_proportions_per_user_2025-09-01-09:12:39.csv
- ./analyses/user_engagement_analysis_2025_06_16/results/weekly_content_label_proportions_per_user_2025-09-01-09:34:13.csv
- ./analyses/in_network_out_of_network_feed_label_comparison_2025_09_02/results/2025-09-02-04:01:15/weekly_out_of_network_feed_content_analysis_2025-09-02-04:01:15.csv
- ./analyses/in_network_out_of_network_feed_label_comparison_2025_09_02/results/2025-09-02-04:01:15/daily_out_of_network_feed_content_analysis_2025-09-02-04:01:15.csv
- ./analyses/in_network_out_of_network_feed_label_comparison_2025_09_02/results/2025-09-02-04:21:25/weekly_in_network_feed_content_analysis_2025-09-02-04:21:25.csv
- ./analyses/in_network_out_of_network_feed_label_comparison_2025_09_02/results/2025-09-02-04:21:25/daily_in_network_feed_content_analysis_2025-09-02-04:21:25.csv
- ./analyses/get_baseline_measures_across_all_posts_2025_09_02/results/weekly_baseline_content_label_metrics_2025-09-02-08:21:21.csv
- ./analyses/get_baseline_measures_across_all_posts_2025_09_02/results/daily_baseline_content_label_metrics_2025-09-02-08:21:21.csv

## Parquet Files Removed (34 files)

- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:33:37/user_profiles_chunk_002.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:34:44/user_profiles_chunk_005.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:28:32/user_profiles_chunk_006.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:40:28/user_profiles_chunk_020.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:36:36/user_profiles_chunk_010.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:28:58/user_profiles_chunk_007.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:27:28/user_profiles_chunk_004.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:35:51/user_profiles_chunk_008.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:36:14/user_profiles_chunk_009.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:28:11/user_profiles_chunk_005.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:40:04/user_profiles_chunk_019.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:38:09/user_profiles_chunk_014.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:25:49/user_profiles_chunk_001.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:29:21/user_profiles_chunk_008.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:26:14/user_profiles_chunk_002.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:26:40/user_profiles_chunk_003.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:33:59/user_profiles_chunk_003.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:34:22/user_profiles_chunk_004.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:39:19/user_profiles_chunk_017.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:36:57/user_profiles_chunk_011.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:39:42/user_profiles_chunk_018.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:35:08/user_profiles_chunk_006.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:35:30/user_profiles_chunk_007.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:23:30/user_profiles_chunk_001.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:38:57/user_profiles_chunk_016.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:37:45/user_profiles_chunk_013.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:29:47/user_profiles_chunk_010.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:37:21/user_profiles_chunk_012.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:33:16/user_profiles_chunk_001.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-28_18:29:46/user_profiles_chunk_009.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_user_profiles/2025-09-29_10:38:33/user_profiles_chunk_015.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/results/2025-09-28_16:43:34/aggregated_author_toxicity_outrage.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_users/2025-09-28_17:04:18/sampled_users.parquet
- ./analyses/toxicity_join_date_analysis_2025_09_28/sampled_users/2025-09-29_10:18:04/sampled_users.parquet

## Summary

- Total CSV files with PII: 31
- Total Parquet files with PII: 34
- **Total files removed: 65**

## Removal Details

- **Date**: 2025-10-20
- **Linear Ticket**: MET-62
- **Branch**: remove-pii-from-analytics
