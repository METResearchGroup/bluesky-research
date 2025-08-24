"""Example usage of the simplified analysis framework.

This module demonstrates how to use the simplified analysis classes for
one-off research analyses. The focus is on simple, direct method execution
rather than complex pipeline orchestration.
"""

from services.calculate_analytics.study_analytics.shared.pipelines import (
    FeedAnalyzer,
    WeeklyThresholdsAnalyzer,
    EngagementAnalyzer,
)


def example_feed_analysis():
    """Example of running feed analysis."""
    print("=== Feed Analysis Example ===")

    try:
        # Create analyzer instance
        analyzer = FeedAnalyzer("feed_analysis_example")

        # Analyze a specific partition date
        print("Analyzing feed data for partition date 2024-10-15...")
        results = analyzer.analyze_partition_date("2024-10-15")

        # Check results
        if not results.empty:
            print("✅ Feed analysis completed successfully!")
            print(f"   Processed users: {len(results)}")
            print(f"   Columns analyzed: {list(results.columns)}")

            # Export results if needed
            output_file = "feed_analysis_results.csv"
            results.to_csv(output_file, index=False)
            print(f"   Results exported to: {output_file}")
        else:
            print("⚠️  Feed analysis completed but returned no results")

    except Exception as e:
        print(f"❌ Feed analysis failed: {e}")


def example_weekly_thresholds():
    """Example of running weekly thresholds analysis."""
    print("\n=== Weekly Thresholds Analysis Example ===")

    try:
        # Create analyzer instance
        analyzer = WeeklyThresholdsAnalyzer("weekly_thresholds_example")

        # Calculate thresholds for a specific time period
        print("Calculating weekly thresholds for period 2024-10-01 to 2024-10-31...")
        results = analyzer.calculate_thresholds("2024-10-01", "2024-10-31")

        # Check results
        if results:
            print("✅ Weekly thresholds analysis completed successfully!")
            print(f"   Analysis period: {results.get('analysis_period', {})}")
            print(f"   Configuration: {results.get('configuration', {})}")
            print(f"   Summary: {results.get('summary', {})}")

            # Get specific threshold types
            static_thresholds = results.get("static_thresholds")
            if static_thresholds is not None and not static_thresholds.empty:
                print(f"   Static thresholds: {len(static_thresholds)} records")

            dynamic_thresholds = results.get("dynamic_thresholds")
            if dynamic_thresholds is not None and not dynamic_thresholds.empty:
                print(f"   Dynamic thresholds: {len(dynamic_thresholds)} records")
        else:
            print("⚠️  Weekly thresholds analysis completed but returned no results")

    except Exception as e:
        print(f"❌ Weekly thresholds analysis failed: {e}")


def example_engagement_analysis():
    """Example of running engagement analysis."""
    print("\n=== Engagement Analysis Example ===")

    try:
        # Create analyzer instance
        analyzer = EngagementAnalyzer("engagement_analysis_example")

        # Analyze engagement for a specific time period
        print("Analyzing engagement for period 2024-10-01 to 2024-10-31...")
        results = analyzer.analyze_period("2024-10-01", "2024-10-31")

        # Check results
        if results:
            print("✅ Engagement analysis completed successfully!")
            print(f"   Analysis period: {results.get('analysis_period', {})}")
            print(f"   Users analyzed: {results.get('users_analyzed', 0)}")

            # Get specific metrics
            daily_metrics = results.get("daily_metrics")
            if daily_metrics:
                print(f"   Daily metrics calculated for {len(daily_metrics)} users")

            engagement_summary = results.get("engagement_summary")
            if engagement_summary is not None and not engagement_summary.empty:
                print(f"   Engagement summary: {len(engagement_summary)} records")
        else:
            print("⚠️  Engagement analysis completed but returned no results")

    except Exception as e:
        print(f"❌ Engagement analysis failed: {e}")


def example_analysis_summaries():
    """Example of getting analysis summaries."""
    print("\n=== Analysis Summaries Example ===")

    try:
        # Feed analysis summary
        print("Getting feed analysis summary...")
        feed_analyzer = FeedAnalyzer("feed_summary_example")
        feed_summary = feed_analyzer.get_analysis_summary("2024-10-15")
        print(f"   Feed summary: {feed_summary}")

        # Weekly thresholds summary
        print("Getting weekly thresholds summary...")
        thresholds_analyzer = WeeklyThresholdsAnalyzer("thresholds_summary_example")
        thresholds_summary = thresholds_analyzer.get_thresholds_summary(
            "2024-10-01", "2024-10-31"
        )
        print(f"   Thresholds summary: {thresholds_summary}")

        print("✅ All summaries retrieved successfully!")

    except Exception as e:
        print(f"❌ Summary retrieval failed: {e}")


def example_specific_metrics():
    """Example of getting specific metrics from analyzers."""
    print("\n=== Specific Metrics Example ===")

    try:
        # Get just the engagement summary
        print("Getting engagement summary only...")
        engagement_analyzer = EngagementAnalyzer("engagement_metrics_example")
        summary_df = engagement_analyzer.get_engagement_summary(
            "2024-10-01", "2024-10-31"
        )

        if not summary_df.empty:
            print(f"   Retrieved engagement summary with {len(summary_df)} records")
            print(f"   Columns: {list(summary_df.columns)}")
        else:
            print("   No engagement summary data available")

        # Get just the daily metrics
        print("Getting daily metrics only...")
        daily_metrics = engagement_analyzer.get_daily_metrics(
            "2024-10-01", "2024-10-31"
        )

        if daily_metrics:
            print(f"   Retrieved daily metrics for {len(daily_metrics)} users")
        else:
            print("   No daily metrics data available")

        print("✅ Specific metrics retrieved successfully!")

    except Exception as e:
        print(f"❌ Specific metrics retrieval failed: {e}")


def main():
    """Run all examples."""
    print("Running Simplified Analysis Framework Examples")
    print("=" * 50)

    # Run all examples
    example_feed_analysis()
    example_weekly_thresholds()
    example_engagement_analysis()
    example_analysis_summaries()
    example_specific_metrics()

    print("\n" + "=" * 50)
    print("All examples completed!")


if __name__ == "__main__":
    main()
