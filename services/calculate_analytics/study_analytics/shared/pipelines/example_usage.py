"""Example usage of the simple pipeline framework.

This module demonstrates how to use the pipeline framework for one-off research analyses.
The focus is on simple, direct execution rather than complex orchestration.
"""

from services.calculate_analytics.study_analytics.shared.pipelines import (
    FeedAnalysisPipeline,
    WeeklyThresholdsPipeline,
    EngagementAnalysisPipeline,
)


def example_feed_analysis():
    """Example of running feed analysis pipeline."""
    print("=== Feed Analysis Pipeline Example ===")

    try:
        # Create pipeline instance
        pipeline = FeedAnalysisPipeline("feed_analysis_example")

        # Configure pipeline parameters
        pipeline.set_partition_date("2024-10-15")

        # Execute pipeline
        print("Executing feed analysis pipeline...")
        result = pipeline.run()

        # Check results
        if result.success:
            print("✅ Feed analysis completed successfully!")
            print(f"   Processed data: {type(result.data)}")
            print(f"   Execution time: {result.execution_time:.2f}s")
            print(f"   Metadata: {result.metadata}")

            # Export results if needed
            if result.data is not None:
                output_file = "feed_analysis_results.csv"
                result.data.to_csv(output_file, index=False)
                print(f"   Results exported to: {output_file}")
        else:
            print(f"❌ Feed analysis failed: {result.error}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def example_weekly_thresholds():
    """Example of running weekly thresholds pipeline."""
    print("\n=== Weekly Thresholds Pipeline Example ===")

    try:
        # Create pipeline instance
        pipeline = WeeklyThresholdsPipeline("weekly_thresholds_example")

        # Configure pipeline parameters
        pipeline.set_time_period("2024-10-01", "2024-10-31")

        # Execute pipeline
        print("Executing weekly thresholds pipeline...")
        result = pipeline.run()

        # Check results
        if result.success:
            print("✅ Weekly thresholds completed successfully!")
            print(f"   Generated thresholds: {type(result.data)}")
            print(f"   Execution time: {result.execution_time:.2f}s")
            print(f"   Metadata: {result.metadata}")

            # Export results if needed
            if result.data is not None:
                output_file = "weekly_thresholds_results.csv"
                result.data.to_csv(output_file, index=False)
                print(f"   Results exported to: {output_file}")
        else:
            print(f"❌ Weekly thresholds failed: {result.error}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def example_engagement_analysis():
    """Example of running engagement analysis pipeline."""
    print("\n=== Engagement Analysis Pipeline Example ===")

    try:
        # Create pipeline instance
        pipeline = EngagementAnalysisPipeline("engagement_analysis_example")

        # Configure pipeline parameters
        pipeline.set_analysis_period("2024-10-01", "2024-10-31")

        # Execute pipeline
        print("Executing engagement analysis pipeline...")
        result = pipeline.run()

        # Check results
        if result.success:
            print("✅ Engagement analysis completed successfully!")
            print(f"   Analyzed data: {type(result.data)}")
            print(f"   Execution time: {result.execution_time:.2f}s")
            print(f"   Metadata: {result.metadata}")

            # Export results if needed
            if result.data is not None:
                output_file = "engagement_analysis_results.csv"
                result.data.to_csv(output_file, index=False)
                print(f"   Results exported to: {output_file}")
        else:
            print(f"❌ Engagement analysis failed: {result.error}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def example_custom_configuration():
    """Example of using custom configuration with pipelines."""
    print("\n=== Custom Configuration Example ===")

    try:
        # Custom configuration for feed analysis
        custom_config = {
            "exclude_partition_dates": ["2024-10-08", "2024-10-09"],
            "default_label_threshold": 0.7,
            "load_unfiltered_posts": False,
        }

        # Create pipeline with custom configuration
        pipeline = FeedAnalysisPipeline("custom_feed_analysis", config=custom_config)

        # Configure pipeline parameters
        pipeline.set_partition_date("2024-10-15")

        # Execute pipeline
        print("Executing custom feed analysis pipeline...")
        result = pipeline.run()

        # Check results
        if result.success:
            print("✅ Custom feed analysis completed successfully!")
            print(f"   Configuration used: {custom_config}")
            print(f"   Execution time: {result.execution_time:.2f}s")
        else:
            print(f"❌ Custom feed analysis failed: {result.error}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def example_error_handling():
    """Example of error handling with pipelines."""
    print("\n=== Error Handling Example ===")

    try:
        # Create pipeline instance
        pipeline = FeedAnalysisPipeline("error_handling_example")

        # Try to execute without setting required parameters
        print("Executing pipeline without required parameters...")
        result = pipeline.run()

        # Check results
        if result.success:
            print("✅ Pipeline completed successfully!")
        else:
            print(f"❌ Pipeline failed as expected: {result.error}")
            print("   This demonstrates proper error handling")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def example_pipeline_status():
    """Example of checking pipeline status."""
    print("\n=== Pipeline Status Example ===")

    try:
        # Create pipeline instance
        pipeline = FeedAnalysisPipeline("status_example")

        # Check initial status
        print("Initial pipeline status:")
        status = pipeline.get_status()
        print(f"   Name: {status['name']}")
        print(f"   State: {status['state']}")
        print(f"   Start time: {status['start_time']}")
        print(f"   End time: {status['end_time']}")
        print(f"   Execution time: {status['execution_time']}")
        print(f"   Error: {status['error']}")
        print(f"   Metadata: {status['metadata']}")

        # Execute pipeline
        print("\nExecuting pipeline...")
        pipeline.run()

        # Check final status
        print("\nFinal pipeline status:")
        status = pipeline.get_status()
        print(f"   State: {status['state']}")
        print(f"   Start time: {status['start_time']}")
        print(f"   End time: {status['end_time']}")
        print(f"   Execution time: {status['execution_time']}")
        print(f"   Error: {status['error']}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def example_sequential_execution():
    """Example of running multiple pipelines sequentially (simple approach)."""
    print("\n=== Sequential Execution Example ===")

    pipelines = [
        ("Feed Analysis", FeedAnalysisPipeline("feed_sequential")),
        ("Weekly Thresholds", WeeklyThresholdsPipeline("thresholds_sequential")),
        ("Engagement Analysis", EngagementAnalysisPipeline("engagement_sequential")),
    ]

    results = []

    for name, pipeline in pipelines:
        print(f"\n--- Executing {name} ---")

        try:
            # Configure pipeline
            if name == "Feed Analysis":
                pipeline.set_partition_date("2024-10-15")
            elif name == "Weekly Thresholds":
                pipeline.set_time_period("2024-10-01", "2024-10-31")
            elif name == "Engagement Analysis":
                pipeline.set_analysis_period("2024-10-01", "2024-10-31")

            # Execute pipeline
            result = pipeline.run()
            results.append((name, result))

            # Report results
            if result.success:
                print(
                    f"✅ {name} completed successfully in {result.execution_time:.2f}s"
                )
            else:
                print(f"❌ {name} failed: {result.error}")

        except Exception as e:
            print(f"❌ {name} encountered unexpected error: {e}")
            results.append((name, None))

    # Summary
    print("\n--- Execution Summary ---")
    successful = sum(1 for _, result in results if result and result.success)
    total = len(results)
    print(f"Successful pipelines: {successful}/{total}")

    for name, result in results:
        if result and result.success:
            print(f"   ✅ {name}: {result.execution_time:.2f}s")
        else:
            print(f"   ❌ {name}: Failed")


def main():
    """Run all examples."""
    print("Simple Pipeline Framework - Usage Examples")
    print("=" * 50)
    print(
        "This demonstrates simple, direct pipeline execution for one-off research analyses."
    )
    print("No complex orchestration - just straightforward pipeline usage.\n")

    # Run examples
    example_feed_analysis()
    example_weekly_thresholds()
    example_engagement_analysis()
    example_custom_configuration()
    example_error_handling()
    example_pipeline_status()
    example_sequential_execution()

    print("\n" + "=" * 50)
    print("All examples completed!")
    print("\nKey Points:")
    print("- Each pipeline runs independently")
    print("- Simple, direct execution pattern")
    print("- No complex orchestration needed")
    print("- Easy to understand and maintain")
    print("- Perfect for one-off research analyses")


if __name__ == "__main__":
    main()
