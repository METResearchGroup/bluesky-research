"""Pipeline framework usage examples.

This module provides comprehensive examples of how to use the ABC-based
pipeline framework for various analytics scenarios.
"""

import logging

import pandas as pd

from services.calculate_analytics.study_analytics.shared.pipelines import (
    BaseResearchPipeline,
    FeedAnalysisPipeline,
    WeeklyThresholdsPipeline,
    EngagementAnalysisPipeline,
    PipelineOrchestrator,
    PipelineState,
    PipelineResult,
    PipelineError,
)


def example_basic_pipeline_usage():
    """Example 1: Basic pipeline usage with a single pipeline."""
    print("=== Example 1: Basic Pipeline Usage ===")

    # Create a feed analysis pipeline
    pipeline = FeedAnalysisPipeline("feed_analysis_example")

    # Set the partition date for analysis
    pipeline.set_partition_date("2024-10-15")

    # Run the pipeline with full lifecycle management
    result = pipeline.run()

    # Check results
    if result.success:
        print("✅ Pipeline completed successfully!")
        print(f"📊 Processed {len(result.data)} users")
        print(f"⏱️  Execution time: {result.execution_time:.2f}s")
        print(f"📋 Metadata: {result.metadata}")

        # Access the results data
        results_df = result.data
        if not results_df.empty:
            print("📈 Sample results:")
            print(results_df.head())
    else:
        print(f"❌ Pipeline failed: {result.error}")

    print()


def example_pipeline_configuration():
    """Example 2: Pipeline configuration and customization."""
    print("=== Example 2: Pipeline Configuration ===")

    # Custom configuration for feed analysis
    feed_config = {
        "exclude_partition_dates": ["2024-10-08", "2024-10-09"],
        "default_label_threshold": 0.7,  # Higher threshold for stricter classification
        "load_unfiltered_posts": False,  # Only load filtered posts
    }

    # Create pipeline with custom configuration
    pipeline = FeedAnalysisPipeline("custom_feed_analysis", config=feed_config)

    # Set partition date
    pipeline.set_partition_date("2024-10-15")

    # Run pipeline
    result = pipeline.run()

    if result.success:
        print("✅ Custom pipeline completed!")
        print(f"🔧 Configuration used: {feed_config}")
        print(f"📊 Results: {len(result.data)} users processed")
    else:
        print(f"❌ Custom pipeline failed: {result.error}")

    print()


def example_pipeline_orchestration():
    """Example 3: Orchestrating multiple pipelines."""
    print("=== Example 3: Pipeline Orchestration ===")

    # Create orchestrator
    orchestrator = PipelineOrchestrator("analytics_orchestrator")

    # Create and configure pipelines
    feed_pipeline = FeedAnalysisPipeline("feed_analysis")
    feed_pipeline.set_partition_date("2024-10-15")

    thresholds_pipeline = WeeklyThresholdsPipeline("weekly_thresholds")

    engagement_pipeline = EngagementAnalysisPipeline("engagement_analysis")

    # Register pipelines with orchestrator
    orchestrator.register_pipeline(feed_pipeline)
    orchestrator.register_pipeline(thresholds_pipeline)
    orchestrator.register_pipeline(engagement_pipeline)

    print(f"📋 Registered {len(orchestrator.pipelines)} pipelines")

    # Execute pipelines sequentially
    print("🚀 Executing pipelines sequentially...")
    results = orchestrator.execute_pipelines_sequential(
        ["feed_analysis", "weekly_thresholds", "engagement_analysis"]
    )

    # Process results
    successful_pipelines = 0
    for result in results:
        if result.success:
            successful_pipelines += 1
            print(f"✅ {result.pipeline_name}: {result.execution_time:.2f}s")
        else:
            print(f"❌ {result.pipeline_name}: {result.error}")

    # Get execution summary
    summary = orchestrator.get_execution_summary()
    print("\n📊 Execution Summary:")
    print(f"   Total pipelines: {summary['total_executions']}")
    print(f"   Successful: {summary['successful_executions']}")
    print(f"   Failed: {summary['failed_executions']}")
    print(f"   Success rate: {summary['success_rate']:.1%}")
    print(f"   Average execution time: {summary['average_execution_time']:.2f}s")

    print()


def example_pipeline_monitoring():
    """Example 4: Monitoring pipeline execution and status."""
    print("=== Example 4: Pipeline Monitoring ===")

    # Create pipeline
    pipeline = FeedAnalysisPipeline("monitored_pipeline")
    pipeline.set_partition_date("2024-10-15")

    # Check initial status
    initial_status = pipeline.get_status()
    print(f"📊 Initial status: {initial_status['state']}")

    # Start pipeline execution
    print("🚀 Starting pipeline execution...")

    # In a real scenario, you might run this in a separate thread
    # For this example, we'll just check the status
    pipeline.state = PipelineState.RUNNING
    pipeline.start_time = pd.Timestamp.now()

    running_status = pipeline.get_status()
    print(f"📊 Running status: {running_status['state']}")
    print(f"⏰ Start time: {running_status['start_time']}")

    # Simulate completion
    pipeline.state = PipelineState.COMPLETED
    pipeline.end_time = pd.Timestamp.now()
    pipeline.execution_time = 45.2  # Simulated execution time

    final_status = pipeline.get_status()
    print(f"📊 Final status: {final_status['state']}")
    print(f"⏰ End time: {final_status['end_time']}")
    print(f"⏱️  Execution time: {final_status['execution_time']:.2f}s")

    print()


def example_error_handling():
    """Example 5: Error handling and recovery."""
    print("=== Example 5: Error Handling ===")

    # Create pipeline with invalid configuration
    invalid_config = {"missing_required_config": "invalid_value"}

    pipeline = FeedAnalysisPipeline("error_example", config=invalid_config)
    pipeline.set_partition_date("2024-10-15")

    try:
        # This should fail during setup
        result = pipeline.run()

        if result.success:
            print("✅ Pipeline completed successfully")
        else:
            print(f"❌ Pipeline failed: {result.error}")

    except PipelineError as e:
        print("🚨 PipelineError caught:")
        print(f"   Pipeline: {e.pipeline_name}")
        print(f"   Stage: {e.stage}")
        print(f"   Message: {e.message}")

    except Exception as e:
        print(f"🚨 Unexpected error: {e}")

    # Check final status
    final_status = pipeline.get_status()
    print(f"📊 Final pipeline state: {final_status['state']}")
    print(f"🚨 Error: {final_status['error']}")

    print()


def example_pipeline_validation():
    """Example 6: Pipeline validation and data quality checks."""
    print("=== Example 6: Pipeline Validation ===")

    # Create pipeline
    pipeline = FeedAnalysisPipeline("validation_example")
    pipeline.set_partition_date("2024-10-15")

    # Run pipeline
    result = pipeline.run()

    if result.success:
        print("✅ Pipeline execution completed")

        # Validate results
        print("🔍 Validating pipeline results...")
        validation_passed = pipeline.validate()

        if validation_passed:
            print("✅ Validation passed")

            # Check data quality
            data = result.data
            if not data.empty:
                print("📊 Data quality metrics:")
                print(f"   Total users: {len(data)}")
                print(f"   Missing values: {data.isnull().sum().sum()}")
                print(f"   Duplicate users: {data['user'].duplicated().sum()}")

                # Check column types
                print("📋 Column types:")
                for col, dtype in data.dtypes.items():
                    print(f"   {col}: {dtype}")
        else:
            print("❌ Validation failed")
    else:
        print(f"❌ Pipeline failed: {result.error}")

    print()


def example_custom_pipeline():
    """Example 7: Creating a custom pipeline by extending base classes."""
    print("=== Example 7: Custom Pipeline Implementation ===")

    class CustomAnalyticsPipeline(BaseResearchPipeline):
        """Custom pipeline for demonstration purposes."""

        def setup(self):
            """Setup custom pipeline resources."""
            self.logger.info("Setting up custom analytics pipeline")

            # Validate custom configuration
            if "custom_parameter" not in self.config:
                raise PipelineError(
                    "Missing custom_parameter in configuration", self.name, "setup"
                )

            self.logger.info("Custom pipeline setup completed")

        def execute(self):
            """Execute custom analytics logic."""
            self.logger.info("Executing custom analytics")

            # Simulate some work
            import time

            time.sleep(0.1)  # Simulate processing time

            # Create sample results
            results = {
                "custom_metric": self.config["custom_parameter"] * 2,
                "timestamp": pd.Timestamp.now().isoformat(),
                "pipeline_name": self.name,
            }

            return PipelineResult(
                success=True, data=results, metadata={"custom_processing": True}
            )

        def cleanup(self):
            """Clean up custom resources."""
            self.logger.info("Cleaning up custom pipeline")

        def validate(self):
            """Validate custom results."""
            self.logger.info("Validating custom results")
            return True

    # Use custom pipeline
    custom_config = {"custom_parameter": 42}
    custom_pipeline = CustomAnalyticsPipeline("custom_analytics", config=custom_config)

    result = custom_pipeline.run()

    if result.success:
        print("✅ Custom pipeline completed successfully!")
        print(f"📊 Results: {result.data}")
        print(f"🔧 Metadata: {result.metadata}")
    else:
        print(f"❌ Custom pipeline failed: {result.error}")

    print()


def example_batch_processing():
    """Example 8: Batch processing with multiple partition dates."""
    print("=== Example 8: Batch Processing ===")

    # Create orchestrator
    orchestrator = PipelineOrchestrator("batch_processor")

    # Define partition dates to process
    partition_dates = ["2024-10-15", "2024-10-16", "2024-10-17"]

    # Process each partition date
    all_results = []

    for partition_date in partition_dates:
        print(f"📅 Processing partition date: {partition_date}")

        # Create pipeline for this date
        pipeline = FeedAnalysisPipeline(f"feed_analysis_{partition_date}")
        pipeline.set_partition_date(partition_date)

        # Register and execute
        orchestrator.register_pipeline(pipeline)
        result = orchestrator.execute_pipeline(pipeline.name)

        if result.success:
            print(f"✅ {partition_date}: {len(result.result.data)} users processed")
            all_results.append(result.result.data)
        else:
            print(f"❌ {partition_date}: {result.error}")

    # Combine all results
    if all_results:
        combined_results = pd.concat(all_results, ignore_index=True)
        print("\n📊 Batch processing completed:")
        print(f"   Total partition dates: {len(partition_dates)}")
        print(f"   Total users processed: {len(combined_results)}")
        print(f"   Unique users: {combined_results['user'].nunique()}")

    print()


def example_pipeline_export():
    """Example 9: Exporting pipeline results and execution history."""
    print("=== Example 9: Pipeline Export ===")

    # Create orchestrator and run some pipelines
    orchestrator = PipelineOrchestrator("export_example")

    # Register and run a few pipelines
    feed_pipeline = FeedAnalysisPipeline("feed_export")
    feed_pipeline.set_partition_date("2024-10-15")

    orchestrator.register_pipeline(feed_pipeline)
    orchestrator.execute_pipeline("feed_export")

    # Export execution history
    try:
        csv_file = orchestrator.export_execution_history(format="csv")
        print(f"📊 Exported execution history to: {csv_file}")

        json_file = orchestrator.export_execution_history(format="json")
        print(f"📊 Exported execution history to: {json_file}")

    except Exception as e:
        print(f"❌ Export failed: {e}")

    # Export pipeline results
    if feed_pipeline.final_results is not None:
        try:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            export_file = f"feed_analysis_results_{timestamp}.csv"
            feed_pipeline.final_results.to_csv(export_file, index=False)
            print(f"📊 Exported pipeline results to: {export_file}")
        except Exception as e:
            print(f"❌ Results export failed: {e}")

    print()


def main():
    """Run all pipeline examples."""
    print("🚀 Pipeline Framework Examples")
    print("=" * 50)
    print()

    # Configure logging for examples
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        # Run examples
        example_basic_pipeline_usage()
        example_pipeline_configuration()
        example_pipeline_orchestration()
        example_pipeline_monitoring()
        example_error_handling()
        example_pipeline_validation()
        example_custom_pipeline()
        example_batch_processing()
        example_pipeline_export()

        print("✅ All examples completed successfully!")

    except Exception as e:
        print(f"❌ Example execution failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
