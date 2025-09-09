#!/usr/bin/env python3
"""
Load testing script for topic modeling analysis.

This script runs the main.py topic modeling analysis across different sample sizes
and collects performance metrics including execution time, coherence scores, and
discovered topics.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from lib.log.logger import get_logger

logger = get_logger(__name__)


def format_time_duration(seconds: float) -> str:
    """Format time duration in human-readable format (minutes and seconds)."""
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes > 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''}, {remaining_seconds} second{'s' if remaining_seconds != 1 else ''}"
    else:
        return f"{remaining_seconds} second{'s' if remaining_seconds != 1 else ''}"


def run_topic_modeling_analysis(
    sample_size: int, mode: str = "local"
) -> Dict[str, Any]:
    """
    Run the topic modeling analysis for a given sample size.

    Args:
        sample_size: Number of documents to use for analysis
        mode: Data loading mode ('local' or 'prod')

    Returns:
        Dictionary containing metrics and results
    """
    logger.info(f"🚀 Starting analysis with sample size: {sample_size}")

    # Get the directory containing this script
    current_dir = Path(__file__).parent
    main_script = current_dir / "main.py"

    # Build command
    cmd = [
        sys.executable,
        str(main_script),
        "--sample-size",
        str(sample_size),
        "--mode",
        mode,
    ]

    logger.info(f"📋 Running command: {' '.join(cmd)}")

    # Record start time
    start_time = time.time()

    try:
        # Run the subprocess
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=current_dir,
            timeout=3600,  # 1 hour timeout
        )

        # Record end time
        end_time = time.time()
        execution_time = end_time - start_time

        if result.returncode != 0:
            logger.error(f"❌ Analysis failed with return code {result.returncode}")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return {
                "success": False,
                "error": f"Process failed with return code {result.returncode}",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "time": format_time_duration(execution_time),
            }

        logger.info(
            f"✅ Analysis completed successfully in {format_time_duration(execution_time)}"
        )

        # Parse results from the output directory
        results = parse_analysis_results(current_dir, execution_time)
        results["time"] = format_time_duration(execution_time)
        results["success"] = True

        return results

    except subprocess.TimeoutExpired:
        logger.error(
            f"⏰ Analysis timed out after 1 hour for sample size {sample_size}"
        )
        return {
            "success": False,
            "error": "Process timed out after 1 hour",
            "time": "1 hour+",
        }
    except Exception as e:
        logger.error(f"❌ Unexpected error during analysis: {e}")
        return {
            "success": False,
            "error": str(e),
            "time": format_time_duration(time.time() - start_time),
        }


def parse_analysis_results(output_dir: Path, execution_time: float) -> Dict[str, Any]:
    """
    Parse analysis results from the output directory.

    Args:
        output_dir: Directory containing the analysis results
        execution_time: Execution time in seconds

    Returns:
        Dictionary containing parsed metrics and topics
    """
    # Find the most recent results directory
    results_dirs = list(output_dir.glob("results/*"))
    if not results_dirs:
        logger.warning("⚠️ No results directory found")
        return {"metrics": {}, "topics": {}}

    # Get the most recent directory
    latest_dir = max(results_dirs, key=os.path.getctime)
    logger.info(f"📁 Parsing results from: {latest_dir}")

    results = {"metrics": {}, "topics": {}}

    # Parse summary file for basic metrics
    summary_files = list(latest_dir.glob("summary_*.json"))
    if summary_files:
        try:
            with open(summary_files[0], "r") as f:
                summary = json.load(f)
                results["metrics"]["c_v"] = summary.get("c_v_coherence", 0)
                results["metrics"]["c_npmi"] = summary.get("c_npmi_coherence", 0)
                results["metrics"]["total_topics"] = summary.get("total_topics", 0)
                results["metrics"]["total_documents"] = summary.get(
                    "total_documents", 0
                )
        except Exception as e:
            logger.warning(f"⚠️ Failed to parse summary file: {e}")

    # Parse quality metrics file
    quality_files = list(latest_dir.glob("quality_metrics_*.json"))
    if quality_files:
        try:
            with open(quality_files[0], "r") as f:
                quality_metrics = json.load(f)
                # Update metrics with any additional quality metrics
                results["metrics"].update(quality_metrics)
        except Exception as e:
            logger.warning(f"⚠️ Failed to parse quality metrics file: {e}")

    # Parse topics file
    topic_files = list(latest_dir.glob("topics_*.csv"))
    if topic_files:
        try:
            import pandas as pd

            topics_df = pd.read_csv(topic_files[0])

            # Extract topic information
            topics = {}
            for _, row in topics_df.iterrows():
                topic_id = row.get("Topic", row.get("topic_id", "unknown"))
                if topic_id != -1:  # Skip outlier topic
                    # Get top words for this topic
                    words = []
                    for i in range(1, 11):  # Check for Word_1, Word_2, etc.
                        word_col = f"Word_{i}"
                        if word_col in row and pd.notna(row[word_col]):
                            words.append(str(row[word_col]))
                        else:
                            break

                    if words:
                        topics[f"topic_{topic_id}"] = words

            results["topics"] = topics

        except Exception as e:
            logger.warning(f"⚠️ Failed to parse topics file: {e}")

    return results


def run_load_test(
    sample_sizes: List[int], mode: str = "local", output_file: str = None
) -> Dict[str, Any]:
    """
    Run load testing across multiple sample sizes.

    Args:
        sample_sizes: List of sample sizes to test
        mode: Data loading mode ('local' or 'prod')
        output_file: Optional output file path for results

    Returns:
        Dictionary containing all test results
    """
    logger.info(f"🧪 Starting load test with sample sizes: {sample_sizes}")
    logger.info(f"📊 Data mode: {mode}")

    # Initialize results structure
    results = {"date": datetime.now().isoformat(), "mode": mode, "runs": []}

    for i, sample_size in enumerate(sample_sizes, 1):
        logger.info(
            f"📊 Run {i}/{len(sample_sizes)}: Testing sample size {sample_size}"
        )

        try:
            run_result = run_topic_modeling_analysis(sample_size, mode)

            # Structure the result according to the requested format
            run_data = {
                "n_samples": sample_size,
                "metrics": run_result.get("metrics", {}),
                "time": run_result.get("time", "unknown"),
                "success": run_result.get("success", False),
            }

            # Add topics if available
            if "topics" in run_result:
                run_data["topics"] = run_result["topics"]

            # Add error information if the run failed
            if not run_result.get("success", False):
                run_data["error"] = run_result.get("error", "Unknown error")

            results["runs"].append(run_data)

            logger.info(
                f"✅ Completed run {i}/{len(sample_sizes)} for sample size {sample_size}"
            )

        except Exception as e:
            logger.error(
                f"❌ Failed run {i}/{len(sample_sizes)} for sample size {sample_size}: {e}"
            )
            results["runs"].append(
                {
                    "n_samples": sample_size,
                    "metrics": {},
                    "time": "unknown",
                    "success": False,
                    "error": str(e),
                }
            )

    # Save results to file
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"load_test_results_{timestamp}.json"

    try:
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"💾 Results saved to: {output_file}")
    except Exception as e:
        logger.error(f"❌ Failed to save results: {e}")

    return results


def main():
    """Main function for load testing."""
    parser = argparse.ArgumentParser(
        description="Load testing for topic modeling analysis"
    )
    parser.add_argument(
        "--sample-sizes",
        nargs="+",
        type=int,
        default=[1000, 10000, 100000, 1000000, 5000000],
        help="List of sample sizes to test (default: 1000 10000 100000 1000000 5000000)",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "prod"],
        default="local",
        help="Data loading mode (default: local)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path for results (default: auto-generated with timestamp)",
    )

    args = parser.parse_args()

    logger.info("🚀 Starting Topic Modeling Load Test")
    logger.info(f"📊 Sample sizes: {args.sample_sizes}")
    logger.info(f"🔧 Mode: {args.mode}")

    try:
        results = run_load_test(
            sample_sizes=args.sample_sizes, mode=args.mode, output_file=args.output
        )

        # Print summary
        print("\n" + "=" * 80)
        print("📊 LOAD TEST SUMMARY")
        print("=" * 80)

        successful_runs = [run for run in results["runs"] if run.get("success", False)]
        failed_runs = [run for run in results["runs"] if not run.get("success", False)]

        print(f"✅ Successful runs: {len(successful_runs)}/{len(results['runs'])}")
        print(f"❌ Failed runs: {len(failed_runs)}/{len(results['runs'])}")

        if successful_runs:
            print("\n📈 Performance Summary:")
            for run in successful_runs:
                metrics = run.get("metrics", {})
                c_v = metrics.get("c_v", 0)
                c_npmi = metrics.get("c_npmi", 0)
                topics_count = len(run.get("topics", {}))
                print(
                    f"  {run['n_samples']:,} docs: {run['time']} | c_v: {c_v:.3f} | c_npmi: {c_npmi:.3f} | topics: {topics_count}"
                )

        if failed_runs:
            print("\n❌ Failed Runs:")
            for run in failed_runs:
                print(
                    f"  {run['n_samples']:,} docs: {run.get('error', 'Unknown error')}"
                )

        print("=" * 80)
        logger.info("🎉 Load test completed successfully!")

    except Exception as e:
        logger.error(f"❌ Load test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
