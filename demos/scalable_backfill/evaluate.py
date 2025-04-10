#!/usr/bin/env python3
"""
Evaluation script for the scalable backfill system.

This script:
1. Retrieves metrics from Redis for a completed job
2. Analyzes performance metrics
3. Generates performance reports and visualizations
4. Compares results to baseline performance
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import redis
import numpy as np
import matplotlib.pyplot as plt
from tabulate import tabulate

# Redis key constants
TASK_QUEUE = "backfill:task_queue"
PROCESSING_QUEUE = "backfill:processing_queue"
COMPLETED_QUEUE = "backfill:completed_queue"
FAILED_QUEUE = "backfill:failed_queue"
JOB_CONFIG = "backfill:config"
JOB_STATUS = "backfill:status"
WORKER_HEARTBEATS = "backfill:worker_heartbeats"
METRICS = "backfill:metrics"
RATE_LIMITS = "backfill:rate_limits"


class BackfillEvaluator:
    """Evaluator for the distributed backfill process."""

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        job_id: Optional[str] = None,
        output_dir: str = "results",
    ):
        """Initialize the evaluator.

        Args:
            redis_host: Hostname of the Redis server
            redis_port: Port of the Redis server
            job_id: Optional job ID to evaluate (if None, will use the latest completed job)
            output_dir: Directory to save evaluation results
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.job_id = job_id
        self.output_dir = output_dir
        
        # Connect to Redis
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        
        # Get job ID if not provided
        if not self.job_id:
            self._find_latest_job()
            
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
            
    def _find_latest_job(self) -> None:
        """Find the latest completed job in Redis."""
        # Get all job configs
        job_keys = self.redis.keys(f"{JOB_CONFIG}:*")
        
        if not job_keys:
            print("No jobs found in Redis")
            sys.exit(1)
        
        # Get the latest completed job
        latest_job = None
        latest_time = None
        
        for job_key in job_keys:
            job_config = json.loads(self.redis.get(job_key))
            end_time = job_config.get("end_time")
            
            if end_time and (not latest_time or end_time > latest_time):
                latest_time = end_time
                latest_job = job_config.get("job_id")
        
        if not latest_job:
            print("Could not find any completed jobs")
            sys.exit(1)
            
        self.job_id = latest_job
        print(f"Evaluating latest completed job: {self.job_id}")
        
    def _get_job_config(self) -> Dict:
        """Get job configuration.

        Returns:
            Job configuration dictionary
        """
        config_json = self.redis.get(f"{JOB_CONFIG}:{self.job_id}")
        if not config_json:
            print(f"No configuration found for job {self.job_id}")
            sys.exit(1)
        return json.loads(config_json)
        
    def _get_job_status(self) -> Dict:
        """Get job status.

        Returns:
            Job status dictionary
        """
        status = self.redis.hgetall(f"{JOB_STATUS}:{self.job_id}")
        return {k: int(v) if v.isdigit() else v for k, v in status.items()}
        
    def _get_metrics(self) -> Dict:
        """Get job metrics.

        Returns:
            Job metrics dictionary
        """
        metrics = self.redis.hgetall(f"{METRICS}:{self.job_id}")
        return {k: int(v) if v.isdigit() else v for k, v in metrics.items()}
        
    def _get_worker_info(self) -> List[Dict]:
        """Get information about workers.

        Returns:
            List of worker info dictionaries
        """
        workers = []
        
        # Get all worker keys
        worker_keys = self.redis.keys(f"backfill:worker:{self.job_id}:*")
        
        for key in worker_keys:
            worker_info = json.loads(self.redis.get(key))
            workers.append(worker_info)
            
        return workers
        
    def _get_completed_batches(self) -> List[Dict]:
        """Get completed batches.

        Returns:
            List of completed batch dictionaries
        """
        batches = []
        
        # Get all completed batches
        batch_jsons = self.redis.lrange(f"{COMPLETED_QUEUE}:{self.job_id}", 0, -1)
        
        for batch_json in batch_jsons:
            batch = json.loads(batch_json)
            batches.append(batch)
            
        return batches
        
    def calculate_performance_metrics(self) -> Dict:
        """Calculate performance metrics.

        Returns:
            Dictionary of performance metrics
        """
        config = self._get_job_config()
        status = self._get_job_status()
        metrics = self._get_metrics()
        workers = self._get_worker_info()
        
        # Extract basic metrics
        total_dids = int(metrics.get("total_dids", 0))
        processed_dids = int(metrics.get("processed_dids", 0))
        completed_dids = int(metrics.get("completed_dids", 0))
        failed_dids = int(metrics.get("failed_dids", 0))
        api_calls = int(metrics.get("api_calls", 0))
        rate_limit_delays = int(metrics.get("rate_limit_delays", 0))
        
        # Calculate time metrics
        start_time = datetime.fromisoformat(config.get("start_time", datetime.now().isoformat()))
        end_time = datetime.fromisoformat(config.get("end_time", datetime.now().isoformat()))
        duration = (end_time - start_time).total_seconds()
        
        # Calculate throughput
        dids_per_second = processed_dids / duration if duration > 0 else 0
        dids_per_minute = dids_per_second * 60
        dids_per_hour = dids_per_minute * 60
        
        # Calculate worker efficiency
        worker_count = len(workers)
        dids_per_worker = processed_dids / worker_count if worker_count > 0 else 0
        
        # Calculate success rate
        success_rate = (completed_dids / processed_dids) * 100 if processed_dids > 0 else 0
        
        # Calculate rate limit efficiency
        rate_limit_ratio = rate_limit_delays / api_calls if api_calls > 0 else 0
        
        # Calculate extrapolation to 400,000 DIDs
        extrapolated_time_hours = (400000 / dids_per_hour) if dids_per_hour > 0 else 0
        extrapolated_time_days = extrapolated_time_hours / 24
        
        # Calculate optimal worker count 
        # (assuming linear scaling up to a point, then diminishing returns)
        est_optimal_workers = min(
            int(worker_count * 1.5),  # 50% more than current
            int((api_calls / duration) / (3000 / 300)),  # Based on rate limit
            64  # Cap at reasonable maximum
        )
        
        return {
            "job_id": self.job_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "duration_formatted": str(timedelta(seconds=duration)),
            "total_dids": total_dids,
            "processed_dids": processed_dids,
            "completed_dids": completed_dids,
            "failed_dids": failed_dids,
            "success_rate": success_rate,
            "dids_per_second": dids_per_second,
            "dids_per_minute": dids_per_minute,
            "dids_per_hour": dids_per_hour,
            "worker_count": worker_count,
            "dids_per_worker": dids_per_worker,
            "api_calls": api_calls,
            "rate_limit_delays": rate_limit_delays,
            "rate_limit_ratio": rate_limit_ratio,
            "extrapolated_time_hours": extrapolated_time_hours,
            "extrapolated_time_days": extrapolated_time_days,
            "estimated_optimal_workers": est_optimal_workers,
        }
        
    def generate_text_report(self, metrics: Dict) -> str:
        """Generate a text report.

        Args:
            metrics: Performance metrics dictionary

        Returns:
            Text report as a string
        """
        # Format the report
        report = []
        report.append(f"# Performance Evaluation for Job: {metrics['job_id']}")
        report.append("")
        report.append(f"Run date: {metrics['start_time']} to {metrics['end_time']}")
        report.append(f"Duration: {metrics['duration_formatted']}")
        report.append("")
        
        report.append("## Summary")
        report.append("")
        report.append(f"- Processed {metrics['processed_dids']:,} DIDs out of {metrics['total_dids']:,} total")
        report.append(f"- Success rate: {metrics['success_rate']:.2f}%")
        report.append(f"- Throughput: {metrics['dids_per_minute']:.2f} DIDs/minute ({metrics['dids_per_hour']:.2f} DIDs/hour)")
        report.append(f"- Workers: {metrics['worker_count']} ({metrics['dids_per_worker']:.2f} DIDs/worker)")
        report.append("")
        
        report.append("## Extrapolation to 400,000 DIDs")
        report.append("")
        report.append(f"- Estimated time with current configuration: {metrics['extrapolated_time_hours']:.2f} hours ({metrics['extrapolated_time_days']:.2f} days)")
        report.append(f"- Estimated optimal worker count: {metrics['estimated_optimal_workers']}")
        
        if metrics['estimated_optimal_workers'] > metrics['worker_count']:
            optimized_time = metrics['extrapolated_time_hours'] * (metrics['worker_count'] / metrics['estimated_optimal_workers'])
            report.append(f"- Estimated time with optimal workers: {optimized_time:.2f} hours ({optimized_time/24:.2f} days)")
        report.append("")
        
        report.append("## API Usage")
        report.append("")
        report.append(f"- Total API calls: {metrics['api_calls']:,}")
        report.append(f"- Rate limit delays: {metrics['rate_limit_delays']:,} ({metrics['rate_limit_ratio']*100:.2f}% of calls)")
        report.append("")
        
        report.append("## Recommendations")
        report.append("")
        
        # Add recommendations based on metrics
        if metrics['rate_limit_ratio'] > 0.1:
            report.append("- **Rate limiting is a bottleneck**: Implement more sophisticated rate limit management")
        
        if metrics['estimated_optimal_workers'] > metrics['worker_count']:
            report.append(f"- **Increase worker count**: Scale up to {metrics['estimated_optimal_workers']} workers")
        
        if metrics['success_rate'] < 95:
            report.append("- **Improve error handling**: Investigate reasons for failed DIDs")
            
        report.append("- **Use multiple accounts**: Distribute API calls across multiple accounts to increase throughput")
        report.append("- **Batch by PDS endpoint**: Group DIDs by PDS endpoint to optimize rate limit usage")
        report.append("")
        
        return "\n".join(report)
        
    def generate_visualizations(self, metrics: Dict) -> None:
        """Generate visualizations.

        Args:
            metrics: Performance metrics dictionary
        """
        # Create figures directory
        figures_dir = os.path.join(self.output_dir, "figures")
        os.makedirs(figures_dir, exist_ok=True)
        
        # 1. Throughput comparison
        plt.figure(figsize=(10, 6))
        throughputs = [
            metrics["dids_per_hour"],  # Current
            metrics["dids_per_hour"] * (metrics["estimated_optimal_workers"] / metrics["worker_count"]) 
                if metrics["worker_count"] > 0 else 0,  # Optimized
            400000 / 100  # Target (100 hour baseline)
        ]
        labels = ["Current", "Optimized", "Target (100h)"]
        colors = ["blue", "green", "red"]
        
        plt.bar(labels, throughputs, color=colors)
        plt.title("Throughput Comparison (DIDs/hour)")
        plt.ylabel("DIDs per hour")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        
        for i, v in enumerate(throughputs):
            plt.text(i, v + 50, f"{v:.1f}", ha="center")
            
        plt.savefig(os.path.join(figures_dir, "throughput_comparison.png"))
        plt.close()
        
        # 2. Time to complete 400,000 DIDs
        plt.figure(figsize=(10, 6))
        times = [
            metrics["extrapolated_time_hours"],  # Current
            metrics["extrapolated_time_hours"] * (metrics["worker_count"] / metrics["estimated_optimal_workers"])
                if metrics["estimated_optimal_workers"] > 0 else 0,  # Optimized
            100  # Target
        ]
        labels = ["Current", "Optimized", "Target"]
        colors = ["blue", "green", "red"]
        
        plt.bar(labels, times, color=colors)
        plt.title("Time to Process 400,000 DIDs (hours)")
        plt.ylabel("Hours")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        
        for i, v in enumerate(times):
            plt.text(i, v + 5, f"{v:.1f}h", ha="center")
            
        plt.savefig(os.path.join(figures_dir, "completion_time.png"))
        plt.close()
        
        # 3. Worker scaling prediction
        worker_counts = list(range(1, metrics["estimated_optimal_workers"] * 2 + 1, 
                                   max(1, metrics["estimated_optimal_workers"] // 10)))
        
        # Predicted throughput with different worker counts (with diminishing returns)
        def predict_throughput(workers):
            # Linear scaling up to optimal count, then sqrt scaling after that
            if workers <= metrics["estimated_optimal_workers"]:
                return metrics["dids_per_hour"] * (workers / metrics["worker_count"]) if metrics["worker_count"] > 0 else 0
            else:
                optimal = metrics["dids_per_hour"] * (metrics["estimated_optimal_workers"] / metrics["worker_count"]) if metrics["worker_count"] > 0 else 0
                beyond_optimal = workers - metrics["estimated_optimal_workers"]
                return optimal * (1 + np.sqrt(beyond_optimal) / np.sqrt(metrics["estimated_optimal_workers"]))
        
        throughputs = [predict_throughput(w) for w in worker_counts]
        
        plt.figure(figsize=(10, 6))
        plt.plot(worker_counts, throughputs, marker='o')
        plt.axvline(x=metrics["estimated_optimal_workers"], color='r', linestyle='--', 
                    label=f'Optimal Workers: {metrics["estimated_optimal_workers"]}')
        plt.axvline(x=metrics["worker_count"], color='g', linestyle='--', 
                    label=f'Current Workers: {metrics["worker_count"]}')
        
        plt.title("Predicted Throughput vs Worker Count")
        plt.xlabel("Number of Workers")
        plt.ylabel("DIDs per Hour")
        plt.grid(True, linestyle="--", alpha=0.7)
        plt.legend()
        
        plt.savefig(os.path.join(figures_dir, "worker_scaling.png"))
        plt.close()
        
    def run(self) -> None:
        """Run the evaluation."""
        # Calculate performance metrics
        metrics = self.calculate_performance_metrics()
        
        # Generate text report
        report = self.generate_text_report(metrics)
        
        # Save report to file
        report_path = os.path.join(self.output_dir, f"evaluation_{self.job_id}.md")
        with open(report_path, "w") as f:
            f.write(report)
            
        print(f"Saved evaluation report to {report_path}")
        
        # Generate visualizations
        self.generate_visualizations(metrics)
        
        print("Generated visualizations in", os.path.join(self.output_dir, "figures"))
        
        # Print summary to console
        print("\nPerformance Summary:")
        print("-" * 50)
        
        table = [
            ["Processed DIDs", f"{metrics['processed_dids']:,} / {metrics['total_dids']:,}"],
            ["Success Rate", f"{metrics['success_rate']:.2f}%"],
            ["Duration", metrics['duration_formatted']],
            ["Throughput", f"{metrics['dids_per_minute']:.2f} DIDs/minute"],
            ["Workers", str(metrics['worker_count'])],
            ["Projected Time (400k)", f"{metrics['extrapolated_time_hours']:.2f} hours ({metrics['extrapolated_time_days']:.2f} days)"],
            ["Optimal Workers", str(metrics['estimated_optimal_workers'])],
        ]
        
        print(tabulate(table, tablefmt="simple"))
        
        print("\nRecommendations:")
        if metrics['estimated_optimal_workers'] > metrics['worker_count']:
            print(f"- Increase worker count to {metrics['estimated_optimal_workers']}")
            
        if metrics['rate_limit_ratio'] > 0.1:
            print("- Improve rate limit management")
            
        print("- Use multiple accounts to increase rate limit capacity")
        print("- See full report for detailed recommendations")


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Evaluate Backfill Performance")
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis server hostname",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis server port",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Job ID to evaluate (if None, will use the latest completed job)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results",
        help="Directory to save evaluation results",
    )
    
    args = parser.parse_args()
    
    # Initialize and run evaluator
    evaluator = BackfillEvaluator(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        job_id=args.job_id,
        output_dir=args.output_dir,
    )
    
    evaluator.run()


if __name__ == "__main__":
    main() 