#!/usr/bin/env python3
"""
Monitor for the scalable backfill system.

The monitor:
1. Connects to Redis and retrieves metrics
2. Displays real-time progress information
3. Shows worker status and performance
4. Visualizes throughput and estimated completion time
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import redis
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

# Redis key constants (matching coordinator.py)
TASK_QUEUE = "backfill:task_queue"
PROCESSING_QUEUE = "backfill:processing_queue"
COMPLETED_QUEUE = "backfill:completed_queue"
FAILED_QUEUE = "backfill:failed_queue"
JOB_CONFIG = "backfill:config"
JOB_STATUS = "backfill:status"
WORKER_HEARTBEATS = "backfill:worker_heartbeats"
METRICS = "backfill:metrics"
RATE_LIMITS = "backfill:rate_limits"


class BackfillMonitor:
    """Monitor for the distributed backfill process."""

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        job_id: Optional[str] = None,
        refresh_interval: float = 1.0,
    ):
        """Initialize the monitor.

        Args:
            redis_host: Hostname of the Redis server
            redis_port: Port of the Redis server
            job_id: Optional job ID to monitor (if None, will use the latest job)
            refresh_interval: Time in seconds between updates
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.job_id = job_id
        self.refresh_interval = refresh_interval
        self.console = Console()
        
        # Connect to Redis
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        
        # Get job ID if not provided
        if not self.job_id:
            self._find_latest_job()
            
        # Initialize metrics history for throughput calculation
        self.metrics_history = []
        self.worker_history = {}
        
    def _find_latest_job(self) -> None:
        """Find the latest job in Redis."""
        # Get all job configs
        job_keys = self.redis.keys(f"{JOB_CONFIG}:*")
        
        if not job_keys:
            self.console.print("[bold red]No jobs found in Redis[/bold red]")
            sys.exit(1)
        
        # Get the latest job by creation time
        latest_job = None
        latest_time = None
        
        for job_key in job_keys:
            job_config = json.loads(self.redis.get(job_key))
            start_time = job_config.get("start_time")
            
            if start_time and (not latest_time or start_time > latest_time):
                latest_time = start_time
                latest_job = job_config.get("job_id")
        
        if not latest_job:
            self.console.print("[bold red]Could not determine latest job[/bold red]")
            sys.exit(1)
            
        self.job_id = latest_job
        self.console.print(f"[bold green]Monitoring latest job: {self.job_id}[/bold green]")
        
    def _get_job_config(self) -> Dict:
        """Get job configuration from Redis.

        Returns:
            Job configuration as a dictionary
        """
        config_json = self.redis.get(f"{JOB_CONFIG}:{self.job_id}")
        if not config_json:
            return {}
        return json.loads(config_json)
        
    def _get_job_status(self) -> Dict:
        """Get job status from Redis.

        Returns:
            Job status as a dictionary
        """
        status = self.redis.hgetall(f"{JOB_STATUS}:{self.job_id}")
        return {k: int(v) if v.isdigit() else v for k, v in status.items()}
        
    def _get_metrics(self) -> Dict:
        """Get job metrics from Redis.

        Returns:
            Job metrics as a dictionary
        """
        metrics = self.redis.hgetall(f"{METRICS}:{self.job_id}")
        return {k: int(v) if v.isdigit() else v for k, v in metrics.items()}
        
    def _get_worker_status(self) -> Dict[str, Dict]:
        """Get worker status from Redis.

        Returns:
            Dictionary mapping worker IDs to status information
        """
        workers = {}
        current_time = time.time()
        
        # Get worker heartbeats
        heartbeats = self.redis.hgetall(f"{WORKER_HEARTBEATS}:{self.job_id}")
        
        # Get worker info
        for worker_id, last_heartbeat in heartbeats.items():
            if worker_id.startswith("batch:"):
                continue
                
            last_beat_time = float(last_heartbeat)
            time_since_beat = current_time - last_beat_time
            
            # Get worker info if available
            worker_info_key = f"backfill:worker:{self.job_id}:{worker_id}"
            worker_info_json = self.redis.get(worker_info_key)
            
            if worker_info_json:
                worker_info = json.loads(worker_info_json)
            else:
                worker_info = {"worker_id": worker_id}
                
            # Add status based on heartbeat
            if time_since_beat < 30:
                status = "active"
            elif time_since_beat < 120:
                status = "stalled"
            else:
                status = "dead"
                
            # Add to workers dict
            workers[worker_id] = {
                **worker_info,
                "last_heartbeat": last_beat_time,
                "time_since_heartbeat": time_since_beat,
                "status": status,
            }
            
            # Track worker history for individual throughput
            if worker_id not in self.worker_history:
                self.worker_history[worker_id] = []
                
            # Get batch assignments for this worker
            current_batch = None
            for batch_id, batch_time in heartbeats.items():
                if batch_id.startswith("batch:"):
                    # This is a batch:jobid:batch:X format key
                    # Check if this batch is recent enough to be considered current
                    if float(batch_time) > current_time - 60:
                        current_batch = batch_id.split(":")[-1]
                        break
                        
            workers[worker_id]["current_batch"] = current_batch
            
        return workers
        
    def _get_rate_limit_status(self) -> Dict:
        """Get rate limit status from Redis.

        Returns:
            Rate limit status as a dictionary
        """
        window_key = f"{RATE_LIMITS}:{self.job_id}:window"
        count_key = f"{RATE_LIMITS}:{self.job_id}:count"
        
        window_start = float(self.redis.get(window_key) or time.time())
        current_count = int(self.redis.get(count_key) or 0)
        window_elapsed = time.time() - window_start
        window_remaining = max(0, 300 - window_elapsed)
        
        return {
            "window_start": window_start,
            "current_count": current_count,
            "limit": 3000,
            "window_elapsed": window_elapsed,
            "window_remaining": window_remaining,
            "utilization": (current_count / 3000) * 100,
        }
        
    def _calculate_throughput(self, current_metrics: Dict) -> float:
        """Calculate throughput based on metrics history.

        Args:
            current_metrics: Current metrics from Redis

        Returns:
            Throughput in DIDs per minute
        """
        # Add current metrics to history
        self.metrics_history.append({
            "timestamp": time.time(),
            "processed_dids": int(current_metrics.get("processed_dids", 0)),
        })
        
        # Limit history to last 5 minutes
        now = time.time()
        self.metrics_history = [
            m for m in self.metrics_history if now - m["timestamp"] < 300
        ]
        
        # Need at least 2 data points to calculate throughput
        if len(self.metrics_history) < 2:
            return 0.0
            
        # Calculate throughput over the last 5 minutes (or available history)
        oldest = self.metrics_history[0]
        newest = self.metrics_history[-1]
        
        time_diff = newest["timestamp"] - oldest["timestamp"]
        if time_diff < 1:
            return 0.0
            
        did_diff = newest["processed_dids"] - oldest["processed_dids"]
        throughput = did_diff / (time_diff / 60)  # DIDs per minute
        
        return throughput
    
    def create_progress_layout(self) -> Layout:
        """Create the layout for the progress display.

        Returns:
            Rich Layout object
        """
        layout = Layout()
        
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3),
        )
        
        layout["main"].split_row(
            Layout(name="left", ratio=2),
            Layout(name="right", ratio=1),
        )
        
        layout["left"].split(
            Layout(name="progress", size=5),
            Layout(name="workers"),
        )
        
        layout["right"].split(
            Layout(name="stats", ratio=1),
            Layout(name="rate_limits", size=8),
        )
        
        return layout
    
    def update_layout(self, layout: Layout) -> None:
        """Update the layout with current status.

        Args:
            layout: Rich Layout object to update
        """
        # Get current status
        config = self._get_job_config()
        status = self._get_job_status()
        metrics = self._get_metrics()
        workers = self._get_worker_status()
        rate_limits = self._get_rate_limit_status()
        
        # Calculate progress
        total_batches = int(status.get("total_batches", 0))
        completed_batches = int(status.get("completed_batches", 0))
        processing_batches = int(status.get("processing_batches", 0))
        failed_batches = int(status.get("failed_batches", 0))
        pending_batches = int(status.get("pending_batches", 0))
        
        total_dids = int(metrics.get("total_dids", 0))
        processed_dids = int(metrics.get("processed_dids", 0))
        
        progress_pct = (completed_batches / total_batches) * 100 if total_batches > 0 else 0
        
        # Calculate throughput
        throughput = self._calculate_throughput(metrics)
        
        # Calculate ETA
        remaining_dids = total_dids - processed_dids
        eta_minutes = remaining_dids / throughput if throughput > 0 else 0
        eta_time = datetime.now() + timedelta(minutes=eta_minutes)
        
        # Update header
        header_text = Text()
        header_text.append(f"Backfill Job: ", style="bold")
        header_text.append(f"{self.job_id}", style="bold green")
        header_text.append(f" | Status: ", style="bold")
        header_text.append(f"{status.get('status', 'unknown')}", style="bold cyan")
        header_text.append(f" | Started: ", style="bold")
        header_text.append(f"{config.get('start_time', 'unknown')}")
        
        layout["header"].update(Panel(header_text, border_style="blue"))
        
        # Update progress
        progress_table = Table.grid(expand=True)
        progress_table.add_column("Metric", style="bold")
        progress_table.add_column("Value")
        progress_table.add_column("Progress")
        
        # Batch progress
        batch_progress = "█" * int(progress_pct / 2) + "░" * (50 - int(progress_pct / 2))
        progress_table.add_row(
            "Batches",
            f"{completed_batches}/{total_batches} ({progress_pct:.1f}%)",
            batch_progress,
        )
        
        # DID progress
        did_pct = (processed_dids / total_dids) * 100 if total_dids > 0 else 0
        did_progress = "█" * int(did_pct / 2) + "░" * (50 - int(did_pct / 2))
        progress_table.add_row(
            "DIDs",
            f"{processed_dids}/{total_dids} ({did_pct:.1f}%)",
            did_progress,
        )
        
        layout["progress"].update(Panel(progress_table, title="Progress", border_style="green"))
        
        # Update workers
        worker_table = Table(
            show_header=True,
            header_style="bold cyan",
            expand=True,
        )
        worker_table.add_column("Worker ID")
        worker_table.add_column("Status")
        worker_table.add_column("Host")
        worker_table.add_column("Current Batch")
        worker_table.add_column("Last Heartbeat")
        
        # Sort workers by ID
        sorted_workers = sorted(workers.values(), key=lambda w: w.get("worker_id", ""))
        
        for worker in sorted_workers:
            status_style = {
                "active": "green",
                "stalled": "yellow",
                "dead": "red",
            }.get(worker.get("status"), "white")
            
            worker_table.add_row(
                worker.get("worker_id", "unknown"),
                Text(worker.get("status", "unknown"), style=status_style),
                worker.get("hostname", "unknown"),
                worker.get("current_batch", "none"),
                f"{worker.get('time_since_heartbeat', 0):.1f}s ago",
            )
        
        layout["workers"].update(
            Panel(
                worker_table,
                title=f"Workers ({len(workers)} active)",
                border_style="blue",
            )
        )
        
        # Update stats
        stats_table = Table.grid(expand=True)
        stats_table.add_column("Metric", style="bold")
        stats_table.add_column("Value")
        
        stats_table.add_row("Throughput", f"{throughput:.1f} DIDs/min")
        stats_table.add_row("API Calls", str(metrics.get("api_calls", 0)))
        stats_table.add_row("Rate Limit Delays", str(metrics.get("rate_limit_delays", 0)))
        stats_table.add_row("Failed DIDs", str(metrics.get("failed_dids", 0)))
        
        # Add time stats
        start_time = float(metrics.get("start_time", time.time()))
        elapsed_seconds = time.time() - start_time
        elapsed = str(timedelta(seconds=int(elapsed_seconds)))
        
        if eta_minutes > 0:
            eta = str(timedelta(minutes=int(eta_minutes)))
            eta_str = f"{eta} (approx. {eta_time.strftime('%H:%M:%S')})"
        else:
            eta_str = "Unknown"
            
        stats_table.add_row("Elapsed Time", elapsed)
        stats_table.add_row("Estimated Time Remaining", eta_str)
        
        layout["stats"].update(Panel(stats_table, title="Statistics", border_style="magenta"))
        
        # Update rate limits
        rate_table = Table.grid(expand=True)
        rate_table.add_column("Metric", style="bold")
        rate_table.add_column("Value")
        
        rate_table.add_row("Current Count", f"{rate_limits.get('current_count', 0)}/3000")
        rate_table.add_row("Window Remaining", f"{rate_limits.get('window_remaining', 0):.1f}s")
        
        # Rate utilization bar
        util_pct = rate_limits.get("utilization", 0)
        util_bar = "█" * int(util_pct / 2) + "░" * (50 - int(util_pct / 2))
        rate_table.add_row("Utilization", f"{util_pct:.1f}%")
        rate_table.add_row("", util_bar)
        
        layout["rate_limits"].update(Panel(rate_table, title="Rate Limits", border_style="yellow"))
        
        # Update footer
        footer_text = Text()
        footer_text.append("Press Ctrl+C to exit | ")
        footer_text.append(f"Refresh interval: {self.refresh_interval}s | ")
        footer_text.append(f"Last update: {datetime.now().strftime('%H:%M:%S')}")
        
        layout["footer"].update(Panel(footer_text, border_style="blue"))
    
    def run(self) -> None:
        """Run the monitor."""
        self.console.print(f"[bold]Starting monitor for job {self.job_id}[/bold]")
        
        # Create layout
        layout = self.create_progress_layout()
        
        try:
            # Update layout continuously
            with Live(layout, refresh_per_second=1/self.refresh_interval) as live:
                while True:
                    self.update_layout(layout)
                    time.sleep(self.refresh_interval)
        except KeyboardInterrupt:
            self.console.print("[bold]Monitor stopped by user[/bold]")


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Backfill Monitor")
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
        help="Job ID to monitor (if None, will use the latest job)",
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=1.0,
        help="Refresh interval in seconds",
    )
    
    args = parser.parse_args()
    
    # Initialize and run monitor
    monitor = BackfillMonitor(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        job_id=args.job_id,
        refresh_interval=args.refresh,
    )
    
    monitor.run()


if __name__ == "__main__":
    main() 