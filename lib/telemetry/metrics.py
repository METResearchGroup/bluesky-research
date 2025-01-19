from contextlib import contextmanager
from dataclasses import dataclass
import time
import psutil
import os
from typing import Any, Dict, Optional
from datetime import datetime

@dataclass
class MetricsSnapshot:
    timestamp: str
    memory_mb: float
    cpu_percent: float
    io_counters: Dict[str, int]

class MetricsCollector:
    def __init__(self):
        self.process = psutil.Process(os.getpid())
    
    def _get_snapshot(self) -> MetricsSnapshot:
        """Capture current system metrics"""
        return MetricsSnapshot(
            timestamp=datetime.utcnow().isoformat(),
            memory_mb=self.process.memory_info().rss / 1024 / 1024,
            cpu_percent=self.process.cpu_percent(),
            io_counters=self.process.io_counters()._asdict()
        )

    @contextmanager
    def measure(self, operation_name: str, tags: Optional[Dict[str, str]] = None):
        """Context manager to measure performance metrics."""
        tags = tags or {}
        start_snapshot = self._get_snapshot()
        start_time = time.time()

        try:
            yield
        finally:
            end_snapshot = self._get_snapshot()
            end_time = time.time()
            
            metrics = {
                "operation": operation_name,
                "tags": tags,
                "timing": {
                    "start_time": start_snapshot.timestamp,
                    "end_time": end_snapshot.timestamp,
                    "duration_seconds": end_time - start_time
                },
                "memory": {
                    "start_mb": start_snapshot.memory_mb,
                    "end_mb": end_snapshot.memory_mb,
                    "delta_mb": end_snapshot.memory_mb - start_snapshot.memory_mb
                },
                "cpu": {
                    "percent": end_snapshot.cpu_percent
                },
                "io": {
                    "start": start_snapshot.io_counters,
                    "end": end_snapshot.io_counters,
                    "delta": {
                        k: end_snapshot.io_counters[k] - start_snapshot.io_counters[k]
                        for k in start_snapshot.io_counters
                    }
                }
            }
            
            self.last_metrics = metrics
