"""Pipeline orchestration utilities.

This module provides utilities for managing pipeline execution, including
state management, error handling, and progress tracking.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.shared.pipelines.base import (
    BaseResearchPipeline,
    PipelineResult,
)


@dataclass
class PipelineExecutionResult:
    """Result of pipeline orchestration execution."""

    pipeline_name: str
    success: bool
    start_time: datetime
    end_time: datetime
    execution_time: float
    result: Optional[PipelineResult] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class PipelineOrchestrator:
    """Orchestrates the execution of multiple pipelines.

    This class provides centralized management of pipeline execution,
    including dependency management, parallel execution, and result aggregation.
    """

    def __init__(self, name: str = "default"):
        """Initialize the pipeline orchestrator.

        Args:
            name: Name for this orchestrator instance
        """
        self.name = name
        self.logger = get_logger(f"orchestrator.{name}")
        self.pipelines: Dict[str, BaseResearchPipeline] = {}
        self.execution_history: List[PipelineExecutionResult] = []
        self.running_pipelines: Dict[str, BaseResearchPipeline] = {}

    def register_pipeline(self, pipeline: BaseResearchPipeline) -> None:
        """Register a pipeline with the orchestrator.

        Args:
            pipeline: Pipeline instance to register
        """
        self.pipelines[pipeline.name] = pipeline
        self.logger.info(f"Registered pipeline: {pipeline.name}")

    def unregister_pipeline(self, pipeline_name: str) -> None:
        """Unregister a pipeline from the orchestrator.

        Args:
            pipeline_name: Name of the pipeline to unregister
        """
        if pipeline_name in self.pipelines:
            del self.pipelines[pipeline_name]
            self.logger.info(f"Unregistered pipeline: {pipeline_name}")

    def execute_pipeline(
        self, pipeline_name: str, config: Optional[Dict[str, Any]] = None
    ) -> PipelineExecutionResult:
        """Execute a single pipeline.

        Args:
            pipeline_name: Name of the pipeline to execute
            config: Optional configuration override

        Returns:
            PipelineExecutionResult with execution details

        Raises:
            ValueError: If pipeline is not registered
        """
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Pipeline '{pipeline_name}' not registered")

        pipeline = self.pipelines[pipeline_name]

        # Override config if provided
        if config:
            pipeline.config.update(config)

        start_time = datetime.now()

        try:
            self.logger.info(f"Executing pipeline: {pipeline_name}")
            self.running_pipelines[pipeline_name] = pipeline

            # Execute the pipeline
            result = pipeline.run()

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            execution_result = PipelineExecutionResult(
                pipeline_name=pipeline_name,
                success=result.success,
                start_time=start_time,
                end_time=end_time,
                execution_time=execution_time,
                result=result,
                metadata=pipeline.metadata,
            )

            self.execution_history.append(execution_result)
            self.logger.info(
                f"Pipeline '{pipeline_name}' completed in {execution_time:.2f}s"
            )

            return execution_result

        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            error_msg = str(e)
            self.logger.error(f"Pipeline '{pipeline_name}' failed: {error_msg}")

            execution_result = PipelineExecutionResult(
                pipeline_name=pipeline_name,
                success=False,
                start_time=start_time,
                end_time=end_time,
                execution_time=execution_time,
                error=error_msg,
            )

            self.execution_history.append(execution_result)
            return execution_result

        finally:
            # Remove from running pipelines
            if pipeline_name in self.running_pipelines:
                del self.running_pipelines[pipeline_name]

    def execute_pipelines_sequential(
        self,
        pipeline_names: List[str],
        configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[PipelineExecutionResult]:
        """Execute multiple pipelines sequentially.

        Args:
            pipeline_names: List of pipeline names to execute in order
            configs: Optional configuration overrides per pipeline

        Returns:
            List of PipelineExecutionResult objects
        """
        results = []
        configs = configs or {}

        for pipeline_name in pipeline_names:
            config = configs.get(pipeline_name, {})
            result = self.execute_pipeline(pipeline_name, config)
            results.append(result)

            # Stop on first failure if configured
            if not result.success:
                self.logger.warning(
                    f"Pipeline '{pipeline_name}' failed, stopping sequential execution"
                )
                break

        return results

    def execute_pipelines_parallel(
        self,
        pipeline_names: List[str],
        configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[PipelineExecutionResult]:
        """Execute multiple pipelines in parallel.

        Note: This is a simplified parallel implementation. For production use,
        consider using asyncio, multiprocessing, or a proper task queue system.

        Args:
            pipeline_names: List of pipeline names to execute in parallel
            configs: Optional configuration overrides per pipeline

        Returns:
            List of PipelineExecutionResult objects
        """
        # For now, implement as sequential execution
        # TODO: Implement proper parallel execution
        self.logger.warning(
            "Parallel execution not yet implemented, falling back to sequential"
        )
        return self.execute_pipelines_sequential(pipeline_names, configs)

    def get_pipeline_status(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific pipeline.

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            Pipeline status dictionary or None if not found
        """
        if pipeline_name in self.pipelines:
            return self.pipelines[pipeline_name].get_status()
        return None

    def get_all_pipeline_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all registered pipelines.

        Returns:
            Dictionary mapping pipeline names to their status
        """
        return {
            name: pipeline.get_status() for name, pipeline in self.pipelines.items()
        }

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of all pipeline executions.

        Returns:
            Dictionary with execution statistics
        """
        if not self.execution_history:
            return {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "total_execution_time": 0.0,
                "average_execution_time": 0.0,
            }

        total_executions = len(self.execution_history)
        successful_executions = sum(1 for r in self.execution_history if r.success)
        failed_executions = total_executions - successful_executions
        total_execution_time = sum(r.execution_time for r in self.execution_history)
        average_execution_time = total_execution_time / total_executions

        return {
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "total_execution_time": total_execution_time,
            "average_execution_time": average_execution_time,
            "success_rate": successful_executions / total_executions
            if total_executions > 0
            else 0.0,
        }

    def cancel_pipeline(self, pipeline_name: str) -> bool:
        """Cancel a running pipeline.

        Args:
            pipeline_name: Name of the pipeline to cancel

        Returns:
            True if pipeline was cancelled, False otherwise
        """
        if pipeline_name in self.running_pipelines:
            pipeline = self.running_pipelines[pipeline_name]
            pipeline.cancel()
            self.logger.info(f"Cancelled pipeline: {pipeline_name}")
            return True
        return False

    def cancel_all_pipelines(self) -> None:
        """Cancel all running pipelines."""
        for pipeline_name in list(self.running_pipelines.keys()):
            self.cancel_pipeline(pipeline_name)

    def clear_execution_history(self) -> None:
        """Clear the execution history."""
        self.execution_history.clear()
        self.logger.info("Cleared execution history")

    def export_execution_history(self, format: str = "json") -> str:
        """Export execution history to a file.

        Args:
            format: Export format ('json', 'csv', 'excel')

        Returns:
            Path to exported file

        Raises:
            ValueError: If format is not supported
        """
        if not self.execution_history:
            raise ValueError("No execution history to export")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"pipeline_execution_history_{timestamp}"

        if format == "json":
            import json

            filepath = f"{filename}.json"
            with open(filepath, "w") as f:
                json.dump(
                    [r.__dict__ for r in self.execution_history],
                    f,
                    default=str,
                    indent=2,
                )

        elif format == "csv":
            filepath = f"{filename}.csv"
            df = pd.DataFrame(
                [
                    {
                        "pipeline_name": r.pipeline_name,
                        "success": r.success,
                        "start_time": r.start_time,
                        "end_time": r.end_time,
                        "execution_time": r.execution_time,
                        "error": r.error,
                    }
                    for r in self.execution_history
                ]
            )
            df.to_csv(filepath, index=False)

        elif format == "excel":
            filepath = f"{filename}.xlsx"
            df = pd.DataFrame(
                [
                    {
                        "pipeline_name": r.pipeline_name,
                        "success": r.success,
                        "start_time": r.start_time,
                        "end_time": r.end_time,
                        "execution_time": r.execution_time,
                        "error": r.error,
                    }
                    for r in self.execution_history
                ]
            )
            df.to_excel(filepath, index=False)

        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.info(f"Exported execution history to: {filepath}")
        return filepath
