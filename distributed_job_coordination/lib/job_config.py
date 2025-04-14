"""
Job configuration parsing and validation for distributed job coordination.

This module provides utilities for loading, validating, and accessing job configuration
files in YAML format.
"""

from enum import Enum
from pathlib import Path
from typing import Optional, Union
import yaml
from pydantic import BaseModel, Field, validator


class Priority(str, Enum):
    """Job priority levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class InputConfig(BaseModel):
    """Input data configuration for a job."""

    type: str = Field(..., description="Storage type (e.g., 's3', 'local')")
    path: str = Field(..., description="Path to input data")
    format: str = Field(..., description="Data format (e.g., 'parquet', 'csv')")
    max_partitions: int = Field(
        default=1, description="Maximum number of partitions for parallel processing"
    )
    batch_size: int = Field(default=1000, description="Number of records per batch")


class ComputeConfig(BaseModel):
    """Compute resource configuration for Slurm jobs."""

    partition: str = Field(
        default="short", description="Slurm partition (e.g., 'short', 'gengpu')"
    )
    nodes: int = Field(default=1, description="Number of nodes")
    ntasks_per_node: int = Field(default=1, description="Number of tasks per node")
    memory_gb: int = Field(default=8, description="Memory requirement in GB")
    max_runtime: str = Field(
        default="1:00:00",
        description="Maximum runtime in Slurm format (e.g., '1:00:00')",
    )
    gpu_type: Optional[str] = Field(
        default=None, description="GPU type if needed (e.g., 'a100')"
    )
    gpu_count: Optional[int] = Field(
        default=None, description="Number of GPUs if needed"
    )
    account: str = Field(default="p32375", description="Slurm account to charge")
    job_name: str = Field(
        default="{job_id}",
        description="Job name format, can include {job_id} placeholder",
    )
    output_log_path: str = Field(
        default="/projects/p32375/bluesky-research/lib/log/{job_name}/{job_name}-%j.log",
        description="Path for output logs, can include {job_name} and %j placeholders",
    )
    mail_type: str = Field(
        default="FAIL",
        description="When to send email notifications (e.g., 'FAIL', 'ALL', 'BEGIN,END')",
    )
    mail_user: Optional[str] = Field(
        default=None, description="Email address for job notifications"
    )

    @validator("partition")
    def validate_partition_with_gpu(cls, v, values):
        gpu_count = values.get("gpu_count")
        gpu_type = values.get("gpu_type")

        # If GPU is requested, ensure partition is appropriate
        if (gpu_count or gpu_type) and v != "gengpu":
            raise ValueError("When requesting GPUs, partition must be 'gengpu'")

        # If gengpu partition is selected, ensure GPU is requested
        if v == "gengpu" and not (gpu_count or gpu_type):
            raise ValueError(
                "When using 'gengpu' partition, gpu_count and gpu_type must be specified"
            )

        return v


class OutputConfig(BaseModel):
    """Output configuration."""

    format: str = Field(default="parquet", description="Output format")
    compression: str = Field(default="snappy", description="Compression algorithm")
    write_mode: str = Field(default="overwrite", description="Write mode")

    @validator("write_mode")
    def validate_write_mode(cls, v):
        valid_modes = ["overwrite", "append", "error_if_exists"]
        if v not in valid_modes:
            raise ValueError(f"Write mode must be one of {valid_modes}")
        return v


class AdvancedConfig(BaseModel):
    """Advanced job configuration options."""

    checkpoint_interval_minutes: int = Field(
        default=30, description="Checkpointing interval in minutes"
    )
    retry_failed_tasks: bool = Field(
        default=True, description="Whether to retry failed tasks"
    )
    max_retries: int = Field(default=3, description="Maximum number of retry attempts")
    backoff_strategy: str = Field(
        default="exponential", description="Retry backoff strategy"
    )
    task_timeout_minutes: int = Field(default=30, description="Task timeout in minutes")

    @validator("backoff_strategy")
    def validate_backoff_strategy(cls, v):
        valid_strategies = ["constant", "exponential", "linear"]
        if v not in valid_strategies:
            raise ValueError(f"Backoff strategy must be one of {valid_strategies}")
        return v


class JobConfig(BaseModel):
    """
    Job configuration model for distributed graph analytics jobs.

    This class provides structured access to job configuration parameters
    defined in YAML files, with validation and default values.
    """

    name: str = Field(..., description="Job name")
    description: str = Field(default="", description="Job description")
    priority: Priority = Field(default=Priority.MEDIUM, description="Job priority")

    input: InputConfig = Field(..., description="Input data configuration")
    compute: ComputeConfig = Field(
        default_factory=ComputeConfig, description="Compute resource configuration"
    )
    output: OutputConfig = Field(
        default_factory=OutputConfig, description="Output configuration"
    )
    advanced: AdvancedConfig = Field(
        default_factory=AdvancedConfig, description="Advanced options"
    )
    contact_email: str = Field(..., description="Contact email")

    @classmethod
    def from_yaml(cls, file_path: Union[str, Path]) -> "JobConfig":
        """
        Load job configuration from a YAML file.

        Args:
            file_path: Path to the YAML configuration file

        Returns:
            JobConfig: Parsed and validated job configuration

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            ValueError: If the configuration is invalid
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        with open(file_path, "r") as f:
            config_dict = yaml.safe_load(f)

        try:
            return cls(**config_dict)
        except Exception as e:
            raise ValueError(f"Invalid configuration in {file_path}: {str(e)}")

    def to_yaml(self, file_path: Union[str, Path]) -> None:
        """
        Save job configuration to a YAML file.

        Args:
            file_path: Path where to save the configuration
        """
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w") as f:
            yaml.dump(self.dict(), f, default_flow_style=False)

    def get_unique_job_id(self) -> str:
        """
        Generate a unique job ID for this configuration.

        Returns:
            str: Unique job ID
        """
        import hashlib
        import time

        # Create a unique ID based on job name and timestamp
        timestamp = int(time.time())
        hash_input = f"{self.name}-{timestamp}"
        job_hash = hashlib.md5(hash_input.encode()).hexdigest()[:8]

        return f"{self.name.replace(' ', '-').lower()}-{job_hash}"
