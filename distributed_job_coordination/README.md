# Distributed Job Coordination System

A system for coordinating distributed data processing jobs across multiple worker nodes.

## Overview

The Distributed Job Coordination System enables parallel processing of large data workloads by dividing them into smaller tasks that can be executed across multiple workers. The system provides:

- A configuration-based job specification
- Automatic task partitioning based on input data
- Distributed task execution with multiple worker nodes
- Fault tolerance with configurable retry policies
- Job and task status tracking
- Result consolidation

## Architecture

The system consists of the following components:

- **Job Config**: Defines the job parameters, input/output, resource requirements, etc.
- **Job State**: Tracks the state of jobs and tasks throughout their lifecycle
- **Job Partitioner**: Divides large jobs into smaller tasks for parallel processing
- **Task Worker**: Executes individual tasks and reports results
- **Job Orchestrator**: Coordinates the overall job lifecycle
- **CLI Tool**: Command-line interface for submitting and managing jobs

## Usage

### Installation

```bash
pip install -e .
```

### Submitting a Job

You can submit a job using the CLI:

```bash
python -m distributed_job_coordination.cli.job_cli submit examples/example_job.yaml
```

Or programmatically:

```python
from distributed_job_coordination.lib.job_config import JobConfig
from distributed_job_coordination.lib.job_orchestrator import create_orchestrator

# Create the orchestrator
orchestrator = create_orchestrator(use_local=True)

# Load job configuration
job_config = JobConfig.from_yaml("examples/example_job.yaml")

# Submit the job
job_id = orchestrator.submit_job(job_config)
print(f"Job submitted with ID: {job_id}")
```

### Running a Worker

To start a worker node:

```bash
python -m distributed_job_coordination.worker.task_worker --use-local
```

You can customize the worker with additional options:

```bash
python -m distributed_job_coordination.worker.task_worker \
  --api-endpoint http://localhost:8000 \
  --api-key your-api-key \
  --worker-id worker-1 \
  --poll-interval 10 \
  --max-concurrent-tasks 2 \
  --task-types text_processor binary_processor
```

### Monitoring Jobs

To check job status:

```bash
python -m distributed_job_coordination.cli.job_cli status JOB_ID
```

To list all jobs:

```bash
python -m distributed_job_coordination.cli.job_cli list
```

To view job logs:

```bash
python -m distributed_job_coordination.cli.job_cli logs JOB_ID
```

### Cancelling Jobs

To cancel a running job:

```bash
python -m distributed_job_coordination.cli.job_cli cancel JOB_ID
```

## Configuration

Job configurations are specified in YAML format. A basic configuration includes:

- **name**: A human-readable name for the job
- **input**: Input data configuration (path, format, etc.)
- **algorithm**: Processing algorithm configuration
- **compute**: Resource requirements
- **output**: Output configuration
- **notification**: Notification settings (optional)
- **advanced**: Advanced settings like batch size, timeout, etc.

See `jobs/example_job/config.yaml` for a detailed example.

## Development

### Project Structure

```
distributed_job_coordination/
├── cli/                       # Command-line interfaces
│   └── job_cli.py             # Job management CLI
├── examples/                  # Example configurations
│   └── example_job.yaml       # Example job configuration
├── lib/                       # Core library modules
│   ├── api.py                 # API client for job coordination
│   ├── job_config.py          # Job configuration classes
│   ├── job_orchestrator.py    # Job lifecycle orchestrator
│   ├── job_partitioner.py     # Job-to-task partitioning
│   └── job_state.py           # Job and task state models
└── worker/                    # Worker node implementation
    └── task_worker.py         # Task execution worker
```

### Running Tests

```bash
pytest
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
