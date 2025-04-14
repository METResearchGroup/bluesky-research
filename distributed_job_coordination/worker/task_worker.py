"""Task worker class that manages the execution of a task.

Triggered by a Slurm script run by the dispatch component.
"""

import argparse
import importlib

from distributed_job_coordination.lib.job_state import TaskState
from lib.log.logger import get_logger

logger = get_logger(__name__)


class TaskWorker:
    """Worker class that manages the task.

    This includes loading the task state, running the handler corresponding
    to the task, and updating the task state as necessary.
    """

    def __init__(self, task_state: TaskState):
        pass

    # TODO: load from scratch path SQLite queue.
    def load_task_data(self) -> list[dict]:
        """Load in the data for the task."""
        pass

    # TODO: should probably load data here and then pass it to the handlers
    # themselves, instead of having handlers load in the data themselves.
    def run(self):
        handler = importlib.import_module(self.task_state.handler)
        try:
            handler.handler(self.task_state)
        except Exception:
            pass
        pass

    def stop(self):
        pass

    def _handle_shutdown_signal(self, signum, frame):
        pass


# NOTE: maybe dispatch can kick off the slurm script, and the slurm script
# can run kickoff_task_worker with the CLI args.
# NOTE: I can't kick this off with a bash script, since the args will change
# e.g. partition, number of nodes, etc., based on configuration, so the dispatch
# needs to generate that slurm script and then run it.
def kickoff_task_worker(job_name: str, job_id: str, task_id: str):
    # TODO: load task state from DynamoDB
    # TODO run task worker.
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Task worker for distributed job coordination"
    )
    parser.add_argument("--job_name", required=True, help="Name of the job")
    parser.add_argument("--job_id", required=True, help="ID of the job")
    parser.add_argument("--task_id", required=True, help="ID of the task to execute")
    args = parser.parse_args()

    kickoff_task_worker(
        job_name=args.job_name, job_id=args.job_id, task_id=args.task_id
    )

    logger.info(
        f"Task worker for job {args.job_id} and task {args.task_id} kicked off."
    )
