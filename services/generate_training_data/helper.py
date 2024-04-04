"""Helper functions for setting up and managing data annotation sessions."""
import json
import os
from typing import Optional

from lib.constants import current_datetime_str

current_file_directory = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SESSION_NUM_SAMPLES = 100

timestamp = current_datetime_str

def config_is_valid(config: dict) -> bool:
    """Checks if a config file has the required fields.

    For now it's just a task name, but we can change this over time.
    """
    return "task_name" in config


def load_config_for_labeling_session(config_path: str) -> dict:
    """Load configuration for a labeling session."""
    full_fp = os.path.join(current_file_directory, config_path)
    if config_path.endswith(".json"):
        config = json.load(full_fp)
    else:
        raise ValueError("Config has to currently be a .json, other formats are not supported") # noqa
    return config


def define_config_for_labeling_session() -> dict:
    """Define the config for the labeling session, using interactive inputs."""
    num_samples = input(f"Number of samples to label (optional, default={DEFAULT_SESSION_NUM_SAMPLES}): ") # noqa
    task_name = None
    while not task_name:
        task_name = input("Task name (required): ")
    labeling_session_name = input(f"Name of the session (optional, default = {task_name}_{timestamp}): ") # noqa
    data_to_label_filename = input(f"Name to give to the dataset that will be labeled (optional, default = {labeling_session_name}.jsonl): ")
    notes = input("Notes (optional): ")
    config_name = input(f"Name of config file (optional, default = {labeling_session_name}.json): ")
    return {
        "num_samples": num_samples,
        "task_name": task_name,
        "labeling_session_name": labeling_session_name,
        "data_to_label_filename": data_to_label_filename,
        "notes": notes,
        "config_name": config_name
    }


def get_config_for_labeling_session(config_path: Optional[str]=None) -> dict:
    """Generate the configs for the current labeling session."""
    if config_path:
        try:
            config = load_config_for_labeling_session(config_path)
        except FileNotFoundError:
            print(f"Config file not found at {config_path}.")
            config = define_config_for_labeling_session()
    else:    
        config = define_config_for_labeling_session()
    if not config_is_valid(config):
        raise ValueError("Config is not valid.")
    return config


# TODO: need to figure out the data to label
# TODO: also should add support for other clauses, e.g., sort by most recent.
def load_data_to_label(num_samples: Optional[int]=None) -> list[dict]:
    """Returns a list of the data to label."""
    if num_samples:
        # put a limit on samples to label
        return []
    pass


def export_data_to_label(data_to_label: list[dict], filename: str) -> None:
    """Export data to label as a .jsonl file."""
    local_fp = os.path.join("data_to_label", filename)
    full_fp = os.path.join(current_file_directory, local_fp)
    with open(full_fp, "w") as f:
        for data in data_to_label:
            json.dump(data, f)
            f.write("\n")
    print(f"Exported data to label at {local_fp}")


def export_config(config: dict, config_name: str) -> None:
    """Exports config file."""
    local_fp = os.path.join("labeling_session_configs", config_name)
    full_fp = os.path.join(current_file_directory, local_fp)
    with open(full_fp, "w") as f:
        json.dump(config, f)
    print(f"Exported config at {local_fp}")


def set_up_labeling_session(config_path: Optional[str]=None) -> dict:
    """Set up a labeling session.

    Defines any configuration required for the training session, and adds
    appropriate logging.
    """
    if config_path:
        # assumes a local path to the file (ideally same directory)
        config_path = os.path.join(current_file_directory, config_path)
    config: dict = get_config_for_labeling_session(config_path=config_path)
    num_samples = config.get("num_samples", DEFAULT_SESSION_NUM_SAMPLES)
    task_name = config.get("task_name")
    labeling_session_name = config.get(
        "labeling_session_name", f"{task_name}_{timestamp}"
    )
    data_to_label_filename = config.get(
        "data_to_label_filename", f"{labeling_session_name}.jsonl"
    )
    notes = config.get("notes", "")
    config_name = config.get("config_name", f"{labeling_session_name}.json")

    # load and export data to be labeled
    data_to_label: list[dict] = load_data_to_label()
    export_data_to_label(
        data_to_label=data_to_label, filename=data_to_label_filename
    )

    # export config
    config_to_export = {
        "labeling_session_name": labeling_session_name,
        "timestamp": timestamp,
        "task_name": task_name,
        "num_samples": num_samples,
        "notes": notes,
        "data_to_label_filename": data_to_label_filename,
    }

    export_config(config=config_to_export, config_name=config_name)

    print(f"Labeling session set up for config {config_name} to label data at {data_to_label_filename} at {timestamp}.") # noqa
