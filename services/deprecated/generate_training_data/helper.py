"""Helper functions for setting up and managing data annotation sessions."""

import json
import os
from typing import Optional

from lib.constants import current_datetime_str
from services.generate_training_data.database import (
    load_ids_of_previous_annotated_samples,
)
from services.sync.stream.database import DEPRECATED_FIELDS
from services.sync.stream.helper import get_posts_as_list_dicts

current_file_directory = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = os.path.join(current_file_directory, "labeling_session_configs")
LABELING_DATA_DIR = os.path.join(current_file_directory, "data_to_label")
DEFAULT_SESSION_NUM_SAMPLES = 100
REQUIRED_FIELDS = ["task_name", "task_description", "label_options"]

default_timestamp = current_datetime_str


def config_is_valid(config: dict) -> bool:
    """Checks if a config file has the required fields."""
    for task in REQUIRED_FIELDS:
        if task not in config:
            return False
    return True


def load_config_for_labeling_session(config_filename: str) -> dict:
    """Load configuration for a labeling session."""
    full_fp = os.path.join(CONFIGS_DIR, config_filename)
    if config_filename.endswith(".json"):
        with open(full_fp, "r") as full_fp:
            config = json.load(full_fp)
    else:
        raise ValueError(
            "Config has to currently be a .json, other formats are not supported"
        )  # noqa
    return config


def define_config_for_labeling_session() -> dict:
    """Define the config for the labeling session, using interactive inputs."""
    num_samples = input(
        f"Number of samples to label (optional, default={DEFAULT_SESSION_NUM_SAMPLES}): "
    )  # noqa
    if num_samples:
        num_samples = int(num_samples)
    else:
        num_samples = DEFAULT_SESSION_NUM_SAMPLES

    task_name = None
    while not task_name:
        task_name = input("Task name (required): ")
    task_description = input(
        "Task description - what is the purpose of the labeling task? (required): "
    )  # noqa
    label_options = input("Label options, as a comma-separated string (required): ")
    label_options: list[str] = label_options.split(",")
    label_options = [option.strip() for option in label_options]

    default_labeling_session_name = f"{task_name}_{default_timestamp}"
    labeling_session_name = input(
        f"Name of the session (optional, default = {default_labeling_session_name}): "
    )  # noqa
    if not labeling_session_name:
        labeling_session_name = default_labeling_session_name
    default_data_to_label_filename = f"{labeling_session_name}.jsonl"
    data_to_label_filename = input(
        f"Name to give to the dataset that will be labeled (optional, default = {default_data_to_label_filename}): "
    )  # noqa
    if not data_to_label_filename:
        data_to_label_filename = default_data_to_label_filename

    notes = input("Notes (optional): ")

    default_config_filename = f"{labeling_session_name}.json"
    config_filename = input(
        f"Name of config file (optional, default = {default_config_filename}): "
    )  # noqa
    if not config_filename:
        config_filename = default_config_filename

    return {
        "num_samples": num_samples,
        "task_name": task_name,
        "task_description": task_description,
        "label_options": label_options,
        "labeling_session_name": labeling_session_name,
        "data_to_label_filename": data_to_label_filename,
        "notes": notes,
        "config_filename": config_filename,
    }


def get_config_for_labeling_session(config_filename: Optional[str] = None) -> dict:
    """Generate the configs for the current labeling session."""
    if config_filename:
        try:
            config: dict = load_config_for_labeling_session(config_filename)
        except FileNotFoundError:
            print(f"Config file not found at {config_filename}.")
            config = define_config_for_labeling_session()
    else:
        config = define_config_for_labeling_session()
    if not config_is_valid(config):
        raise ValueError("Config is not valid.")
    return config


def load_existing_labeled_data_ids(task_name: str) -> set[str]:
    """Load the IDs of the data that has been labeled for a given task."""
    return set(load_ids_of_previous_annotated_samples(task_name=task_name))


def load_existing_data_to_label(data_to_label_filename: str) -> list[dict]:
    """Load the data that has already been labeled."""
    full_fp = os.path.join(LABELING_DATA_DIR, data_to_label_filename)
    with open(full_fp, "r") as f:
        data_to_label = [json.loads(line) for line in f.readlines()]
    return data_to_label


def load_data_to_label(
    task_name: str,
    data_to_label_filename: Optional[str] = None,
    num_samples: Optional[int] = DEFAULT_SESSION_NUM_SAMPLES,
    most_recent_posts: Optional[bool] = False,
) -> list[dict]:
    """Returns a list of the data to label. Loads data that hasn't been labeled
    yet (optionally can return the most recent posts that haven't been labeled).

    For now, we will load raw data from the database, but we can change this
    to load only filtered data instead. By default, we'll take a subset of the
    fields.
    """  # noqa
    if data_to_label_filename:
        try:
            data_to_label: list[dict] = load_existing_data_to_label(
                data_to_label_filename
            )  # noqa
            return data_to_label
        except FileNotFoundError:
            print(
                f"Data to label not found at {data_to_label_filename}. Generating new set of data to label..."
            )  # noqa
    # TODO: update to get only filtered posts.
    if most_recent_posts:
        posts: list[dict] = get_posts_as_list_dicts(
            k=num_samples, order_by="synctimestamp", desc=True
        )
    else:
        posts: list[dict] = get_posts_as_list_dicts(k=num_samples)
    set_previously_labeled_data_ids: set[str] = load_existing_labeled_data_ids(
        task_name
    )
    res: list[dict] = [
        post for post in posts if post["id"] not in set_previously_labeled_data_ids
    ]
    return res


def preprocess_data_to_label(data: dict) -> dict:
    """Does any necessary preprocessing and field selection before dumping
    the data as to be labeled."""
    # drop deprecated fields
    for field in DEPRECATED_FIELDS:
        data.pop(field, None)
    return data


def export_data_to_label(data_to_label: list[dict], filename: str) -> None:
    """Export data to label as a .jsonl file."""
    full_fp = os.path.join(LABELING_DATA_DIR, filename)
    with open(full_fp, "w") as f:
        for data in data_to_label:
            preprocessed_data: dict = preprocess_data_to_label(data)
            json.dump(preprocessed_data, f)
            f.write("\n")
    print(f"Exported data to label at {full_fp}")


def export_config(config: dict, config_filename: str) -> None:
    """Exports config file."""
    full_fp = os.path.join(CONFIGS_DIR, config_filename)
    with open(full_fp, "w") as f:
        json.dump(config, f)
    print(f"Exported config at {full_fp}")


def set_up_labeling_session(config_filename: Optional[str] = None) -> dict:
    """Set up a labeling session.

    Defines any configuration required for the training session, and adds
    appropriate logging.

    Returns the paths to the config file as well as the data to label.
    """
    config: dict = get_config_for_labeling_session(config_filename=config_filename)
    num_samples = config.get("num_samples", DEFAULT_SESSION_NUM_SAMPLES)
    task_name = config.get("task_name")
    task_description = config.get("task_description")
    label_options = config.get("label_options")
    timestamp = config.get("timestamp", default_timestamp)
    labeling_session_name = config.get(
        "labeling_session_name", f"{task_name}_{timestamp}"
    )
    data_to_label_filename = config.get(
        "data_to_label_filename", f"{labeling_session_name}.jsonl"
    )
    notes = config.get("notes", "")
    config_filename = config.get("config_filename", f"{labeling_session_name}.json")

    # load and export data to be labeled
    data_to_label: list[dict] = load_data_to_label(
        task_name=task_name, data_to_label_filename=data_to_label_filename
    )
    export_data_to_label(data_to_label=data_to_label, filename=data_to_label_filename)

    # export config
    config_to_export = {
        "labeling_session_name": labeling_session_name,
        "timestamp": timestamp,
        "task_name": task_name,
        "task_description": task_description,
        "label_options": label_options,
        "num_samples": num_samples,
        "notes": notes,
        "data_to_label_filename": data_to_label_filename,
    }

    export_config(config=config_to_export, config_filename=config_filename)

    print(
        f"Labeling session set up for config {config_filename} to label data at {data_to_label_filename} at {timestamp}."
    )  # noqa
    return {"config": config_to_export, "data_to_label": data_to_label}


# TODO: put the function here that setups of the posts for labeling
