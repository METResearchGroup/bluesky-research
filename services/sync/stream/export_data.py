"""Tooling for managing the export of firehose data, both to local cache and to
S3."""
import json
import os

current_file_directory = os.path.dirname(os.path.abspath(__file__))
root_write_path = os.path.join(current_file_directory, "cache")
root_create_path = os.path.join(root_write_path, "create")
root_delete_path = os.path.join(root_write_path, "delete")
operation_types = ["post", "like", "follow"]

export_filepath_map = {
    "create": {
        "post": os.path.join(root_create_path, "post"),
        "like": os.path.join(root_create_path, "like"),
        "follow": os.path.join(root_create_path, "follow")
    },
    "delete": {
        "post": os.path.join(root_delete_path, "post"),
        "like": os.path.join(root_delete_path, "like"),
        "follow": os.path.join(root_delete_path, "follow")
    }
}


def rebuild_cache_paths():
    """Rebuild the paths for the cache, if necessary."""
    if not os.path.exists(root_write_path):
        os.makedirs(root_write_path)

    for path in [root_create_path, root_delete_path]:
        if not os.path.exists(path):
            os.makedirs(path)
        for op_type in operation_types:
            op_path = os.path.join(path, op_type)
            if not os.path.exists(op_path):
                os.makedirs(op_path)


def delete_cache_paths():
    """Deletes the cache paths."""
    if os.path.exists(root_write_path):
        os.rmdir(root_write_path)


rebuild_cache_paths()


def write_data_to_json(data: dict, path: str):
    with open(path, 'w') as f:
        json.dump(data, f)


# TODO: move these to a new file.
def write_batch_to_s3():
    """Writes the batched data to S3.

    Crawls the "created" and "deleted" folders and updates the records
    where necessary.

    Then deletes the local cache.
    """
    rebuild_cache_paths()
    # TODO: steps for writing batch to s3
