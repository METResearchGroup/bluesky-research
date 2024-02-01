import json

def dump_data(data) -> None:
    """Dump all data to a file."""
    with open("data.json", "w") as f:
        json.dump(data, f)


def load_data() -> list[dict]:
    """Load data from a file."""
    with open("data.json", "r") as f:
        data = json.load(f)
    return data
