"""Script version of generating configuration and labeling data options.

Doesn't instantiate the labeling session, but sets up the configuration and
data to be labeled.

Example run command:

python main.py
python main.py example_config.json
"""
import sys

from services.generate_training_data.helper import set_up_labeling_session

def main():
    config_filename: str = sys.argv[1] if len(sys.argv) > 1 else None
    session_dict: dict = set_up_labeling_session(
        config_filename=config_filename
    )

if __name__ == "__main__":
    main()
