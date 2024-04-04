"""Service for starting a manual data labeling and annotation session."""
from services.generate_training_data.helper import set_up_labeling_session


def main() -> None:
    set_up_labeling_session()


if __name__ == "__main__":
    main()
