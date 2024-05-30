"""Classifies data from pilot and stores result in DB.

These classifications will be used in production more generally, so I want to
store them in a database for future reference (so, not just a .csv file for
the pilot, though we also do want a .csv as well).
"""
import pandas as pd


def load_pilot_data() -> pd.DataFrame:
    pass


def classify_data():
    pass


def export_labels():
    # export to DB
    # export to .csv
    pass


def main():
    pass

if __name__ == "__main__":
    main()