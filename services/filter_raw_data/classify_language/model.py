"""Model for the classify_language service.

We want to check if the language of a post is English.
"""
import os

import fasttext

current_file_directory = os.path.dirname(os.path.abspath(__file__))
binary_filename = "lid.176.bin"
fp = os.path.join(current_file_directory, binary_filename)
model = fasttext.load_model(fp)


def classify(text: str) -> bool:
    """Classifies if a text is English or not."""
    return (
        model.predict(text)[0][0] == "__label__eng_Latn"
        or model.predict(text)[0][0] == "__label__en"
    )


if __name__ == "__main__":
    1+1