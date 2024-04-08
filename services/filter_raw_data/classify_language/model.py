"""Model for the classify_language service.

We want to check if the language of a post is English.

We use our own model (`fasttext`) for this, though it looks like Bluesky (as of 2024-04-08)
is trying to classify the language of every post on their end
- https://github.com/bluesky-social/atproto/pull/2301
- https://github.com/bluesky-social/atproto/pull/2161/

Our model only runs if the language isn't specificed by the record, so it
could be the case that we don't have to run our model altogether, depending on
how Bluesky proceeds.
""" # noqa
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