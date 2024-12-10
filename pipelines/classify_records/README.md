# Classify records

Pipeline for performing text classifications on the records.

This is for the key ML components of the project (specifically, ML classification with the Perspective API as well as sociopolitical classification with LLMs) and could change later.

Each of these classification types should run as a batch job, taking the latest preprocessed posts and running inference.

The basic logic for all the classification modules is:

1. Load the latest preprocessed posts from the local storage.
2. Run inference on the posts. Export results as .json files in a __cache_{inference_type} folder.
3. Load the results from the __cache_{inference_type} folder and then export
to a permanent folder as .parquet files.
