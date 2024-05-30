# Analyze pilot data
After running the preprocessing pipeline, I now have n~150,000 posts that passed filtering (out of n~300,000 original posts). I'll use these posts for my basic analysis.

What I want to do is the following:
- Export the pilot data from the databases (probably as a .csv file of some sort).
- Load the data from .csv and then run inference:
    - Perspective API, to track toxicity and constructiveness.
    - Some LLM (probably some variant of Llama), for sociopolitical and political ideology classifications.
- Write the inference results to a database (think I can just use SQLite again, so I can ship quickly).
- Do analysis on the results (e.g., plotting histograms of the probability distributions).

I did the following:
1. Collect and export the pilot data (in `collect_pilot_data.py`)
2. Classify the pilot data (in `classify_pilot_data.py`)
3. Analyze the pilot data and labels (in `analyze_pilot_data.py`)

