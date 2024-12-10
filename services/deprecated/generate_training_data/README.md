# Generate training data

We need training data for our various ML models, for various purposes (such as fine-tuning a model, clarifying expected behaviors, and comparing the quality of feeds).

We'll need a way to systematically gather training data as needed. This service, powered by [Pigeon](https://github.com/agermanidis/pigeon), which allows us to quickly annotate data via Jupyter notebooks.

We'll manage this through main.ipynb, which will be where we do any annotations. The annotations will be stored in a SQLite database, `annotated_training_data.db`.

All the steps are done in `main.ipynb`. Following this notebook will generate text labels for a given set of training data and then save those annotations in the database.
