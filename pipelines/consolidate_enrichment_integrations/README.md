# Consolidate Enrichment Integrations

This pipeline consolidates data from various enrichment services into a single
consolidated representation. It is intended to run as a batch job, taking the
latest data from the enrichment services and consolidating it into a single
file.

The pipeline is run via `orchestration/data_pipeline.py`, after the upstream
integrations (e.g., ML classification) have been run.
