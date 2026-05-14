# Repository setup

This runbook covers local development setup, optional dependency groups, and environment variables for the Bluesky research codebase.

## Scope

This repository can be inspected and tested locally, but full production operation requires private credentials and institutional infrastructure (AWS, SLURM/Quest, Prefect, and study-specific data paths).

## Prerequisites

- Python 3.10 or newer.
- `uv` for Python environment and dependency management.
- Git.
- Optional: conda, if using the legacy setup path in `scripts/setup_environment.sh`.
- For production-like workflows: AWS credentials, S3/Athena access, Prefect configuration, SLURM/Quest access, and service-specific API keys.

## Local development

Install the lightweight development environment:

```bash
uv sync --extra dev
```

Install additional dependency groups for specific workflows:

```bash
# ML classifiers and embeddings
uv sync --extra dev --extra ml

# LLM-backed or API-backed labeling workflows
uv sync --extra dev --extra llm

# Feed API development
uv sync --extra dev --extra feed_api

# Analysis workflows, including topic modeling support
uv sync --extra dev --extra analysis

# Everything defined by the project
uv sync --all-extras
```

The repository also includes an environment setup helper:

```bash
./scripts/setup_environment.sh
```

Use that script if you want an opinionated setup path with Python version selection, optional conda support, pre-commit installation, and validation checks.

## Environment variables

1. **Copy the template** from the repository root:

   ```bash
   cp .env.example .env
   ```

2. **Edit `.env`** with real values. Never commit `.env`; it is for local and deployment secrets only.

3. **Variable reference**: see [`.env.example`](../../.env.example) in the repo root for all supported keys and one-line descriptions.

4. **AWS**: for SSO or programmatic access patterns, see [`lib/aws/README.md`](../../lib/aws/README.md).

`lib/load_env_vars.py` loads `.env` from the repository root when resolving configuration for non-test `RUN_MODE` values.

## Related documentation

- [`README.md`](../../README.md) or [`PROPOSED_README.md`](../../PROPOSED_README.md) for project overview.
- [`lib/aws/README.md`](../../lib/aws/README.md) for AWS access.
- [`feed_api/README.md`](../../feed_api/README.md) for feed API deployment variables (`HOSTNAME`, `SERVICE_DID`, etc.).
