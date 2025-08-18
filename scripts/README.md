# Scripts

This directory contains scripts that are used to run various tasks. They're generally one-off scripts or are bash scripts for specific tasks. This is largely compilation of code that isn't really run in production.

## Core Scripts

### `setup_environment.sh`
**Automated environment setup script for the Bluesky Research Infrastructure.**

Creates a complete development environment with all dependencies. Supports both uv (recommended) and conda for package management.

**Usage:**
```bash
# Quick setup with defaults (uv + Python 3.10)
./scripts/setup_environment.sh

# Custom Python version with uv
./scripts/setup_environment.sh --python 3.12

# Use conda instead of uv
./scripts/setup_environment.sh --conda --python 3.11

# Custom environment name
./scripts/setup_environment.sh --env-name my-bluesky-env

# Show help
./scripts/setup_environment.sh --help
```

**Features:**
- ✅ Validates system requirements and installs missing tools
- ✅ Creates virtual environment with specified Python version (3.10, 3.11, 3.12)
- ✅ Installs all project dependencies (core, dev, ML tooling)
- ✅ Sets up pre-commit hooks for code quality
- ✅ Installs project in editable mode
- ✅ Runs validation tests to ensure everything works
- ✅ Provides clear activation instructions

**Options:**
- `-p, --python VERSION` - Python version (default: 3.10)
- `-c, --conda` - Use conda instead of uv
- `-e, --env-name NAME` - Environment name (default: bluesky-research)
- `-h, --help` - Show detailed help

## Infrastructure Scripts

### Deployment Scripts
