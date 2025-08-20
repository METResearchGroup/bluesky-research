#!/bin/bash

# Bluesky Research Infrastructure - Environment Setup Script
# Automates the creation of development environment with all dependencies
#
# Usage:
#   ./scripts/setup_environment.sh [OPTIONS]
#
# Options:
#   -p, --python VERSION    Python version to use (default: 3.10)
#   -c, --conda            Use conda instead of uv for package management
#   -e, --env-name NAME    Environment name (default: bluesky-research)
#   -h, --help             Show this help message
#
# Examples:
#   ./scripts/setup_environment.sh                           # Default setup with uv and Python 3.10
#   ./scripts/setup_environment.sh --conda --python 3.12     # Use conda with Python 3.12
#   ./scripts/setup_environment.sh --env-name my-env         # Custom environment name

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Default configuration
DEFAULT_PYTHON_VERSION="3.10"
DEFAULT_ENV_NAME="bluesky-research"
USE_CONDA=false
PYTHON_VERSION=""
ENV_NAME=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Bluesky Research Infrastructure - Environment Setup Script

DESCRIPTION:
    Automates the creation of a complete development environment with all
    project dependencies. Supports both uv (recommended) and conda for
    package management.

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -p, --python VERSION    Python version to use (default: $DEFAULT_PYTHON_VERSION)
                           Supported: 3.10, 3.11, 3.12
    -c, --conda            Use conda instead of uv for package management
    -e, --env-name NAME    Environment name (default: $DEFAULT_ENV_NAME)
    -h, --help             Show this help message

EXAMPLES:
    # Default setup with uv and Python 3.10
    $0

    # Use conda with Python 3.12
    $0 --conda --python 3.12

    # Custom environment name with uv
    $0 --env-name my-bluesky-env --python 3.11

    # Full conda setup with custom name
    $0 --conda --python 3.12 --env-name bluesky-dev

DEPENDENCIES:
    For uv setup:  uv package manager
    For conda:     conda or miniconda

WHAT THIS SCRIPT DOES:
    1. Validates system requirements
    2. Creates virtual environment (uv venv or conda env)
    3. Installs core project dependencies
    4. Installs development dependencies
    5. Installs ML tooling dependencies
    6. Sets up pre-commit hooks
    7. Installs project in editable mode
    8. Runs validation tests

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -p|--python)
                PYTHON_VERSION="$2"
                shift 2
                ;;
            -c|--conda)
                USE_CONDA=true
                shift
                ;;
            -e|--env-name)
                ENV_NAME="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                echo "Use --help for usage information."
                exit 1
                ;;
        esac
    done

    # Set defaults if not provided
    PYTHON_VERSION="${PYTHON_VERSION:-$DEFAULT_PYTHON_VERSION}"
    ENV_NAME="${ENV_NAME:-$DEFAULT_ENV_NAME}"
}

# Validate Python version
validate_python_version() {
    case "$PYTHON_VERSION" in
        3.10|3.11|3.12)
            log_info "Using Python $PYTHON_VERSION"
            ;;
        *)
            log_error "Unsupported Python version: $PYTHON_VERSION"
            log_error "Supported versions: 3.10, 3.11, 3.12"
            exit 1
            ;;
    esac
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Validate system requirements
validate_system() {
    log_info "Validating system requirements..."

    # Check if we're in the correct directory
    if [[ ! -f "$PROJECT_ROOT/requirements.in" ]]; then
        log_error "Please run this script from the project root or ensure requirements.in exists"
        exit 1
    fi

    if $USE_CONDA; then
        if ! command_exists conda; then
            log_error "conda not found. Please install conda or miniconda first."
            log_info "Visit: https://docs.conda.io/en/latest/miniconda.html"
            exit 1
        fi
        log_info "Found conda: $(conda --version)"
    else
        if ! command_exists uv; then
            log_error "uv not found. Installing uv..."
            if command_exists curl; then
                curl -LsSf https://astral.sh/uv/install.sh | sh
                # Source the shell to get uv in PATH
                export PATH="$HOME/.cargo/bin:$PATH"
                if ! command_exists uv; then
                    log_error "Failed to install uv. Please install manually:"
                    log_info "Visit: https://github.com/astral-sh/uv#installation"
                    exit 1
                fi
            else
                log_error "curl not found. Please install uv manually:"
                log_info "Visit: https://github.com/astral-sh/uv#installation"
                exit 1
            fi
        fi
        log_info "Found uv: $(uv --version)"
    fi

    # Check for git (needed for pre-commit)
    if ! command_exists git; then
        log_error "git not found. Please install git first."
        exit 1
    fi

    log_success "System requirements validated"
}

# Create environment with uv
create_uv_environment() {
    log_info "Creating uv virtual environment: $ENV_NAME"
    
    cd "$PROJECT_ROOT"
    
    # Remove existing environment if it exists
    if [[ -d ".venv" ]]; then
        log_warning "Removing existing .venv directory"
        rm -rf .venv
    fi

    # Create new virtual environment
    uv venv --python "$PYTHON_VERSION" .venv
    
    # Activate environment
    source .venv/bin/activate
    
    log_success "Created uv environment with Python $PYTHON_VERSION"
}

# Create environment with conda
create_conda_environment() {
    log_info "Creating conda environment: $ENV_NAME"
    
    # Remove existing environment if it exists
    if conda info --envs | grep -q "^$ENV_NAME "; then
        log_warning "Removing existing conda environment: $ENV_NAME"
        conda env remove -n "$ENV_NAME" -y
    fi

    # Create new conda environment
    conda create -n "$ENV_NAME" python="$PYTHON_VERSION" -y
    
    log_success "Created conda environment: $ENV_NAME with Python $PYTHON_VERSION"
}

# Install dependencies with uv
install_uv_dependencies() {
    log_info "Installing dependencies with uv..."
    
    cd "$PROJECT_ROOT"
    source .venv/bin/activate
    
    # Install core dependencies
    log_info "Installing core dependencies..."
    uv pip install -r requirements.txt
    
    # Install development dependencies
    log_info "Installing development dependencies..."
    uv pip install -r dev_requirements.txt
    
    # Install ML tooling dependencies
    log_info "Installing ML tooling dependencies..."
    uv pip install -r ml_tooling/requirements.txt
    
    # Install project in editable mode
    log_info "Installing project in editable mode..."
    uv pip install -e .
    
    log_success "All dependencies installed successfully"
}

# Install dependencies with conda/pip
install_conda_dependencies() {
    log_info "Installing dependencies with conda/pip..."
    
    # Activate conda environment
    eval "$(conda shell.bash hook)"
    conda activate "$ENV_NAME"
    
    # Install pip-tools for requirements compilation
    pip install pip-tools
    
    # Install core dependencies
    log_info "Installing core dependencies..."
    pip install -r requirements.txt
    
    # Install development dependencies
    log_info "Installing development dependencies..."
    pip install -r dev_requirements.txt
    
    # Install ML tooling dependencies
    log_info "Installing ML tooling dependencies..."
    pip install -r ml_tooling/requirements.txt
    
    # Install project in editable mode
    log_info "Installing project in editable mode..."
    pip install -e .
    
    log_success "All dependencies installed successfully"
}

# Setup pre-commit hooks
setup_pre_commit() {
    log_info "Setting up pre-commit hooks..."
    
    cd "$PROJECT_ROOT"
    
    if $USE_CONDA; then
        eval "$(conda shell.bash hook)"
        conda activate "$ENV_NAME"
    else
        source .venv/bin/activate
    fi
    
    # Install and setup pre-commit hooks
    pre-commit install
    
    log_success "Pre-commit hooks installed"
}

# Run validation tests
run_validation() {
    log_info "Running validation tests..."
    
    cd "$PROJECT_ROOT"
    
    if $USE_CONDA; then
        eval "$(conda shell.bash hook)"
        conda activate "$ENV_NAME"
    else
        source .venv/bin/activate
    fi
    
    # Test key imports
    log_info "Testing core imports..."
    python -c "
import sys
print(f'Python version: {sys.version}')

test_imports = [
    'aiosqlite',
    'atproto', 
    'boto3',
    'pandas',
    'flask',
    'pytest',
    'ruff'
]

failed_imports = []
for module in test_imports:
    try:
        __import__(module)
        print(f'✅ {module}')
    except ImportError as e:
        print(f'❌ {module}: {e}')
        failed_imports.append(module)

if failed_imports:
    print(f'\\nFailed imports: {failed_imports}')
    sys.exit(1)
else:
    print('\\n✅ All core imports successful!')
"

    # Run linter check
    log_info "Running linter check..."
    ruff check . --exit-zero
    
    # Run a subset of tests to validate environment
    log_info "Running core library tests..."
    export PYTHONPATH="$PROJECT_ROOT"
    pytest lib/tests -v --tb=short -x
    
    log_success "Validation tests passed!"
}

# Display final instructions
show_final_instructions() {
    log_success "Environment setup completed successfully!"
    echo
    log_info "Environment Details:"
    echo "  • Environment name: $ENV_NAME"
    echo "  • Python version: $PYTHON_VERSION"
    echo "  • Package manager: $(if $USE_CONDA; then echo 'conda'; else echo 'uv'; fi)"
    echo "  • Project root: $PROJECT_ROOT"
    echo
    
    if $USE_CONDA; then
        log_info "To activate your environment:"
        echo "  conda activate $ENV_NAME"
        echo
        log_info "To deactivate:"
        echo "  conda deactivate"
    else
        log_info "To activate your environment:"
        echo "  source .venv/bin/activate"
        echo
        log_info "To deactivate:"
        echo "  deactivate"
    fi
    
    echo
    log_info "Next steps:"
    echo "  1. Activate your environment (see above)"
    echo "  2. Copy .env.example to .env and configure your API keys"
    echo "  3. Start development with 'cd bluesky_database/frontend && npm install && npm run dev'"
    echo "  4. Run tests with 'pytest'"
    echo "  5. See README.md for detailed usage instructions"
    echo
    log_info "Happy coding! 🚀"
}

# Main execution
main() {
    echo "Bluesky Research Infrastructure - Environment Setup"
    echo "=================================================="
    echo
    
    parse_args "$@"
    validate_python_version
    validate_system
    
    if $USE_CONDA; then
        create_conda_environment
        install_conda_dependencies
    else
        create_uv_environment
        install_uv_dependencies
    fi
    
    setup_pre_commit
    run_validation
    show_final_instructions
}

# Run main function with all arguments
main "$@"
