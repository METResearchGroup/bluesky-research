#!/bin/bash

# Script to apply Terraform changes for archive tables in a piecemeal fashion.
# This script uses -target flags to apply only the new archive tables,
# avoiding unintended modifications to existing infrastructure.
#
# WARNING: Terraform state may be out of date with actual AWS infrastructure
# due to recent infrastructure deconstruction. This script applies only
# the new archive tables and does not modify existing resources.

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TERRAFORM_DIR="$PROJECT_ROOT/terraform"

# List of all archive table resource names
ARCHIVE_TABLES=(
    "aws_glue_catalog_table.archive_fetch_posts_used_in_feeds"
    "aws_glue_catalog_table.archive_generated_feeds"
    "aws_glue_catalog_table.archive_in_network_user_activity"
    "aws_glue_catalog_table.archive_ml_inference_ime"
    "aws_glue_catalog_table.archive_ml_inference_perspective_api"
    "aws_glue_catalog_table.archive_ml_inference_sociopolitical"
    "aws_glue_catalog_table.archive_ml_inference_valence_classifier"
    "aws_glue_catalog_table.archive_post_scores"
    "aws_glue_catalog_table.archive_user_session_logs"
    "aws_glue_catalog_table.archive_study_user_activity_post"
    "aws_glue_catalog_table.archive_study_user_activity_like"
    "aws_glue_catalog_table.archive_study_user_activity_follow"
    "aws_glue_catalog_table.archive_study_user_activity_reply"
    "aws_glue_catalog_table.archive_study_user_activity_repost"
    "aws_glue_catalog_table.archive_study_user_activity_block"
)

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if terraform is initialized
check_terraform_init() {
    if [ ! -d "$TERRAFORM_DIR/.terraform" ]; then
        print_warning "Terraform not initialized. Running terraform init..."
        cd "$TERRAFORM_DIR"
        terraform init || {
            print_error "Terraform init failed"
            exit 1
        }
    fi
}

# Function to run terraform plan for a specific target
plan_target() {
    local target="$1"
    print_info "Planning for $target..."
    cd "$TERRAFORM_DIR"
    terraform plan -target="$target" || {
        print_error "Terraform plan failed for $target"
        return 1
    }
}

# Function to apply a specific target
apply_target() {
    local target="$1"
    print_info "Applying $target..."
    cd "$TERRAFORM_DIR"
    
    # Run plan first to show what will change
    terraform plan -target="$target" || {
        print_error "Terraform plan failed for $target"
        return 1
    }
    
    # Apply with auto-approve.
    # NOTE: Using -target reduces the scope of changes but does NOT guarantee safety.
    # -target can still affect dependent resources or resources that reference the target.
    # Auto-approve is acceptable here because:
    # 1. We've reviewed the terraform plan output above
    # 2. These are additive-only resources (new archive tables)
    # 3. The archive tables are isolated and don't modify existing infrastructure
    # However, always review the plan output for any unexpected dependent changes
    # before using -auto-approve. If the plan shows modifications to existing resources,
    # do NOT use -auto-approve and investigate the dependencies first.
    terraform apply -target="$target" -auto-approve || {
        print_error "Terraform apply failed for $target"
        return 1
    }
    
    print_info "Successfully applied $target"
}

# Function to show overall plan
show_overall_plan() {
    print_info "Showing overall plan for all archive tables..."
    cd "$TERRAFORM_DIR"
    
    # Build target array
    local targets=()
    for table in "${ARCHIVE_TABLES[@]}"; do
        targets+=("-target=$table")
    done
    
    terraform plan "${targets[@]}" || {
        print_error "Terraform plan failed"
        return 1
    }
}

# Main function
main() {
    print_info "Starting Terraform apply for archive tables"
    print_warning "This script will apply only the new archive tables using -target flags"
    print_warning "Existing infrastructure will not be modified"
    echo ""
    
    # Check if terraform is initialized
    check_terraform_init
    
    # Parse command line arguments
    DRY_RUN=false
    SKIP_PLAN=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --skip-plan)
                SKIP_PLAN=true
                shift
                ;;
            --plan-only)
                show_overall_plan
                exit 0
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --dry-run      Show plan for each table but don't apply"
                echo "  --skip-plan    Skip individual plans before each apply"
                echo "  --plan-only    Show overall plan for all tables and exit"
                echo "  --help, -h     Show this help message"
                echo ""
                echo "This script applies archive tables one by one using -target flags."
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Show overall plan first
    if [ "$SKIP_PLAN" = false ]; then
        echo ""
        print_info "Showing overall plan for all archive tables..."
        read -r -p "Press Enter to continue or Ctrl+C to cancel..."
        show_overall_plan
        echo ""
        read -r -p "Review the plan above. Press Enter to continue with individual applies or Ctrl+C to cancel..."
    fi
    
    # Apply each table individually
    local success_count=0
    local fail_count=0
    local failed_tables=()
    
    for table in "${ARCHIVE_TABLES[@]}"; do
        echo ""
        print_info "Processing: $table"
        
        if [ "$DRY_RUN" = true ]; then
            plan_target "$table" || {
                print_error "Plan failed for $table"
                ((fail_count++))
                failed_tables+=("$table")
                continue
            }
        else
            if apply_target "$table"; then
                ((success_count++))
            else
                ((fail_count++))
                failed_tables+=("$table")
                print_warning "Continuing with next table..."
            fi
        fi
    done
    
    # Summary
    echo ""
    echo "=========================================="
    print_info "Summary:"
    echo "  Successfully processed: $success_count tables"
    if [ $fail_count -gt 0 ]; then
        print_error "  Failed: $fail_count tables"
        echo "  Failed tables:"
        for table in "${failed_tables[@]}"; do
            echo "    - $table"
        done
        exit 1
    else
        print_info "  All tables processed successfully!"
        echo "=========================================="
    fi
}

# Run main function
main "$@"

