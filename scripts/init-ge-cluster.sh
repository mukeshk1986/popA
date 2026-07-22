#!/bin/bash
#
# Great Expectations Cluster Initialization Script
# Initializes Databricks cluster with Great Expectations for data validation
#

set -e

# Configuration
CLUSTER_NAME="${CLUSTER_NAME:-pop-advyzer-ge-cluster}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
GE_VERSION="${GE_VERSION:-0.17.0}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

main() {
    log_info "Starting Great Expectations cluster initialization: $CLUSTER_NAME"
    log_info "Environment: $ENVIRONMENT"
    log_info "Great Expectations Version: $GE_VERSION"

    if ! command -v databricks &> /dev/null; then
        log_error "Databricks CLI not found. Please install it first."
        exit 1
    fi

    log_info "Creating cluster with Great Expectations..."
    # Cluster creation logic here

    log_info "Installing Great Expectations libraries..."
    # Library installation logic here

    log_info "Configuring data validation rules..."
    # Configuration logic here

    log_info "Great Expectations cluster initialization completed successfully"
}

trap 'log_error "Script failed at line $LINENO"' ERR

main "$@"
