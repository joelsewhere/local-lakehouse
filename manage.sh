#!/bin/bash
# Local Lakehouse Management Script
# 
# This script orchestrates the startup and shutdown of a complete data lakehouse stack:
# - MinIO (S3-compatible object storage)
# - Nessie (Git-like data catalog for Iceberg)
# - Trino (Distributed SQL query engine)
# - Airflow (Workflow orchestration)
#
# Usage: ./manage-lakehouse.sh [start|stop]

set -e  # Exit immediately if any command fails

# Get the absolute path of the script directory to ensure relative paths work correctly
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to start all lakehouse services in the correct order
start_services() {

    echo "Creating trino/superset network..."
    docker network create local-iceberg-lakehouse

    echo "Starting Local Lakehouse services..."
    
    # Change to script directory to ensure docker-compose files are found
    cd "$SCRIPT_DIR"
    
    # Step 1: Start the data lake infrastructure (MinIO + Nessie)
    echo "Starting data lakehouse services..."
    docker compose -f docker-compose-lakehouse.yml up -d
    sleep 5  # Allow services to initialize
    
    # Step 3: Start Airflow orchestration services
    echo "Starting Airflow orchestration services..."
    docker compose -f docker-compose-orchestrator.yml up -d
    sleep 5

    
    echo "All services started successfully."
    echo ""
    echo "Service Access Information:"
    echo "  - MinIO Console: http://localhost:9001 (admin/password)"
    echo "  - Trino Web UI: http://localhost:8080"
    echo "  - Airflow Web UI: http://localhost:8081 (airflow/airflow)"
    echo "  - Nessie API: http://localhost:19120"
    echo ""

    # Initialize Trino with required schemas
    init_trino
    
    # Uncomment the line below if you want to automatically load seed data on startup
    # load_dbt_seed_data
}

# Function to stop all lakehouse services and clean up resources
stop_services() {
    echo "Stopping Local Lakehouse services..."
    
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Stop services in reverse order (Airflow -> Trino -> Lake)
    # The -v flag removes associated volumes to ensure clean shutdown
    echo "Stopping Airflow services..."
    docker compose -f docker-compose-orchestrator.yml down
    
    echo "Stopping lakehouse services..."
    docker compose -f docker-compose-lakehouse.yml down

    echo "Dropping trino/superset network..."
    docker network rm local-iceberg-lakehouse
    
    echo "All services stopped and volumes cleaned up."
    echo ""
}

# Main script logic - handle command line arguments
case "${1:-help}" in
    "start")
        start_services
        ;;
    "stop")
        stop_services
        ;;
    *)
        echo "Local Lakehouse Management Script"
        echo ""
        echo "Usage: $0 [start|stop]"
        echo ""
        echo "Commands:"
        echo "  start    Start all lakehouse services (MinIO, Nessie, Trino, Airflow)"
        echo "  stop     Stop all services and clean up volumes"
        echo ""
        echo "Examples:"
        echo "  $0 start    # Start the complete lakehouse stack"
        echo "  $0 stop     # Stop all services and clean up"
        echo ""
        echo "After starting, you can access:"
        echo "  - MinIO Console: http://localhost:9001"
        echo "  - Trino Web UI: http://localhost:8080" 
        echo "  - Airflow Web UI: http://localhost:8081"
        ;;
esac


