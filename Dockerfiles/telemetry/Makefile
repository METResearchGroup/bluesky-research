.PHONY: help start stop restart status logs clean setup

# Determine which docker compose command to use
DOCKER_COMPOSE := $(shell if command -v docker-compose > /dev/null; then echo "docker-compose"; else echo "docker compose"; fi)

# Default target
help:
	@echo "Available commands:"
	@echo "  make setup    - Create required directories and prepare configuration files"
	@echo "  make start    - Start Prometheus and Grafana containers"
	@echo "  make stop     - Stop the running containers"
	@echo "  make restart  - Restart the containers"
	@echo "  make status   - Check status of containers"
	@echo "  make logs     - Show logs from both containers"
	@echo "  make clean    - Stop and remove containers, networks, and volumes"

# Setup the telemetry environment
setup:
	@echo "Setting up telemetry environment..."
	mkdir -p grafana/provisioning/dashboards grafana/provisioning/datasources grafana/dashboards
	@echo "Setup complete, you can now run 'make start'"

# Start the telemetry stack
start:
	@echo "Starting Prometheus and Grafana..."
	$(DOCKER_COMPOSE) up -d
	@echo "Grafana available at: http://localhost:3000"
	@echo "Grafana default credentials: admin/default"
	@echo ""
	@echo "IMPORTANT: To see metrics in Prometheus:"
	@echo "1. Make sure your Python app is exposing metrics on port 8000"
	@echo "2. In Prometheus, try querying for 'backfill_endpoint_requests_total' or '{job=\"backfill_service\"}'"
	@echo ""
	@echo "IMPORTANT: To view dashboards in Grafana:"
	@echo "1. Log in to Grafana at http://localhost:3000 (admin/default)"
	@echo "2. Go to Dashboards -> Browse"
	@echo "3. You should see the 'Backfill Service Overview' dashboard"

# Stop the containers
stop:
	@echo "Stopping containers..."
	$(DOCKER_COMPOSE) stop

# Restart the containers
restart:
	@echo "Restarting containers..."
	$(DOCKER_COMPOSE) restart

# Check the status of the containers
status:
	@echo "Container status:"
	$(DOCKER_COMPOSE) ps

# View the logs
logs:
	$(DOCKER_COMPOSE) logs -f

# Clean everything
clean:
	@echo "Stopping and removing containers, networks, and volumes..."
	$(DOCKER_COMPOSE) down -v
	@echo "Cleanup complete" 