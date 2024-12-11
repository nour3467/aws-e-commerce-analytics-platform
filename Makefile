# Load environment variables
include .env
export

# Variables for Docker Compose and Python
DOCKER_COMPOSE_CMD=docker-compose
SHELL := /bin/bash
VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

# Targets
.PHONY: all setup services generate_products generate_users generate_demo_data

all: setup services generate_categories generate_products generate_users generate_users_demo events_producers events_consumers

setup:
	@echo "Setting up environment..."
	python3 -m venv $(VENV)
	@echo "Activating virtual environment..."
	source venv/bin/activate
	$(PIP) install -r requirements.txt


postgres:
	@echo "Starting Docker Compose..."
	$(DOCKER_COMPOSE_CMD) up -d postgres

services:
	@echo "Starting Docker Compose..."
	$(DOCKER_COMPOSE_CMD) up -d postgres kafka kafka-ui

generate_categories:
	@echo "Generating categories with count $(CATEGORY_COUNT)..."
	$(PYTHON) data_generators/static_generators/product_category_generator.py $(CATEGORY_COUNT)

generate_products:
	@echo "Generating products with count $(PRODUCT_COUNT)..."
	$(PYTHON) data_generators/static_generators/product_generator.py $(PRODUCT_COUNT)

generate_users:
	@echo "Generating users with count $(USER_COUNT)..."
	$(PYTHON) data_generators/static_generators/user_generator.py $(USER_COUNT)

generate_users_demo:
	@echo "Generating users demographics and addresses ...."
	$(PYTHON) data_generators/static_generators/demographic_address_generator.py

events_producers:
	@echo "Building Event Producers..."
	$(DOCKER_COMPOSE_CMD) build session-producer \
		product-view-producer \
		cart-producer \
		cart-items-producer \
		orders-producer \
		order-items-producer \
		support-tickets-producer \
		ticket-messages-producer \
		wishlists-producer

	@echo "Starting Event Producers..."
	$(DOCKER_COMPOSE_CMD) up -d session-producer \
		product-view-producer \
		cart-producer \
		cart-items-producer \
		orders-producer \
		order-items-producer \
		support-tickets-producer \
		ticket-messages-producer \
		wishlists-producer

events_consumers:
	@echo "Building Event Consumers..."
	$(DOCKER_COMPOSE_CMD) build session-consumer \
		product-view-consumer \
		cart-consumer \
		cart-items-consumer \
		orders-consumer \
		order-items-consumer \
		support-tickets-consumer \
		ticket-messages-consumer \
		wishlists-consumer
	@echo "Starting Event Consumers..."
	$(DOCKER_COMPOSE_CMD) up -d session-consumer \
		product-view-consumer \
		cart-consumer \
		cart-items-consumer \
		orders-consumer \
		order-items-consumer \
		support-tickets-consumer \
		ticket-messages-consumer \
		wishlists-consumer





# logs:
# 	@echo "Fetching logs for service $(SERVICE_NAME)..."
# 	$(DOCKER_COMPOSE_CMD) logs -f $(SERVICE_NAME)

cleanup:
	@echo "Stopping Docker Compose and removing unused resources..."
	$(DOCKER_COMPOSE_CMD) down --volumes --remove-orphans
	@echo "Removing the 'postgres_data' volume..."
	docker volume rm postgres_data || echo "Volume 'postgres_data' does not exist or is already removed."
	@echo "Removing the virtual environment..."
	rm -rf venv || echo "'venv' directory does not exist or is already removed."



