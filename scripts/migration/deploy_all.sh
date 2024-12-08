#!/bin/bash

# Exit on any error
set -e

# Step 1: Provision Infrastructure
echo "Provisioning AWS infrastructure..."
cd infrastructure/aws/terraform
terraform init
terraform apply -auto-approve

# Step 2: Retrieve Outputs
echo "Retrieving Terraform outputs..."
KAFKA_BOOTSTRAP_SERVERS=$(terraform output -raw kafka_bootstrap_servers)
RDS_HOST=$(terraform output -raw rds_endpoint)
RDS_PASSWORD=$(terraform output -raw rds_password)

# Step 3: Migrate Database
echo "Starting database migration..."
cd ../../scripts
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS \
RDS_HOST=$RDS_HOST \
RDS_PASSWORD=$RDS_PASSWORD \
bash migrate_db.sh

# Step 4: Confirm CI/CD Deployment
echo "Ensure that the CI/CD pipeline (deploy.yml) handles producers and consumers deployment."
