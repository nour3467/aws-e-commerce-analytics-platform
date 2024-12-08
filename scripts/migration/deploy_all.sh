#!/bin/bash

# Step 1: Provision Infrastructure
echo "Provisioning AWS infrastructure..."
cd infrastructure/aws/terraform
terraform init
terraform apply -auto-approve

# Step 2: Migrate Database
echo "Starting database migration..."
cd ../../scripts
bash migrate_db.sh
