#!/bin/bash

# Exit on errors
set -e

# Load variables into the shell
set -a
source .env
set +a

# Load sensitive information from environment variables
DB_NAME=${DB_NAME:-ecommerce}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD}
DB_HOST=${DB_HOST:-localhost}
LOCAL_DB_PORT=${LOCAL_DB_PORT:-5432}
RDS_DB_PORT=${RDS_DB_PORT:-5432}
DUMP_DIR=${DUMP_DIR:-/tmp}
DUMP_FILE="$DUMP_DIR/db_backup.sql"

# Step 1: Retrieve RDS Endpoint
echo "Retrieving RDS endpoint from Terraform..."
cd ../infrastructure/aws/terraform
RDS_ENDPOINT=$(terraform output -raw rds_endpoint)
if [ -z "$RDS_ENDPOINT" ]; then
  echo "Failed to retrieve RDS endpoint. Ensure Terraform outputs are configured."
  exit 1
fi
echo "RDS Endpoint: $RDS_ENDPOINT"

# Step 2: Export Local PostgreSQL Database
echo "Exporting local database..."
PGPASSWORD=$DB_PASSWORD pg_dump -h $DB_HOST -p $LOCAL_DB_PORT -U $DB_USER $DB_NAME > $DUMP_FILE
if [ $? -ne 0 ]; then
  echo "Failed to export the database. Check local PostgreSQL connection and credentials."
  exit 1
fi
echo "Database exported successfully to $DUMP_FILE."

# Step 3: Restore Database to Amazon RDS
echo "Restoring database to Amazon RDS..."
PGPASSWORD=$DB_PASSWORD psql -h $RDS_ENDPOINT -p $RDS_DB_PORT -U $DB_USER -d $DB_NAME -f $DUMP_FILE
if [ $? -ne 0 ]; then
  echo "Failed to restore the database to RDS. Check RDS connection and credentials."
  exit 1
fi
echo "Database restored successfully to Amazon RDS."

# Step 4: Verify Migration
echo "Verifying database migration..."
PGPASSWORD=$DB_PASSWORD psql -h $RDS_ENDPOINT -p $RDS_DB_PORT -U $DB_USER -d $DB_NAME -c "\dt"
if [ $? -ne 0 ]; then
  echo "Failed to verify the database schema. Migration might be incomplete."
  exit 1
fi

# Optional: Check table row counts
PGPASSWORD=$DB_PASSWORD psql -h $RDS_ENDPOINT -p $RDS_DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT table_name, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"
echo "Database migration verified successfully."

# Step 5: Cleanup
echo "Cleaning up..."
rm $DUMP_FILE
echo "Migration process completed successfully!"
