#!/bin/bash
# Exit on errors
set -e

# Step 1: Navigate to the script's directory
CURRENT_DIR=$(realpath "$(dirname "$0")")
DUMP_FILE="$CURRENT_DIR/../migration/dump/db_backup.sql"

# Step 2: Check if PostgreSQL is running
echo "Checking PostgreSQL status..."
if ! docker ps | grep -q local_postgres; then
    echo "PostgreSQL container (local_postgres) is not running. Exiting..."
    exit 1
fi

# Step 3: Warn the user about overriding the database
echo "WARNING: This script will override the current database in the 'local_postgres' container."
echo "All existing data in the database will be lost!"
read -p "Do you want to continue? (yes/no): " CONFIRMATION

if [[ "$CONFIRMATION" != "yes" ]]; then
    echo "Operation canceled. Exiting..."
    exit 0
fi

# Step 4: Verify the SQL dump file exists
if [ ! -f "$DUMP_FILE" ]; then
    echo "SQL dump file not found at $DUMP_FILE. Exiting..."
    exit 1
fi

# Step 5: Terminate active connections and drop the database
echo "Overriding the database in 'local_postgres'..."

# Default database name (change this if your database has a different name)
DB_NAME="postgres"
DB_USER="postgres"

# Switch to a different database (e.g., template1) to avoid being connected to the target database
echo "Switching to 'template1' to avoid conflicts..."
docker exec local_postgres psql -U "$DB_USER" -d template1 -c "
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = '$DB_NAME' AND pid <> pg_backend_pid();
"

# Drop and recreate the database
echo "Dropping and recreating the database..."
docker exec local_postgres psql -U "$DB_USER" -d template1 -c "DROP DATABASE IF EXISTS $DB_NAME;"
docker exec local_postgres psql -U "$DB_USER" -d template1 -c "CREATE DATABASE $DB_NAME;"

# Step 6: Restore the database from the SQL dump file
echo "Restoring database from $DUMP_FILE..."
docker exec -i local_postgres psql -U "$DB_USER" -d "$DB_NAME" < "$DUMP_FILE"

# Step 7: Confirm success
echo "Database successfully overridden with the contents of $DUMP_FILE."