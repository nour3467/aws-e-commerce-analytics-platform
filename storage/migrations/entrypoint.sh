#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h postgres -U postgres; do
  echo "PostgreSQL is not ready yet. Waiting..."
  sleep 2
done

# Check if the database contains user-defined tables
echo "Checking if the database is empty..."
DB_EMPTY=$(psql -h postgres -U postgres -d postgres -tAc "
SELECT CASE
  WHEN EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
  )
  THEN 'false'
  ELSE 'true'
END;
")

if [ "$DB_EMPTY" == "true" ]; then
  echo "Database is empty. Running initialization script..."
  if [ -f /docker-entrypoint-initdb.d/initdb.sql ]; then
    psql -h postgres -U postgres -d postgres -f /docker-entrypoint-initdb.d/initdb.sql
  else
    echo "Initialization script not found. Skipping initialization."
  fi
else
  echo "Database already contains data. Skipping initialization."
fi

echo "PostgreSQL is ready and initialization complete."
