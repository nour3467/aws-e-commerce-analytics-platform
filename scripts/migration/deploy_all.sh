#!/bin/bash
# Exit on errors
set -e

# Step 1: Navigate to the script's directory
CURRENT_DIR=$(realpath "$(dirname "$0")")
ENV_FILE="$CURRENT_DIR/../../.env.prod"

# Step 2: Load environment variables
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment variables from $ENV_FILE..."
    set -o allexport
    source "$ENV_FILE"
    set +o allexport
else
    echo "$ENV_FILE not found. Exiting..."
    exit 1
fi

# Step 3: Check if PostgreSQL is running
echo "Checking PostgreSQL status..."
if ! docker ps | grep -q local_postgres; then
    echo "Starting required PostgreSQL..."
    cd "$CURRENT_DIR/../../"
    echo "Current directory: $(pwd)"
    make postgres
fi

# Add PostgreSQL connection test
echo "Testing PostgreSQL connection..."
if ! docker exec local_postgres pg_isready -U postgres; then
    echo "PostgreSQL is not ready. Waiting..."
    sleep 20
    if ! docker exec local_postgres pg_isready -U postgres; then
        echo "PostgreSQL failed to start properly"
        exit 1
    fi
fi

# Step 4: Retrieve AWS DMS Configuration
echo "Setting up AWS DMS for migration..."
cd "$CURRENT_DIR/../../infrastructure/aws/terraform"

# Ensure Terraform outputs exist
RDS_ENDPOINT=$(terraform output -raw rds_endpoint)
RDS_USERNAME=$(terraform output -raw rds_username)
RDS_DB_NAME=$(terraform output -raw rds_db_name)
DMS_TASK_ARN=$(terraform output -raw dms_task_arn)
if [ -z "$RDS_ENDPOINT" ] || [ -z "$RDS_USERNAME" ] || [ -z "$RDS_DB_NAME" ] || [ -z "$DMS_TASK_ARN" ]; then
    echo "Failed to retrieve Terraform outputs. Ensure outputs are configured correctly."
    exit 1
fi

# Test DMS endpoint connections before starting task
echo "Testing DMS endpoints..."
INSTANCE_ARN=$(aws dms describe-replication-instances --query 'ReplicationInstances[0].ReplicationInstanceArn' --output text)
SOURCE_ARN=$(aws dms describe-endpoints --filters Name=endpoint-id,Values=local-postgres-source --query 'Endpoints[0].EndpointArn' --output text)


echo "Testing source endpoint connection..."
aws dms test-connection \
    --replication-instance-arn $INSTANCE_ARN \
    --endpoint-arn $SOURCE_ARN


echo "Waiting for connection test results..."
TEST_STATUS="testing"
while [ "$TEST_STATUS" = "testing" ]; do
    TEST_STATUS=$(aws dms describe-connections \
        --filters Name=replication-instance-arn,Values=$INSTANCE_ARN \
                 Name=endpoint-arn,Values=$SOURCE_ARN \
        --query 'Connections[0].Status' --output text)
    echo "Connection test status: $TEST_STATUS"
    sleep 10
done


if [ "$TEST_STATUS" != "successful" ]; then
    echo "DMS endpoint connection test failed. Status: $TEST_STATUS"
    echo "Please check your PostgreSQL configuration and network connectivity."
    exit 1
fi

echo "DMS endpoint connection test successful!"

echo "RDS Endpoint: $RDS_ENDPOINT"
echo "RDS DB Name: $RDS_DB_NAME"
echo "RDS Username: $RDS_USERNAME"
echo "DMS Task ARN: $DMS_TASK_ARN"

# Step 5: Run AWS DMS Task
echo "Starting DMS Task for migration..."
aws dms start-replication-task --replication-task-arn "$DMS_TASK_ARN" --start-replication-task-type reload-target

# Monitor the task status with more detailed error reporting
echo "Monitoring DMS Task status..."
STATUS="STARTING"
while [ "$STATUS" != "STOPPED" ] && [ "$STATUS" != "FAILED" ]; do
    TASK_INFO=$(aws dms describe-replication-tasks --filters Name=replication-task-arn,Values="$DMS_TASK_ARN")
    STATUS=$(echo $TASK_INFO | jq -r '.ReplicationTasks[0].Status')
    LAST_FAILURE=$(echo $TASK_INFO | jq -r '.ReplicationTasks[0].LastFailureMessage')
    echo "Current status: $STATUS"
    if [ "$LAST_FAILURE" != "null" ]; then
        echo "Last failure message: $LAST_FAILURE"
    fi
    sleep 10
done

if [ "$STATUS" == "FAILED" ]; then
    echo "DMS Task failed. Check AWS DMS logs for details."
    exit 1
fi

echo "DMS Task completed successfully."

# Step 6: Verify Migration
echo "Verifying database migration..."
PGPASSWORD=$TF_VAR_db_password psql -h "$RDS_ENDPOINT" -p "$RDS_DB_PORT" -U "$RDS_USERNAME" -d "$RDS_DB_NAME" -c "\dt"
if [ $? -ne 0 ]; then
    echo "Failed to verify the database schema. Migration might be incomplete."
    exit 1
fi
echo "Database migration verified successfully."

# Optional: Check table row counts
PGPASSWORD=$TF_VAR_db_password psql -h "$RDS_ENDPOINT" -p "$RDS_DB_PORT" -U "$RDS_USERNAME" -d "$RDS_DB_NAME" -c "SELECT table_name, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"
echo "Table row counts verified successfully."

# Step 7: Stop Services
echo "Stopping services using Docker Compose..."
cd "$CURRENT_DIR/../../"
docker-compose down

# Step 8: Cleanup
echo "Migration process completed successfully!"