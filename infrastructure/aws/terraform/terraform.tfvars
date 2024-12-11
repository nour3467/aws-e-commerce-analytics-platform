# Project name for resource naming
project_name = "aws-ecommerce-platform"

# Networking configuration

private_subnets = []
public_subnets  = []




# RDS Configuration
db_name      = "ecommerce"
db_username  = "db_admin"  # Define the RDS username here
db_password  = "secureDBPassword123"  # Used for RDS credentials

db_user_local       = "postgres"
db_password_local   = "admin_password"
db_host_local       = "localhost"
local_db_port_local = "5432"
db_name_local       = "ecommerce"

source_db_cidr = "192.168.1.0/24" # Replace with your local network CIDR

