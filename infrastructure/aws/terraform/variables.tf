# Project Name
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

# RDS Variables
variable "db_name" {
  description = "The name of the database to be created in RDS"
  type        = string
}

variable "db_username" {
  description = "The username for the RDS database"
  type        = string
}

variable "db_password" {
  description = "The password for the RDS database"
  type        = string
  sensitive   = true
}

# Fetch the private subnet IDs dynamically
variable "private_subnets" {
  description = "List of private subnet IDs"
  type        = list(string)
}

# Fetch the public subnet IDs dynamically
variable "public_subnets" {
  description = "List of public subnet IDs"
  type        = list(string)
}

variable "db_name_local" {
  description = "Local database name"
  type        = string
}

variable "db_user_local" {
  description = "Local database username"
  type        = string
}

variable "db_password_local" {
  description = "Local database password"
  type        = string
}

variable "db_host_local" {
  description = "Local database host"
  type        = string
}

variable "local_db_port_local" {
  description = "Local database port"
  type        = number
}


variable "source_db_cidr" {
  description = "The CIDR block for the source PostgreSQL database."
  type        = string
}

