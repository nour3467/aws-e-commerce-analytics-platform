# Project Name
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

# Networking Variables
variable "public_subnets" {
  description = "List of public subnet IDs"
  type        = list(string)
}

variable "private_subnets" {
  description = "List of private subnet IDs"
  type        = list(string)
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
