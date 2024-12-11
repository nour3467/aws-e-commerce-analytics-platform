output "rds_endpoint" {
  value       = aws_db_instance.analytics_rds.endpoint
  description = "Amazon RDS PostgreSQL endpoint."
}


output "rds_db_name" {
  value       = var.db_name
  description = "Amazon RDS PostgreSQL database name from variables."
}

output "rds_username" {
  value       = aws_db_instance.analytics_rds.username
  description = "Amazon RDS PostgreSQL username."
}


# Networking Outputs
output "vpc_id" {
  value = aws_vpc.main_vpc.id
}


output "ecs_security_group_id" {
  value = aws_security_group.ecs_security_group.id
}



# ECR Outputs
output "ecr_producer_repo_urls" {
  value = { for name, repo in aws_ecr_repository.producers : name => repo.repository_url }
}

# Outputs for consumers
output "ecr_consumer_repo_urls" {
  value = { for name, repo in aws_ecr_repository.consumers : name => repo.repository_url }
}

# Networking
output "public_subnet_ids" {
  description = "List of public subnet IDs"
  value = [aws_subnet.public_subnet.id]
}

output "private_subnet_ids" {
  description = "List of private subnet IDs"
  value = [
    aws_subnet.private_subnet_a.id,
    aws_subnet.private_subnet_b.id
  ]
}

# Output for DMS Replication Subnet Group
output "dms_replication_subnet_group_id" {
  description = "DMS Replication Subnet Group ID"
  value       = aws_dms_replication_subnet_group.replication_subnet_group.replication_subnet_group_id
}

output "dms_replication_instance_arn" {
  description = "DMS Replication Instance ARN"
  value       = aws_dms_replication_instance.replication_instance.replication_instance_arn
}

# Output for Source Endpoint ARN
output "dms_source_endpoint_arn" {
  description = "DMS Source Endpoint ARN"
  value       = aws_dms_endpoint.source_postgres.endpoint_arn
}

# Output for Target Endpoint ARN
output "dms_target_endpoint_arn" {
  description = "DMS Target Endpoint ARN"
  value       = aws_dms_endpoint.target_rds.endpoint_arn
}

# Output for DMS Replication Task
output "dms_replication_task_arn" {
  description = "DMS Replication Task ARN"
  value       = aws_dms_replication_task.migration_task.replication_task_arn
}

# Add output for DMS task ARN
output "dms_task_arn" {
  value = aws_dms_replication_task.migration_task.replication_task_arn
  description = "The ARN of the DMS replication task"
}






