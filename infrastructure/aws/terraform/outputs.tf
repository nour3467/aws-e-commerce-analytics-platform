output "rds_endpoint" {
  value       = aws_db_instance.analytics_rds.endpoint
  description = "Amazon RDS PostgreSQL endpoint."
}

# Networking Outputs
output "vpc_id" {
  value = aws_vpc.main_vpc.id
}

output "public_subnet_id" {
  value = aws_subnet.public_subnet.id
}

output "private_subnet_id" {
  value = aws_subnet.private_subnet.id
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






