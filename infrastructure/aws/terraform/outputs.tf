output "rds_endpoint" {
  value       = aws_db_instance.analytics_rds.endpoint
  description = "Amazon RDS PostgreSQL endpoint."
}

output "ecr_producer_repository_url" {
  value       = aws_ecr_repository.producer_repo.repository_url
  description = "ECR repository URL for producer images."
}

output "ecr_consumer_repository_url" {
  value       = aws_ecr_repository.consumer_repo.repository_url
  description = "ECR repository URL for consumer images."
}
