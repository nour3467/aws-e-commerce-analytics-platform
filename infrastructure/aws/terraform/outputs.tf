

output "rds_endpoint" {
  value = aws_db_instance.analytics_rds.endpoint
}

output "ecr_repository_url" {
  value = aws_ecr_repository.producer_repo.repository_url
}
