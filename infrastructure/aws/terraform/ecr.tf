resource "aws_ecr_repository" "producer_repo" {
  name = "${var.project_name}-producer-repo"
  image_tag_mutability = "MUTABLE"
  lifecycle_policy {
    policy = <<EOL
    {
      "rules": [
        {
          "rulePriority": 1,
          "description": "Expire untagged images after 30 days",
          "selection": {
            "tagStatus": "untagged",
            "countType": "imageCountMoreThan",
            "countNumber": 30
          },
          "action": {
            "type": "expire"
          }
        }
      ]
    }
    EOL
  }
}

resource "aws_ecr_repository" "consumer_repo" {
  name = "${var.project_name}-consumer-repo"
  image_tag_mutability = "MUTABLE"
  lifecycle_policy {
    policy = <<EOL
    {
      "rules": [
        {
          "rulePriority": 1,
          "description": "Expire untagged images after 30 days",
          "selection": {
            "tagStatus": "untagged",
            "countType": "imageCountMoreThan",
            "countNumber": 30
          },
          "action": {
            "type": "expire"
          }
        }
      ]
    }
    EOL
  }
}

output "ecr_producer_repo_url" {
  value = aws_ecr_repository.producer_repo.repository_url
  description = "Producer ECR Repository URL"
}

output "ecr_consumer_repo_url" {
  value = aws_ecr_repository.consumer_repo.repository_url
  description = "Consumer ECR Repository URL"
}
