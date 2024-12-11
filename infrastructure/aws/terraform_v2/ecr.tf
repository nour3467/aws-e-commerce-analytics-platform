# Define ECR repositories for producers
resource "aws_ecr_repository" "producers" {
  for_each = toset([
    "cart_producer",
    "cart_items_producer",
    "order_producer",
    "order_items_producer",
    "product_view_producer",
    "session_producer",
    "support_tickets_producer",
    "ticket_messages_producer",
    "wishlists_producer"
  ])

  name = "${var.project_name}-${each.key}-repo"
}

# Define ECR lifecycle policies for producers
resource "aws_ecr_lifecycle_policy" "producer_policies" {
  for_each = aws_ecr_repository.producers

  repository = each.value.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 30 days"
        selection = {
          tagStatus = "untagged"
          countType = "imageCountMoreThan"
          countNumber = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Define ECR repositories for consumers
resource "aws_ecr_repository" "consumers" {
  for_each = toset([
    "cart_consumer",
    "cart_items_consumer",
    "order_consumer",
    "order_items_consumer",
    "product_view_consumer",
    "session_consumer",
    "support_tickets_consumer",
    "ticket_messages_consumer",
    "wishlists_consumer"
  ])

  name = "${var.project_name}-${each.key}-repo"
}

# Define ECR lifecycle policies for consumers
resource "aws_ecr_lifecycle_policy" "consumer_policies" {
  for_each = aws_ecr_repository.consumers

  repository = each.value.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 30 days"
        selection = {
          tagStatus = "untagged"
          countType = "imageCountMoreThan"
          countNumber = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Outputs for producers

