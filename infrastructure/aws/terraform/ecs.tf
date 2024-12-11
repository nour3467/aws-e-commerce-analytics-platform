# ECS Cluster
resource "aws_ecs_cluster" "ecs_cluster" {
  name = "${var.project_name}-ecs-cluster"
}

# Producers Task Definition
resource "aws_ecs_task_definition" "producers" {
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

  family                   = "${var.project_name}-${each.key}-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  container_definitions    = jsonencode([
    {
      name      = each.key
      image     = "${aws_ecr_repository.producers[each.key].repository_url}:latest"
      cpu       = 256
      memory    = 512
      essential = true
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_cluster.kafka_cluster.bootstrap_brokers },
        { name = "TOPIC_NAME", value = each.key },
        { name = "AWS_EXECUTION_ENV", value = "AWS" } # Added here
      ]
    }
  ])
}

# Consumers Task Definition
resource "aws_ecs_task_definition" "consumers" {
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

  family                   = "${var.project_name}-${each.key}-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  container_definitions    = jsonencode([
    {
      name      = each.key
      image     = "${aws_ecr_repository.consumers[each.key].repository_url}:latest"
      cpu       = 256
      memory    = 512
      essential = true
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_cluster.kafka_cluster.bootstrap_brokers },
        { name = "RDS_HOST", value = aws_db_instance.analytics_rds.endpoint },
        { name = "RDS_USER", value = "admin" }, # Static for now; update as needed
        { name = "RDS_PASSWORD", value = var.db_password },
        { name = "TOPIC_NAME", value = each.key },
        { name = "AWS_EXECUTION_ENV", value = "AWS" } # Added here
      ]
    }
  ])
}

# Producers ECS Service
resource "aws_ecs_service" "producers" {
  for_each = aws_ecs_task_definition.producers

  name            = "${var.project_name}-${each.key}-service"
  cluster         = aws_ecs_cluster.ecs_cluster.id
  task_definition = each.value.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = local.private_subnets
    security_groups = [aws_security_group.ecs_security_group.id]
  }
}


# Consumers ECS Service
resource "aws_ecs_service" "consumers" {
  for_each = aws_ecs_task_definition.consumers

  name            = "${var.project_name}-${each.key}-service"
  cluster         = aws_ecs_cluster.ecs_cluster.id
  task_definition = each.value.arn
  desired_count   = 1
  launch_type     = "FARGATE"
  network_configuration {
    subnets         = local.private_subnets
    security_groups = [aws_security_group.ecs_security_group.id]
  }
}

# Outputs for ECS Services
output "ecs_producers_services" {
  value = { for name, service in aws_ecs_service.producers : name => service.name }
}

output "ecs_consumers_services" {
  value = { for name, service in aws_ecs_service.consumers : name => service.name }
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name = "ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "ecs_task_execution_role_policy" {
  name       = "ecs-task-execution-role-policy-attachment"
  roles      = [aws_iam_role.ecs_task_execution_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
