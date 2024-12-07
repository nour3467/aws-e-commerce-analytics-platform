resource "aws_ecs_service" "producer_service" {
  name            = "producer-service"
  cluster         = aws_ecs_cluster.ecs_cluster.id
  task_definition = aws_ecs_task_definition.producer_task.arn
  desired_count   = 1
}

resource "aws_ecs_service" "consumer_service" {
  name            = "consumer-service"
  cluster         = aws_ecs_cluster.ecs_cluster.id
  task_definition = aws_ecs_task_definition.consumer_task.arn
  desired_count   = 1
}
