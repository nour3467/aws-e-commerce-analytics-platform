resource "aws_ecs_cluster" "ecs_cluster" {
  name = "${var.project_name}-ecs-cluster"
}

resource "aws_ecs_task_definition" "producer_task" {
  family                   = "producer"
  container_definitions    = <<DEFINITION
[
  {
    "name": "producer",
    "image": "${aws_ecr_repository.producer_repo.repository_url}:latest",
    "cpu": 256,
    "memory": 512,
    "essential": true,
    "command": ["python", "data_generators/event_producers/session_producer.py"]
  }
]
DEFINITION
}

resource "aws_ecs_task_definition" "consumer_task" {
  family                   = "consumer"
  container_definitions    = <<DEFINITION
[
  {
    "name": "consumer",
    "image": "${aws_ecr_repository.consumer_repo.repository_url}:latest",
    "cpu": 256,
    "memory": 512,
    "essential": true,
    "command": ["python", "stream_processing/consumers/session_consumer.py"]
  }
]
DEFINITION
}
