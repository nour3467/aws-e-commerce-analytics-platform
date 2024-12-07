

resource "aws_ecr_repository" "producer_repo" {
  name = "${var.project_name}-producer"
}

resource "aws_ecr_repository" "consumer_repo" {
  name = "${var.project_name}-consumer"
}
