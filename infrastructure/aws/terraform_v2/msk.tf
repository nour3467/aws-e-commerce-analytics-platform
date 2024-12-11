resource "aws_msk_cluster" "kafka_cluster" {
  cluster_name           = "${var.project_name}-kafka-cluster"
  kafka_version          = "2.8.1"
  number_of_broker_nodes = 3
  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.private_subnets
    security_groups = [aws_security_group.ecs_security_group.id]
  }
}

output "kafka_bootstrap_servers" {
  value       = aws_msk_cluster.kafka_cluster.bootstrap_brokers
  description = "Bootstrap servers for Kafka"
}
