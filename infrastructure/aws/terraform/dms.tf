# 1. First IAM Roles and Policies (needed before other resources)
# Create the DMS VPC Role
resource "aws_iam_role" "dms_vpc_role" {
  name = "dms-vpc-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "dms.amazonaws.com"
        }
      }
    ]
  })
}

# Create a custom policy for DMS VPC management
resource "aws_iam_role_policy" "dms_vpc_policy" {
  name = "dms-vpc-policy"
  role = aws_iam_role.dms_vpc_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeInternetGateways",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs",
          "ec2:DeleteNetworkInterface",
          "ec2:ModifyNetworkInterfaceAttribute"
        ]
        Resource = "*"
      }
    ]
  })
}


resource "aws_iam_role" "dms_role" {
  name = "dms-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "dms.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "dms_policy" {
  depends_on = [aws_iam_role_policy.dms_vpc_policy]
  role   = aws_iam_role.dms_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "s3:*",
          "ec2:*",
          "rds:*",
          "cloudwatch:*",
          "logs:*"
        ],
        Resource = "*"
      }
    ]
  })
}

# 2. Security Group (needs VPC but before DMS instance)
resource "aws_security_group" "dms_security_group" {
  name        = "dms-security-group"
  description = "Security group for DMS Replication Instance"
  vpc_id      = aws_vpc.main_vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "PostgreSQL from Docker"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["172.18.0.0/16"]
  }

  ingress {
    description = "All traffic from VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main_vpc.cidr_block]
  }
}

# 3. Subnet Group (needs VPC and subnets)
resource "aws_dms_replication_subnet_group" "replication_subnet_group" {
  depends_on = [aws_iam_role_policy.dms_vpc_policy]  # Updated to reference the new policy resource
  replication_subnet_group_id = "dms-replication-subnet-group"
  replication_subnet_group_description = "DMS Replication Subnet Group"
  subnet_ids = local.private_subnets
}

# 4. DMS Instance (needs subnet group and security group)
resource "aws_dms_replication_instance" "replication_instance" {
  depends_on = [aws_dms_replication_subnet_group.replication_subnet_group]
  replication_instance_id    = "dms-replication-instance"
  replication_instance_class = "dms.t3.medium"
  allocated_storage         = 5
  publicly_accessible       = false
  vpc_security_group_ids    = [aws_security_group.dms_security_group.id]
  replication_subnet_group_id = aws_dms_replication_subnet_group.replication_subnet_group.replication_subnet_group_id
}

# 5. Endpoints (need DMS instance)
resource "aws_dms_endpoint" "source_postgres" {
  depends_on = [aws_dms_replication_instance.replication_instance]
  endpoint_id      = "local-postgres-source"
  endpoint_type    = "source"
  engine_name      = "postgres"
  username         = "postgres"  # Match your docker-compose
  password         = "admin_password"  # Match your docker-compose
  server_name      = "102.100.107.232"  # Get this from `curl ifconfig.me`
  port             = 5432
  database_name    = "ecommerce"

  extra_connection_attributes = join(";", [
    "heartbeatFrequency=300",
    "captureDdls=Y"
  ])

  ssl_mode = "none"
}

# Create the DMS Target Endpoint for Amazon RDS
resource "aws_dms_endpoint" "target_rds" {
  depends_on = [aws_dms_replication_instance.replication_instance]
  endpoint_id      = "rds-target"
  endpoint_type    = "target"
  engine_name      = "postgres"
  username         = aws_db_instance.analytics_rds.username
  password         = aws_db_instance.analytics_rds.password
  server_name      = replace(aws_db_instance.analytics_rds.endpoint, ":5432", "")  # Remove port from endpoint
  port             = aws_db_instance.analytics_rds.port
  database_name    = aws_db_instance.analytics_rds.db_name  # Add this line
}

# 6. Replication Task (needs endpoints and instance)
resource "aws_dms_replication_task" "migration_task" {
  depends_on = [
    aws_dms_endpoint.source_postgres,
    aws_dms_endpoint.target_rds
  ]
  replication_task_id      = "postgres-to-rds-task"
  migration_type           = "full-load"
  source_endpoint_arn      = aws_dms_endpoint.source_postgres.endpoint_arn
  target_endpoint_arn      = aws_dms_endpoint.target_rds.endpoint_arn
  replication_instance_arn = aws_dms_replication_instance.replication_instance.replication_instance_arn

  table_mappings = jsonencode({
    "rules": [
      {
        "rule-type": "selection",
        "rule-id": "1",
        "rule-name": "include-all-tables",
        "object-locator": {
          "schema-name": "public",
          "table-name": "%"
        },
        "rule-action": "include"
      }
    ]
  })
}