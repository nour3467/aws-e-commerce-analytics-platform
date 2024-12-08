resource "aws_db_instance" "analytics_rds" {
  allocated_storage    = 20
  engine               = "postgres"
  engine_version       = "14"
  instance_class       = "db.t2.micro"
  db_name              = var.db_name
  username             = var.db_username
  password             = var.db_password
  publicly_accessible  = false
  skip_final_snapshot  = true
  multi_az             = false
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
}

resource "aws_security_group" "rds_sg" {
  name = "${var.project_name}-rds-sg"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Adjust for tighter security (e.g., VPC CIDR)
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
