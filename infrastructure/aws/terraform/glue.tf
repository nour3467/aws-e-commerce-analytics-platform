resource "aws_glue_connection" "rds_connection" {
  name = "${var.project_name}-rds-connection"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://${aws_db_instance.analytics_rds.endpoint}/${var.db_name}"
    USERNAME            = "admin"
    PASSWORD            = var.db_password
  }
  physical_connection_requirements {
    subnet_id            = var.private_subnets[0]
    security_group_id_list = [aws_security_group.rds_sg.id]
  }
}


resource "aws_glue_catalog_database" "ecommerce_db" {
  name = "${var.project_name}-ecommerce"
}


resource "aws_glue_crawler" "rds_crawler" {
  name           = "${var.project_name}-rds-crawler"
  database_name  = aws_glue_catalog_database.ecommerce_db.name
  role           = aws_iam_role.glue_role.arn
  description    = "Crawler for eCommerce RDS data"

  jdbc_target {
    connection_name = aws_glue_connection.rds_connection.name
    path            = "${var.db_name}/%"
  }

  schedule = "cron(0 1 * * ? *)" # Run daily at 1 AM
}

resource "aws_iam_role" "glue_role" {
  name = "glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "glue_service_policy" {
  name       = "glue-service-policy-attachment"
  roles      = [aws_iam_role.glue_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy_attachment" "glue_s3_access_policy" {
  name       = "glue-s3-access-policy-attachment"
  roles      = [aws_iam_role.glue_role.name]
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}


resource "aws_glue_job" "daily_kpis_job" {
  name        = "${var.project_name}-daily-kpis-job"
  role_arn    = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.etl_scripts_bucket.bucket}/ETL/daily_kpis_etl.py"
    name            = "glueetl"
  }
  default_arguments = {
    "--TempDir"               = "s3://${aws_s3_bucket.temp_bucket.bucket}/temp/"
    "--job-language"          = "python"
    "--RDS_HOST"              = aws_db_instance.analytics_rds.endpoint
    "--RDS_USER"              = "admin"
    "--RDS_PASSWORD"          = var.db_password
    "--DB_NAME"               = var.db_name
    "--OUTPUT_BUCKET"         = aws_s3_bucket.data_bucket.bucket
    "--RUN_TYPE"              = "daily"
  }
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 5
}

resource "aws_glue_job" "weekly_kpis_job" {
  name        = "${var.project_name}-weekly-kpis-job"
  role_arn    = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.etl_scripts_bucket.bucket}/ETL/weekly_kpis_etl.py"
    name            = "glueetl"
  }
  default_arguments = {
    "--TempDir"               = "s3://${aws_s3_bucket.temp_bucket.bucket}/temp/"
    "--job-language"          = "python"
    "--RDS_HOST"              = aws_db_instance.analytics_rds.endpoint
    "--RDS_USER"              = "admin"
    "--RDS_PASSWORD"          = var.db_password
    "--DB_NAME"               = var.db_name
    "--OUTPUT_BUCKET"         = aws_s3_bucket.data_bucket.bucket
    "--RUN_TYPE"              = "weekly"
  }
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 5
}

resource "aws_glue_job" "monthly_kpis_job" {
  name        = "${var.project_name}-monthly-kpis-job"
  role_arn    = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.etl_scripts_bucket.bucket}/ETL/monthly_kpis_etl.py"
    name            = "glueetl"
  }
  default_arguments = {
    "--TempDir"               = "s3://${aws_s3_bucket.temp_bucket.bucket}/temp/"
    "--job-language"          = "python"
    "--RDS_HOST"              = aws_db_instance.analytics_rds.endpoint
    "--RDS_USER"              = "admin"
    "--RDS_PASSWORD"          = var.db_password
    "--DB_NAME"               = var.db_name
    "--OUTPUT_BUCKET"         = aws_s3_bucket.data_bucket.bucket
    "--RUN_TYPE"              = "monthly"
  }
  glue_version      = "2.0"
  worker_type       = "Standard"
  number_of_workers = 5
}



resource "aws_eventbridge_rule" "etl_trigger" {
  name        = "${var.project_name}-etl-trigger"
  schedule_expression = "rate(1 day)"
  targets {
    id   = "GlueJob"
    arn  = aws_glue_job.etl_job.arn
    role_arn = aws_iam_role.glue_execution_role.arn
  }
}

resource "aws_glue_job" "etl_job" {
  name        = "${var.project_name}-etl-job"
  role_arn    = aws_iam_role.glue_role.arn
  description = "ETL job for processing eCommerce data for KPIs and QuickSight"

  command {
    name            = "glueetl" # Specify the runtime
    script_location = "s3://${aws_s3_bucket.etl_scripts_bucket.bucket}/scripts/etl_script.py" # Path to your ETL script
    python_version  = "3" # Use the appropriate Python version
  }

  default_arguments = {
    "--enable-continuous-log-filter" = "true"
    "--enable-glue-datacatalog"      = "true"
    "--TempDir"                      = "s3://${aws_s3_bucket.temp_bucket.bucket}/temp/"
    "--JOB_NAME"                     = "${var.project_name}-etl-job" # Glue job name
    "--RDS_HOST"                     = aws_db_instance.analytics_rds.endpoint
    "--RDS_USER"                     = "admin"
    "--RDS_PASSWORD"                 = var.db_password
    "--DB_NAME"                      = var.db_name
    "--OUTPUT_BUCKET"                = aws_s3_bucket.data_bucket.bucket
    "--partitionKeys"                = "year,month" # For partitioning by year and month
  }

  max_retries             = 3 # Number of retries for job failures
  number_of_workers       = 5 # Number of workers to handle the job
  worker_type             = "G.1X" # Worker type for cost and performance balance
  glue_version            = "3.0" # Specify Glue version
  timeout                 = 1440 # Timeout in minutes (24 hours)
  security_configuration  = aws_glue_security_configuration.glue_security.name # Enable encryption
  tags = {
    Environment = "production"
    Project     = var.project_name
  }
}

resource "aws_glue_security_configuration" "glue_security" {
  name = "${var.project_name}-security-config"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED" # Enable if you need CloudWatch encryption
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.glue_kms_key.arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.glue_kms_key.arn
    }
  }
}

resource "aws_kms_key" "glue_kms_key" {
  description             = "KMS key for Glue job encryption"
  deletion_window_in_days = 10
}

resource "aws_s3_bucket" "etl_scripts_bucket" {
  bucket = "${var.project_name}-etl-scripts"
}

resource "aws_s3_bucket" "temp_bucket" {
  bucket = "${var.project_name}-temp"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = "${var.project_name}-data"
}


