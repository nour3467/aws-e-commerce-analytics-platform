resource "aws_s3_bucket" "data_bucket" {
  bucket = "${var.project_name}-data-bucket"

  tags = {
    Environment = "Production"
    Project     = var.project_name
  }
}

resource "aws_s3_bucket_acl" "data_bucket_acl" {
  bucket = aws_s3_bucket.data_bucket.id
  acl    = "private"
}


