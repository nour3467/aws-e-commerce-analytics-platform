resource "aws_s3_bucket_policy" "quicksight_access" {
  bucket = aws_s3_bucket.data_bucket.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = { Service = "quicksight.amazonaws.com" }
        Action    = ["s3:GetObject", "s3:ListBucket"]
        Resource  = [
          "${aws_s3_bucket.data_bucket.arn}",
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_s3_bucket_policy" "quicksight_policy" {
  bucket = aws_s3_bucket.data_bucket.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = { Service = "quicksight.amazonaws.com" }
        Action    = ["s3:GetObject", "s3:ListBucket"]
        Resource  = [
          "${aws_s3_bucket.data_bucket.arn}",
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}
