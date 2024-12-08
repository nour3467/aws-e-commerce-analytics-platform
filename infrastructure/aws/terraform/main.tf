provider "aws" {
  region = "us-east-1" # Replace with your preferred region
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}



