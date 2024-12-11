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


locals {
  private_subnets = [
    aws_subnet.private_subnet_a.id,
    aws_subnet.private_subnet_b.id
  ]

  public_subnets = [
    aws_subnet.public_subnet.id
  ]
}






