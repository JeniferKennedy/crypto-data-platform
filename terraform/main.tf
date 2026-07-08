# Terraform — reference for the "make it cloud" step (Phase 9 in the README).
#
# This is intentionally minimal and NOT applied by default. It shows you can
# express infrastructure as code — an S3 bucket to replace MinIO as the data lake.
# When you're ready, `terraform init && terraform plan` (needs AWS credentials).
#
# On a resume, "provisioned cloud storage via Terraform" is the honest claim this
# supports once you've actually run it against a free-tier AWS account.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "lake_bucket_name" {
  type        = string
  description = "Globally-unique S3 bucket name for the data lake."
  default     = "crypto-data-platform-lake-CHANGE-ME"
}

resource "aws_s3_bucket" "lake" {
  bucket = var.lake_bucket_name
  tags = {
    Project = "crypto-data-platform"
    Purpose = "raw-data-lake"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}
