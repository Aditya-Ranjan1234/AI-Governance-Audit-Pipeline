terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.70.0, < 7.0.0" }
    tls = { source = "hashicorp/tls", version = ">= 4.0.0, < 5.0.0" }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags { tags = var.tags }
}

