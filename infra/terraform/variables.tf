variable "aws_region" {
  type = string
}

variable "name" {
  type    = string
  default = "ai-governance"
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "admin_cidr" {
  type        = string
  description = "CIDR allowed to reach the private environment through your approved access path."
}

variable "kafka_version" {
  type    = string
  default = "3.7.x"
}

variable "kafka_instance_type" {
  type    = string
  default = "kafka.m7g.large"
}

variable "eks_instance_types" {
  type    = list(string)
  default = ["m7g.large"]
}

variable "database_name" {
  type    = string
  default = "governance"
}

variable "database_username" {
  type    = string
  default = "governance_admin"
}

variable "multi_az_database" {
  type    = bool
  default = true
}
variable "tags" {
  type = map(string)
  default = {
    Project   = "ai-governance-audit"
    ManagedBy = "terraform"
  }
}
