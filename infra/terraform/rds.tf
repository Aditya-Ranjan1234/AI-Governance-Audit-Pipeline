resource "aws_db_subnet_group" "main" {
  name       = "${var.name}-rds"
  subnet_ids = [for subnet in aws_subnet.private : subnet.id]
}

resource "aws_db_instance" "main" {
  identifier                   = "${var.name}-postgres"
  engine                       = "postgres"
  instance_class               = "db.r7g.large"
  allocated_storage            = 100
  max_allocated_storage        = 500
  storage_type                 = "gp3"
  storage_encrypted            = true
  db_name                      = var.database_name
  username                     = var.database_username
  manage_master_user_password  = true
  db_subnet_group_name         = aws_db_subnet_group.main.name
  vpc_security_group_ids       = [aws_security_group.rds.id]
  publicly_accessible          = false
  multi_az                     = var.multi_az_database
  backup_retention_period      = 7
  deletion_protection          = true
  skip_final_snapshot          = false
  final_snapshot_identifier    = "${var.name}-final"
  performance_insights_enabled = true
  auto_minor_version_upgrade   = true
}

resource "aws_ecr_repository" "services" {
  for_each = toset([
    "stream-processor", "risk-scorer", "postgres-sink",
    "keycloak-adapter", "kubernetes-audit-adapter", "airflow-openlineage-adapter",
    "model-registry-adapter", "llm-gateway-adapter"
  ])
  name = "${var.name}/${each.key}"
  image_scanning_configuration { scan_on_push = true }
  image_tag_mutability = "IMMUTABLE"
}
