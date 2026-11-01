output "eks_cluster_name" { value = aws_eks_cluster.main.name }
output "msk_bootstrap_brokers_sasl_iam" { value = aws_msk_cluster.main.bootstrap_brokers_sasl_iam }
output "rds_endpoint" { value = aws_db_instance.main.address }
output "rds_master_secret_arn" {
  value     = aws_db_instance.main.master_user_secret[0].secret_arn
  sensitive = true
}
output "workload_role_arn" { value = aws_iam_role.governance_workload.arn }
output "producer_adapter_role_arns" { value = { for name, role in aws_iam_role.producer_adapter : name => role.arn } }
output "ecr_repositories" { value = { for name, repository in aws_ecr_repository.services : name => repository.repository_url } }
