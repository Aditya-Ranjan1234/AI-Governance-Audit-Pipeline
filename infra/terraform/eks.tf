resource "aws_iam_role" "eks_cluster" {
  name = "${var.name}-eks-cluster"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "eks.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy_attachment" "eks_cluster" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "main" {
  name                      = "${var.name}-eks"
  role_arn                  = aws_iam_role.eks_cluster.arn
  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
  vpc_config {
    subnet_ids              = [for subnet in aws_subnet.private : subnet.id]
    security_group_ids      = [aws_security_group.eks_control_plane.id]
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = [var.admin_cidr]
  }
  depends_on = [aws_iam_role_policy_attachment.eks_cluster]
}

resource "aws_iam_role" "eks_nodes" {
  name = "${var.name}-eks-nodes"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "nodes_worker" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}
resource "aws_iam_role_policy_attachment" "nodes_cni" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}
resource "aws_iam_role_policy_attachment" "nodes_ecr" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "governance-spot-workers"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = [for subnet in aws_subnet.private : subnet.id]
  instance_types  = var.eks_instance_types
  ami_type        = "AL2023_ARM_64_STANDARD"
  capacity_type   = "SPOT"
  scaling_config {
    desired_size = 3
    min_size     = 3
    max_size     = 6
  }
  update_config { max_unavailable = 1 }
  lifecycle { create_before_destroy = true }
  depends_on = [aws_iam_role_policy_attachment.nodes_worker, aws_iam_role_policy_attachment.nodes_cni, aws_iam_role_policy_attachment.nodes_ecr]
}

data "tls_certificate" "eks" { url = aws_eks_cluster.main.identity[0].oidc[0].issuer }
resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
}

resource "aws_iam_role" "governance_workload" {
  name = "${var.name}-workload"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow", Action = "sts:AssumeRoleWithWebIdentity",
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn },
      Condition = { StringEquals = {
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com",
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:governance:governance-pipeline"
      } }
    }]
  })
}

resource "aws_iam_role_policy" "governance_msk" {
  name = "msk-access"
  role = aws_iam_role.governance_workload.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:WriteDataIdempotently"], Resource = aws_msk_cluster.main.arn },
      { Effect = "Allow", Action = ["kafka-cluster:ReadData", "kafka-cluster:WriteData", "kafka-cluster:DescribeTopic", "kafka-cluster:CreateTopic"], Resource = "${replace(aws_msk_cluster.main.arn, ":cluster/", ":topic/")}/*" },
      { Effect = "Allow", Action = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"], Resource = "${replace(aws_msk_cluster.main.arn, ":cluster/", ":group/")}/*" },
      { Effect = "Allow", Action = ["kafka-cluster:AlterTransactionalId", "kafka-cluster:DescribeTransactionalId"], Resource = "${replace(aws_msk_cluster.main.arn, ":cluster/", ":transactional-id/")}/*" }
    ]
  })
}

locals {
  producer_topics = {
    keycloak-adapter            = "governance.keycloak.v1"
    kubernetes-audit-adapter    = "governance.kubernetes.v1"
    airflow-openlineage-adapter = "governance.pipeline.v1"
    model-registry-adapter      = "governance.model-registry.v1"
    llm-gateway-adapter         = "governance.llm-gateway.v1"
  }
}

resource "aws_iam_role" "producer_adapter" {
  for_each = local.producer_topics
  name     = "${var.name}-${each.key}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow", Action = "sts:AssumeRoleWithWebIdentity",
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn },
      Condition = { StringEquals = {
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com",
        "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:governance:${each.key}"
      } }
    }]
  })
}

resource "aws_iam_role_policy" "producer_msk" {
  for_each = local.producer_topics
  name     = "write-own-msk-topic"
  role     = aws_iam_role.producer_adapter[each.key].id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:WriteDataIdempotently"],
        Resource = aws_msk_cluster.main.arn
      },
      {
        Effect = "Allow",
        Action = ["kafka-cluster:WriteData", "kafka-cluster:DescribeTopic"],
        Resource = [
          "${replace(aws_msk_cluster.main.arn, ":cluster/", ":topic/")}/${each.value}",
          "${replace(aws_msk_cluster.main.arn, ":cluster/", ":topic/")}/governance.dead-letter.v1"
        ]
      }
    ]
  })
}
