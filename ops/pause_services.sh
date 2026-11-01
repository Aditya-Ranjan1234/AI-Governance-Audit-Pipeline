#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_REGION="${AWS_REGION:-us-west-2}"
EKS_CLUSTER="${EKS_CLUSTER:-ai-governance-eks}"
NODE_GROUP="${NODE_GROUP:-governance-spot-workers}"
DB_INSTANCE="${DB_INSTANCE:-ai-governance-postgres}"
NAMESPACE="${NAMESPACE:-governance}"

cd "$ROOT_DIR"

docker compose stop postgres >/dev/null
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION" >/dev/null

kubectl delete hpa --all -n "$NAMESPACE" --ignore-not-found >/dev/null
kubectl scale deployment --all -n "$NAMESPACE" --replicas=0 >/dev/null

aws eks update-nodegroup-config \
  --cluster-name "$EKS_CLUSTER" \
  --nodegroup-name "$NODE_GROUP" \
  --region "$AWS_REGION" \
  --scaling-config minSize=0,maxSize=6,desiredSize=0 >/dev/null

db_status="$(aws rds describe-db-instances \
  --db-instance-identifier "$DB_INSTANCE" \
  --region "$AWS_REGION" \
  --query 'DBInstances[0].DBInstanceStatus' \
  --output text)"
if [[ "$db_status" == "available" ]]; then
  aws rds stop-db-instance \
    --db-instance-identifier "$DB_INSTANCE" \
    --region "$AWS_REGION" >/dev/null
fi

echo "Pause requested for local PostgreSQL, Kubernetes workloads, EKS workers, and RDS."
echo "Amazon MSK and the EKS control plane remain provisioned because AWS does not support pausing them."
