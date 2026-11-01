#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_REGION="${AWS_REGION:-us-west-2}"
EKS_CLUSTER="${EKS_CLUSTER:-ai-governance-eks}"
NODE_GROUP="${NODE_GROUP:-governance-spot-workers}"
DB_INSTANCE="${DB_INSTANCE:-ai-governance-postgres}"
NAMESPACE="${NAMESPACE:-governance}"

cd "$ROOT_DIR"

docker compose up -d postgres >/dev/null
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION" >/dev/null

db_status="$(aws rds describe-db-instances \
  --db-instance-identifier "$DB_INSTANCE" \
  --region "$AWS_REGION" \
  --query 'DBInstances[0].DBInstanceStatus' \
  --output text)"
if [[ "$db_status" == "stopped" ]]; then
  aws rds start-db-instance \
    --db-instance-identifier "$DB_INSTANCE" \
    --region "$AWS_REGION" >/dev/null
fi

aws eks update-nodegroup-config \
  --cluster-name "$EKS_CLUSTER" \
  --nodegroup-name "$NODE_GROUP" \
  --region "$AWS_REGION" \
  --scaling-config minSize=3,maxSize=6,desiredSize=3 >/dev/null

aws eks wait nodegroup-active \
  --cluster-name "$EKS_CLUSTER" \
  --nodegroup-name "$NODE_GROUP" \
  --region "$AWS_REGION"
aws rds wait db-instance-available \
  --db-instance-identifier "$DB_INSTANCE" \
  --region "$AWS_REGION"
kubectl wait --for=condition=Ready node --all --timeout=600s >/dev/null

kubectl scale deployment governance-rule-engine -n "$NAMESPACE" --replicas=3 >/dev/null
kubectl scale deployment governance-risk-scorer -n "$NAMESPACE" --replicas=1 >/dev/null
kubectl scale deployment governance-postgres-sink -n "$NAMESPACE" --replicas=2 >/dev/null
for adapter in \
  keycloak-adapter \
  kubernetes-audit-adapter \
  airflow-openlineage-adapter \
  model-registry-adapter \
  llm-gateway-adapter; do
  kubectl scale deployment "$adapter" -n "$NAMESPACE" --replicas=2 >/dev/null
done

kubectl autoscale deployment governance-rule-engine -n "$NAMESPACE" \
  --min=3 --max=6 --cpu-percent=70 >/dev/null
for adapter in \
  keycloak-adapter \
  kubernetes-audit-adapter \
  airflow-openlineage-adapter \
  model-registry-adapter \
  llm-gateway-adapter; do
  kubectl autoscale deployment "$adapter" -n "$NAMESPACE" \
    --min=2 --max=6 --cpu-percent=70 >/dev/null
done

kubectl rollout status deployment --all -n "$NAMESPACE" --timeout=600s
echo "AI governance services are running again."
