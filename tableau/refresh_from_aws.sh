#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_REGION="${AWS_REGION:-us-west-2}"
EKS_CLUSTER="${EKS_CLUSTER:-ai-governance-eks}"
DB_INSTANCE="${DB_INSTANCE:-ai-governance-postgres}"
NAMESPACE="${NAMESPACE:-governance}"
TUNNEL_POD="tableau-rds-tunnel"
LOCAL_SOURCE_PORT="${LOCAL_SOURCE_PORT:-15432}"
EXPORT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aws-tableau-refresh.XXXXXX")"
PORT_FORWARD_PID=""

cleanup() {
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
  kubectl -n "$NAMESPACE" delete pod "$TUNNEL_POD" --ignore-not-found >/dev/null 2>&1 || true
  find "$EXPORT_DIR" -type f -delete 2>/dev/null || true
  rmdir "$EXPORT_DIR" 2>/dev/null || true
  unset SOURCE_PASSWORD
}
trap cleanup EXIT INT TERM

for command in aws kubectl docker python3; do
  command -v "$command" >/dev/null || {
    echo "Required command is missing: $command" >&2
    exit 1
  }
done

cd "$ROOT_DIR"
aws sts get-caller-identity >/dev/null
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION" >/dev/null

docker compose up -d postgres >/dev/null
until docker compose exec -T postgres pg_isready -U governance -d governance >/dev/null 2>&1; do
  sleep 1
done
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance \
  < tableau/aws_reporting_schema.sql >/dev/null

kubectl -n "$NAMESPACE" delete pod "$TUNNEL_POD" --ignore-not-found >/dev/null
kubectl -n "$NAMESPACE" run "$TUNNEL_POD" \
  --image=alpine/socat \
  --restart=Never \
  -- TCP-LISTEN:5432,fork,reuseaddr \
  "TCP:$(aws rds describe-db-instances \
    --db-instance-identifier "$DB_INSTANCE" \
    --region "$AWS_REGION" \
    --query 'DBInstances[0].Endpoint.Address' \
    --output text):5432" >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$TUNNEL_POD" --timeout=90s >/dev/null

kubectl -n "$NAMESPACE" port-forward "pod/$TUNNEL_POD" \
  "$LOCAL_SOURCE_PORT:5432" >"$EXPORT_DIR/port-forward.log" 2>&1 &
PORT_FORWARD_PID=$!

python3 - "$LOCAL_SOURCE_PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
for _ in range(60):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            break
    except OSError:
        time.sleep(0.5)
else:
    raise SystemExit("The private RDS tunnel did not become ready")
PY

SECRET_ARN="$(aws rds describe-db-instances \
  --db-instance-identifier "$DB_INSTANCE" \
  --region "$AWS_REGION" \
  --query 'DBInstances[0].MasterUserSecret.SecretArn' \
  --output text)"
SOURCE_PASSWORD="$(aws secretsmanager get-secret-value \
  --secret-id "$SECRET_ARN" \
  --region "$AWS_REGION" \
  --query SecretString \
  --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["password"])')"
export SOURCE_PASSWORD

source_query() {
  local query="$1"
  local output_file="$2"
  docker run --rm \
    -e PGPASSWORD="$SOURCE_PASSWORD" \
    -v "$EXPORT_DIR:/exports" \
    postgres:18-alpine \
    psql -v ON_ERROR_STOP=1 \
      -h host.docker.internal -p "$LOCAL_SOURCE_PORT" \
      -U governance_admin -d governance \
      -c "\\copy ($query) TO '/exports/$output_file' CSV HEADER" >/dev/null
}

source_query \
  "SELECT * FROM tableau_framework_status ORDER BY day, framework" \
  framework_status.csv
source_query \
  "SELECT * FROM tableau_risk_trend ORDER BY time_bucket, producer, rule_id" \
  risk_trend.csv
source_query \
  "SELECT finding_id, detected_at, rule_id, title, producer, severity_label, baseline_risk, isolation_forest_risk, risk_delta, recommended_risk, model_status, source_event_id, jsonb_build_object('sanitized', true) AS evidence FROM tableau_incident_history ORDER BY detected_at DESC" \
  incident_history.csv
source_query \
  "SELECT * FROM tableau_pipeline_latency ORDER BY time_bucket, producer" \
  pipeline_latency.csv

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance \
  -c "TRUNCATE aws_tableau.framework_status_next, aws_tableau.risk_trend_next, aws_tableau.incident_history_next, aws_tableau.pipeline_latency_next" >/dev/null

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance \
  -c "\\copy aws_tableau.framework_status_next FROM STDIN CSV HEADER" \
  < "$EXPORT_DIR/framework_status.csv" >/dev/null
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance \
  -c "\\copy aws_tableau.risk_trend_next FROM STDIN CSV HEADER" \
  < "$EXPORT_DIR/risk_trend.csv" >/dev/null
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance \
  -c "\\copy aws_tableau.incident_history_next FROM STDIN CSV HEADER" \
  < "$EXPORT_DIR/incident_history.csv" >/dev/null
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance \
  -c "\\copy aws_tableau.pipeline_latency_next FROM STDIN CSV HEADER" \
  < "$EXPORT_DIR/pipeline_latency.csv" >/dev/null

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U governance -d governance <<'SQL' >/dev/null
BEGIN;
TRUNCATE aws_tableau.framework_status, aws_tableau.risk_trend,
  aws_tableau.incident_history, aws_tableau.pipeline_latency;
INSERT INTO aws_tableau.framework_status SELECT * FROM aws_tableau.framework_status_next;
INSERT INTO aws_tableau.risk_trend SELECT * FROM aws_tableau.risk_trend_next;
INSERT INTO aws_tableau.incident_history SELECT * FROM aws_tableau.incident_history_next;
INSERT INTO aws_tableau.pipeline_latency SELECT * FROM aws_tableau.pipeline_latency_next;
COMMIT;
SQL

docker compose exec -T postgres psql -U governance -d governance -P pager=off -c \
  "SELECT 'framework status' AS dataset, count(*) AS rows FROM aws_tableau.framework_status
   UNION ALL SELECT 'risk trend', count(*) FROM aws_tableau.risk_trend
   UNION ALL SELECT 'incident history', count(*) FROM aws_tableau.incident_history
   UNION ALL SELECT 'pipeline latency', count(*) FROM aws_tableau.pipeline_latency
   ORDER BY dataset;"

echo "AWS reporting data is ready in local PostgreSQL on 127.0.0.1:5432."
