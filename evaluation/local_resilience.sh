#!/usr/bin/env bash
set -euo pipefail

case_name="${1:-}"
if [[ "$case_name" != "processor" && "$case_name" != "broker" ]]; then
  echo "Usage: evaluation/local_resilience.sh processor|broker" >&2
  exit 2
fi

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

raw_event_count() {
  docker compose exec -T postgres psql -U governance -d governance -Atqc \
    "SELECT count(*) FROM raw_events"
}

before="$(raw_event_count)"
output_file="$(mktemp)"
trap 'rm -f "$output_file"' EXIT

docker compose --profile evaluation run --rm evaluator inject --delay-ms 100 >"$output_file" &
inject_pid=$!

deadline=$((SECONDS + 60))
while (( $(raw_event_count) < before + 10 )); do
  if ! kill -0 "$inject_pid" 2>/dev/null; then
    wait "$inject_pid"
    echo "Injection ended before the failure could be introduced." >&2
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for the workload to start." >&2
    exit 1
  fi
  sleep 0.25
done

fault_started="$(date +%s)"
if [[ "$case_name" == "processor" ]]; then
  docker compose stop stream-processor
  docker compose rm -f stream-processor
  docker compose up -d stream-processor
else
  docker compose restart redpanda
fi
fault_recovered="$(date +%s)"

wait "$inject_pid"
cat "$output_file"
echo "${case_name}_restart_seconds=$((fault_recovered - fault_started))"
docker compose --profile evaluation run --rm evaluator report --wait 180
