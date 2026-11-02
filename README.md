# Real-time AI governance audit pipeline

This repository is a runnable reference implementation of an event-to-framework compliance pipeline on AWS. Each of the five source systems has its own standalone adapter service, image, HTTP endpoint, deployment, scaling policy, IAM role, and Amazon MSK topic. A separate Kafka Streams service evaluates 16 controls with event-time windows and stateful joins. Independent risk-scoring and PostgreSQL-sink services complete the pipeline, and Tableau reads purpose-built database views.

The project scope follows the supplied requirements. The attached setup guide was used for its MSK, EKS, RDS, and Tableau deployment pattern; its additional producers and unrelated framework list are intentionally outside this implementation.

## Architecture

```text
Keycloak ───────────────> Keycloak adapter ─────────────> keycloak topic ────────┐
Kubernetes audit ───────> Kubernetes audit adapter ─────> kubernetes topic ──────┤
Airflow + OpenLineage ──> Airflow/OpenLineage adapter ──> pipeline topic ────────┼─> Rule engine
Model registry ─────────> Model registry adapter ───────> model-registry topic ──┤      │
LLM gateway ────────────> LLM gateway adapter ──────────> llm-gateway topic ─────┘      ▼
                                                                                  raw findings
                                                                                       │
                                                                                       ▼
                                                                              Risk scorer service
                                                                                       │
                                                                                       ▼
                                                                               PostgreSQL sink
                                                                                       │
                                                                                RDS PostgreSQL
                                                                                       │
                                                                                    Tableau
```

The adapters do not call one another and share no runtime state. Kafka schemas and topics are their only integration contract. The rule engine, risk scorer, and database sink are also separate consumer-group applications.

## What is included

- `schemas/`: versioned JSON contracts for producer events and compliance findings.
- `services/producer_adapters/`: five independently built source adapters with source-specific normalization, health/readiness endpoints, schema validation, safe dead-letter handling, and MSK IAM support.
- `config/rules.yml`: the 16-rule catalog, severity, and mappings to the five frameworks.
- `stream-processor/`: Kafka Streams topology with a five-minute failed-login window and model-promotion joins against approval and evaluation state.
- `services/risk-scorer/`: online Isolation Forest scoring and direct comparison with the deterministic baseline.
- `services/postgres-sink/`: idempotent Kafka-to-PostgreSQL writer.
- `db/`: RDS-compatible schema plus Tableau views for framework status, risk trend, incident history, and latency.
- `infra/terraform/`: VPC, three-broker MSK cluster, EKS, RDS, ECR, security groups, and workload IAM.
- `deploy/k8s/`: EKS manifests with consumer-group scaling and health probes.
- `evaluation/`: labelled event generation, precision/recall and p50/p95/p99 measurement, throughput reporting, and failure-test instructions.
- `evaluation/RESULTS.md`: measured baseline, processor-replacement, and broker-restart results from the local reference stack.
- `tableau/`: dashboard field definitions and connection instructions.
- `docs/requirements-traceability.md`: direct requirement-to-code and requirement-to-test mapping.

## Run locally

Requirements: Docker Desktop with at least 6 GB available.

```bash
cp .env.example .env
docker compose up --build -d
docker compose --profile evaluation run --rm evaluator inject
docker compose --profile evaluation run --rm evaluator report
```

Open the database on `localhost:5432` with database/user/password `governance`. The local Kafka-compatible broker is Redpanda on `localhost:19092`. To watch findings:

```bash
docker compose exec redpanda rpk topic consume governance.findings.scored.v1
```

The local stack uses the same topics, schemas, consumer-group behavior, and SQL as AWS. Redpanda stands in for MSK and the local PostgreSQL container stands in for RDS.

The five adapter endpoints are independently addressable locally:

| Adapter | Event endpoint | Kafka topic |
|---|---|---|
| Keycloak | `http://localhost:8101/events` | `governance.keycloak.v1` |
| Kubernetes audit | `http://localhost:8102/events` | `governance.kubernetes.v1` |
| Airflow + OpenLineage | `http://localhost:8103/events` | `governance.pipeline.v1` |
| Model registry | `http://localhost:8104/events` | `governance.model-registry.v1` |
| LLM gateway | `http://localhost:8105/events` | `governance.llm-gateway.v1` |

Each exposes `/healthz`, `/readyz`, and `/metrics` in addition to `POST /events`.

## Test

```bash
docker build --target build -f stream-processor/Dockerfile .
docker compose --profile test run --rm producer-adapter-tests
docker compose --profile evaluation run --rm evaluator adapter-smoke
docker compose build risk-scorer
docker compose run --rm risk-scorer pytest -q
```

Local recovery smoke tests keep publishing events while replacing the stream processor or restarting the broker:

```bash
evaluation/local_resilience.sh processor
evaluation/local_resilience.sh broker
```

Use `evaluation/RESILIENCE.md` for the corresponding EKS pod and three-broker MSK evaluation and evidence checklist.

## Deploy to AWS

1. Copy `infra/terraform/terraform.tfvars.example` to `terraform.tfvars`, set the region, allowed admin CIDR, and database password, then run `terraform init`, `terraform plan`, and `terraform apply`.
2. Build and push the eight independent service images to the ECR repositories emitted by Terraform.
3. Create the Kubernetes secret described in `deploy/k8s/README.md`, replace the image and role placeholders, and apply both manifests.
4. Configure each producer adapter using `docs/producer-contracts.md` and create the Tableau connection using `tableau/README.md`.
5. Run the evaluation suite before recording results. The failure tests intentionally terminate a processor pod and, for broker testing, require an approved maintenance window.

The framework mappings are evidence-routing decisions for this prototype. They identify the control or legal provision for which a finding is relevant; they do not by themselves establish conformity.

## Optional investigator and evidence reviewer agents

Two Amazon Bedrock agents cooperate through durable PostgreSQL jobs: **finding → investigator report → evidence review → at most one investigator revision → recheck or human review**. Reports and evidence snapshots remain immutable, and neither agent performs remediation. Enable the optional `agents` Compose profile after configuring Bedrock; see the [two-agent setup and exact communication contract](docs/reviewer-agent.md) and [investigator configuration](docs/investigator-agent.md). The original offline project explanation is a historical source snapshot from before the optional agents were added.

## End-to-end results

The November 2026 run exercised all five HTTP adapters, all 16 rules, risk scoring, PostgreSQL persistence, processor/broker recovery, and live Bedrock investigations. Open the [self-contained results page](docs/e2e-results-2026-11-01.html) or read the [execution and reproduction notes](docs/e2e-run.md). The final model is Amazon Nova 2 Lite; earlier model failures are retained in the evidence export.
