# Producer contracts

Every producer adapter is a standalone HTTP-to-Kafka service. It validates and writes `GovernanceEvent v1` JSON and uses the event's `event_time` as the Kafka record timestamp. An adapter sends invalid records to `governance.dead-letter.v1` with the validation error and a payload fingerprint; it does not copy the rejected source payload into the dead-letter record. The rule engine logs and skips any malformed record that reaches a raw topic. Raw LLM prompt text is never copied into the canonical envelope; the gateway sends detection flags, a non-reversible fingerprint, and a redacted excerpt only when evidence policy permits it.

| Producer service | Topic | Partition key | Native source integration |
|---|---|---|---|
| Keycloak adapter | `governance.keycloak.v1` | actor ID | Keycloak Event Listener posts user/admin events to `/events` |
| Kubernetes audit adapter | `governance.kubernetes.v1` | namespace or actor ID | Kubernetes audit webhook sends an Event or EventList to `/events` |
| Airflow/OpenLineage adapter | `governance.pipeline.v1` | DAG/job ID or `model:version` | OpenLineage HTTP transport sends run events and governance facets to `/events` |
| Model registry adapter | `governance.model-registry.v1` | `model_name:model_version` | Registry lifecycle webhooks post to `/events` |
| LLM gateway adapter | `governance.llm-gateway.v1` | tenant or actor ID | Gateway middleware posts policy signals and fingerprints to `/events` |

All five implementations use idempotent producers, `acks=all`, TLS with MSK IAM authentication, compression, deterministic event IDs when the source supplies an ID, and bounded HTTP request size. The Kafka key is required for events that participate in windows or joins.

## Attributes used by the rule engine

| Event type | Required attributes |
|---|---|
| `LOGIN_FAILURE` | none; `actor.id` is the window key |
| `ROLE_GRANT` | `privileged_role`, `change_approved` |
| `LOGIN_SUCCESS` | `privileged_identity`, `mfa_used` |
| `CLUSTER_ROLE_BINDING` | `cluster_role`, `change_approved` |
| `SECRET_ACCESS` | `cross_namespace` |
| `POD_CREATE` | `privileged`, `image_approved` |
| `DATASET_READ` | `dataset_owner` |
| `SCHEMA_CHANGE` | `version_bumped` |
| `DRIFT_EVALUATION` | `metric_value`, `threshold`, `remediation_status` |
| `BIAS_EVALUATION` | `metric_value`, `threshold`, `approved_exception` |
| `MODEL_EVALUATION_COMPLETED` | `passed`; model name/version in `subject` |
| `APPROVAL_GRANTED` | model name/version in `subject` |
| `MODEL_PROMOTED` | model name/version in `subject`; `environment=prod` |
| `ARTIFACT_REGISTERED` | `signature_verified`, `sha256` |
| `PROMPT_REQUEST` | `pii_detected`, `pii_handling_allowed`, `model_approved`, `prompt_injection_score`, `data_exfiltration_detected` |

## Ordering and state

Approval and passed-evaluation events must use the exact same `model_name:model_version` key as promotion events. The Kafka Streams state tables then hold the latest evidence for each immutable model version. A production promotion that has no matching approval and/or passed evaluation emits `GOV-011` and/or `GOV-012`.

Login failures are keyed by identity and counted in a five-minute event-time window with one minute of grace for late arrival. The finding is emitted once when the count first reaches five.
