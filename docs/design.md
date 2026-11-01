# Design and acceptance criteria

## Independent service boundaries

The architecture contains eight independently deployable application services:

1. Keycloak adapter
2. Kubernetes audit adapter
3. Airflow/OpenLineage adapter
4. Model registry adapter
5. LLM gateway adapter
6. Kafka Streams rule engine
7. Isolation Forest risk scorer
8. PostgreSQL sink

Each source adapter has one responsibility: translate its native event into `GovernanceEvent v1`, validate it, and publish it to its own topic. It has its own image, Kubernetes Deployment, Service, HPA, service account, topic-scoped IAM role, and health metrics. No adapter depends on another adapter. Kafka absorbs temporary downstream outages and lets each component scale and recover independently.

The rule engine consumes all five normalized topics because cross-source policy evaluation is its responsibility. It publishes raw findings without calling the scorer. The scorer consumes raw findings and publishes scored findings. The sink consumes source events and scored findings and performs idempotent database writes. This preserves independent failure and scaling boundaries throughout the processing path.

## Rule behavior

The catalog in `config/rules.yml` is the single reviewable inventory of rule metadata and framework mappings. Executable predicates live in `StatelessRuleEngine`; the Kafka Streams topology owns window and join rules. This separation keeps mappings auditable while allowing stateful code to use Kafka's changelog-backed stores.

`GOV-001` is a five-minute tumbling window with one minute of grace. `GOV-011` and `GOV-012` are stream-table joins: production promotion is the stream, while approval and passed evaluation are materialized tables keyed by immutable model version. The other 13 rules evaluate one event without external state.

## Risk comparison

The baseline first converts occurrence count into a five-point frequency band, then calculates `severity × frequency_band × 4`, producing a 0–100 scale. This is deterministic and explainable.

The Isolation Forest consumes severity, frequency band, baseline score, hour-of-day encoding, and one-hot rule and producer features. It trains after 32 findings and retrains every 25 findings over a bounded 1,000-row history. Its decision function is mapped to 0–100 with 50 as the anomaly boundary. Both scores and their delta are stored. `recommended_risk` is the larger score so an anomalous low-baseline event remains visible; that policy is a configurable operational choice, not a trained claim of impact.

## Measurement definitions

- Precision and recall compare `(source_event_id, rule_id)` pairs with labelled ground truth.
- End-to-end latency is `compliance_findings.persisted_at - raw_events.ingested_at`; reports include p50, p95, and p99 in milliseconds.
- Throughput is the count of persisted injected events divided by the persistence interval. Sustained tests should also retain Kafka records-per-second and consumer-lag metrics from CloudWatch.
- Pod and broker recovery pass only after lag drains to zero and event IDs, finding IDs, and expected detections reconcile.

Recommended prototype acceptance targets are precision ≥ 0.95, recall ≥ 0.95, p95 latency ≤ 5 seconds at the declared test rate, no loss of acknowledged records in a single-pod or single-broker failure, and recovery to zero consumer lag. Record the exact hardware, event mix, rate, duration, and software commit with every result.

## Framework interpretation

A mapping means the event is useful evidence for the cited governance obligation or control. One technical finding can support several provisions, but the mapping does not prove the organization satisfies the whole provision. Review mappings with the control owner whenever rule logic, system purpose, risk classification, or a framework edition changes.
