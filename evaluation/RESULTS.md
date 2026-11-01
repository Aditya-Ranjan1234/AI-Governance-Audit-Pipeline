# Local evaluation results

Measured on 2026-10-04 with the Docker Compose reference stack. Each run covered all 16 rules and compared the detected `(event_id, rule_id)` pairs with labelled ground truth.

| Scenario | Events | Expected / observed | Precision | Recall | p50 / p95 / p99 latency | Throughput | Fault recovery |
|---|---:|---:|---:|---:|---:|---:|---:|
| Baseline | 72 | 48 / 48 | 1.0000 | 1.0000 | 2,275 / 3,113 / 3,139 ms | 130.90 events/s | n/a |
| Processor replacement during injection | 72 | 48 / 48 | 1.0000 | 1.0000 | 47,298 / 48,338 / 48,352 ms | 11.50 events/s | Container replaced in 3 s; backlog drained with no loss |
| Broker restart during injection | 72 | 48 / 48 | 1.0000 | 1.0000 | 7,538 / 7,919 / 7,931 ms | 11.48 events/s | Broker restarted in 1 s; producer retried with no loss |
| Independent-service regression baseline | 72 | 48 / 48 | 1.0000 | 1.0000 | 1,620 / 2,710 / 2,724 ms | 667.12 events/s | Five standalone producer adapters running |

The recovery runs intentionally publish at 10 events per second so the fault occurs during the workload. Latency includes injection, outage, consumer recovery, risk scoring, and PostgreSQL persistence. The local single-broker test verifies retry and durable recovery behavior. The three-broker MSK failover procedure in `RESILIENCE.md` must be run in the target AWS account to measure partition-leader failover and replica recovery.

The independent-adapter smoke test posted one native event to each of the five separate HTTP services. All five were normalized, schema validated, written to their assigned topics, detected by the rule engine, scored, and persisted. It observed 5/5 expected findings. A raw prompt marker sent to the LLM adapter was absent from both the canonical event attributes and the PostgreSQL JSON payload.
