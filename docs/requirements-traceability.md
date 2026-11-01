# Requirements traceability

| Requirement | Implementation | Verification |
|---|---|---|
| Unified schema for five producers | Five standalone adapters under `services/producer_adapters`, `schemas/governance-event.schema.json`, and `docs/producer-contracts.md` | Source-specific mapper tests validate every adapter output against JSON Schema Draft 2020-12 |
| One MSK topic per producer | Five versioned raw topics in the topology, local topic initializer, and three-broker Terraform configuration | Compose validation and six partitions per topic |
| Independent producer services | Separate images, ports, Deployments, Services, HPAs, service accounts, ECR repositories, and topic-scoped IAM roles for all five adapters | Adapter unit tests plus HTTP-to-Kafka integration test for every service |
| Consumer-group scaling | Independent adapter HPAs, three Kafka Streams replicas, six partitions, HPA to six, and two idempotent sink replicas | Pod-failure procedure checks rebalance, lag, and row reconciliation |
| Sixteen core rules | `config/rules.yml` plus executable predicates in the Java rule engine | Tests assert catalog size, multi-hit behavior, negative behavior, windows, and joins |
| Windowed detection | Five-minute failed-login event-time window with one-minute grace | Topology test emits exactly one finding on the fifth failure |
| Stateful joins | Production promotion joined with model approval and passed-evaluation state tables | Topology test verifies missing evidence emits `GOV-011` and `GOV-012` |
| Five framework mappings | Each rule maps to NIST AI RMF 1.0, ISO/IEC 42001:2023, EU AI Act, NIST SP 800-53 Rev. 5, and GDPR | Catalog loader fails unless all 16 entries are readable; mappings are exposed in PostgreSQL |
| Severity × frequency score | Deterministic 1–5 frequency buckets and normalized 0–100 baseline | Unit tests cover bucket boundaries and maximum score |
| Isolation Forest comparison | Online bounded-history scorer stores ML score, delta, status, and recommended score beside baseline | Python tests cover warm-up and ready states with bounded output |
| PostgreSQL persistence | Idempotent raw-event and finding upserts plus indexed JSON evidence | SQL parses in PostgreSQL dialect; duplicate finding IDs update scores |
| Tableau dashboards | Four purpose-built views and dashboard field/layout instructions | Views cover framework status, risk trend, incident history, and pipeline latency |
| Precision and recall | Labelled injection for all 16 rules plus five benign events per repeat | Pairwise `(event_id, rule_id)` confusion counts |
| p50/p95/p99 latency | End-to-end persistence latency in evaluator and Tableau view | Percentiles reported per run and per producer/time bucket |
| Throughput | Persisted input count divided by observed ingestion interval | Evaluation result stores events per second |
| Pod and broker resilience | EKS PDB/HPA, replication factor 3, min ISR 2, idempotent writes, and failure protocol | Offset, lag, database, and precision/recall reconciliation after each fault |
