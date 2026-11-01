# Resilience evaluation

Before the AWS test, run `evaluation/local_resilience.sh processor` and `evaluation/local_resilience.sh broker`. Each smoke test publishes the labelled workload at 10 events per second while introducing the fault, then reruns the same precision, recall, latency, throughput, and database reconciliation report. The local broker restart checks client retry and durable consumption behavior; the three-broker MSK procedure below measures broker failover.

Run the labelled injection first and record the latest `evaluation_runs` row. Capture topic end offsets and the consumer-group lag for `governance-rule-engine-v1`, `governance-risk-scorer-v1`, and `governance-postgres-sink-v1`.

## Processor pod failure

1. Continuously inject at a fixed rate for at least five minutes.
2. Delete one rule-engine pod: `kubectl delete pod -n governance -l app=governance-rule-engine --field-selector=status.phase=Running --wait=false`.
3. Record rebalance duration from Kafka consumer-group metrics and the time until the replacement pod is ready.
4. After lag returns to zero, verify that every injected event exists in `raw_events`, finding IDs remain unique, and precision/recall are unchanged.

The expected result is a short lag spike, consumer-group reassignment to the remaining replicas, no duplicate database rows, and no missing labelled findings.

## MSK broker failure

Use a non-production cluster and an approved test window. Record partition leaders and offsets, then reboot exactly one broker with the MSK `RebootBroker` API. Keep the event generator running throughout the reboot. Track under-replicated partitions, producer errors, consumer lag, recovery time, and database counts. Replication factor 3 and `acks=all` should preserve acknowledged records.

The broker test passes when all partitions return in-sync, lag drains to zero, every acknowledged injected event is present, and the evaluation result has no new false negatives. Do not terminate two brokers at once in the three-broker test cluster.

## Evidence to retain

- UTC start/end times and git commit.
- Input rate and total acknowledged events.
- Consumer lag and rebalance/recovery duration.
- Before/after partition offsets and database row counts.
- Precision, recall, p50/p95/p99 latency, and throughput report.
- CloudWatch screenshots or exported metrics for MSK, EKS, and RDS.
