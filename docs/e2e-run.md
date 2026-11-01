# End-to-end execution, started 1 November 2026

The measured flow uses the local Docker reference stack and **live Amazon Bedrock**. It exercises five HTTP producer adapters, all 16 rules, baseline and Isolation Forest scoring, PostgreSQL persistence, and the investigator. Source events are synthetic fixtures. Tableau is skipped; its database views and an offline results report cover result inspection.

The final model configuration is `us.amazon.nova-2-lite-v1:0`, region `us-west-2`, with 3,000 output tokens per model turn and the existing six-turn limit. The investigator uses a dedicated database login that assumes `governance_investigator`. Its packaged container runs as UID 10001 with a read-only filesystem, dropped capabilities and temporary AWS session credentials.

## Results

- Baseline: 72 events through HTTP; 48/48 expected findings; precision and recall 1.0; p95 1.97 seconds.
- Native-format smoke cases: five adapters, five expected findings, all observed.
- Processor replacement during injection: 24/24 events and 16/16 findings recovered; p95 45.88 seconds.
- Broker restart during injection: 24/24 events and 16/16 findings recovered; p95 2.05 seconds.
- Total: 125 distinct events and 85 findings, plus a successful replay with no additional finding row.
- The investigator covers GOV-002, GOV-003, GOV-004, GOV-006, GOV-011 and GOV-012: 32 eligible findings. The HTML/JSON export records their final status and complete reports.
- Raw prompt markers were absent from persisted event payloads. The actual agent database login was denied a source-table UPDATE.

Latency begins at adapter normalization and ends at finding persistence. These small functional/recovery runs do not establish production throughput or a managed-MSK failover SLA. The existing EKS check did not succeed; no new cloud infrastructure was deployed.

Open `docs/e2e-results-2026-11-01.html` for the complete, self-contained report. Its companion JSON contains the measured runs, findings, reports, evidence snapshots, job history and audit records. Raw local artifacts and a database backup are under `evaluation/results/e2e-2026-11-01/`.

## Model trials and fixes

Amazon Nova Lite v1 produced invalid tool sequences and some report-submission failures. A diagnostic rerun succeeded for one original failure; that failed attempt and its explicit operator requeue remain recorded. Subsequent failures motivated actionable, safe schema feedback, correction of mixed read/submission batches, and bounded retries when a model error omits an original HTTP status.

Claude Haiku 4.5 passed a short text probe but its full tool requests were denied with an AWS Marketplace payment-instrument error. Billing settings were not changed. The worker now stops on systemic configuration errors rather than failing every queued finding. Unstarted jobs for the disabled model were explicitly marked as disabled; their history was retained.

Amazon Nova 2 Lite passed the complete tool flow and was selected for the final investigation run. Earlier model trials are retained separately and must not be confused with final-model success counts. No mock model is used in the final live run. Unit tests still use scripted responses and botocore Stubber.

## Why one agent

One investigator is sufficient for this workflow. The stream processor and scorer are deterministic services, not agents. The live integration needed reliable model/tool behavior before another agent could be justified.

The exact communication contract is:

1. The sink writes raw events and findings to PostgreSQL.
2. The investigator polls persisted findings and creates unique jobs keyed by finding ID, agent version, policy hash and model ID.
3. It claims a job with a lease and calls Bedrock Converse over HTTPS.
4. Bedrock returns an allowlisted `toolUse` name, `toolUseId` and JSON input.
5. The worker performs fixed, scoped reads and returns `toolResult` with the same `toolUseId` and stable evidence references.
6. Bedrock calls `submit_investigation`; validation checks structure, references and timestamps before a separate report/evidence snapshot is saved.
7. Jobs, runs and audit records retain outcomes. Source findings and scores remain outside the agent's write permissions.

There is no agent-to-agent channel in this implementation. If added later, an independent reviewer should consume a durable review job keyed by the completed investigation's `run_id`, read the immutable report/evidence snapshot, and write its own review verdict. That is a future interface proposal and was not built or tested here.

## Reproduction

The overlay reuses existing dependency images and mounts current Python source files. The Java JAR was built from the current repository using the Maven build stage, and its seven tests passed. The previous PostgreSQL volume was preserved by using a separate Compose project.

```bash
docker build --target build -f stream-processor/Dockerfile -t philstock-stream-processor-build:e2e .
docker create --name e2e-jar-export philstock-stream-processor-build:e2e
docker cp e2e-jar-export:/workspace/target/stream-processor-1.0.0.jar stream-processor/target/stream-processor-1.0.0.jar
docker rm e2e-jar-export

docker compose -p philstock-e2e-20261101 -f docker-compose.yml -f evaluation/compose.e2e.yml up -d --no-build --pull never
docker compose -p philstock-e2e-20261101 -f docker-compose.yml -f evaluation/compose.e2e.yml --profile evaluation run --rm --no-deps evaluator inject --via-adapters --repeats 3
docker compose -p philstock-e2e-20261101 -f docker-compose.yml -f evaluation/compose.e2e.yml --profile evaluation run --rm --no-deps evaluator report --run-id RUN_UUID
```

The overlay's `philstock-*:v2` dependency images and evaluator image must exist. If unavailable, build them with the same Compose files before starting. The current Java JAR must also exist for its read-only bind mount.

`evaluation/run_e2e_recovery.py` replaces the processor or restarts the broker only for an explicitly named `philstock-e2e-*` project. It waits until events are in flight, applies the fault, and reconciles exact run IDs and labelled ground truth. It must only target an isolated local test stack.

Use the investigator setup guide for Bedrock credentials and migration commands. The local `.env.e2e` is private and ignored by Git; it contains this run's restricted database login. Temporary container credentials must not be reused after expiry.

## Verification

67 automated tests passed: 49 investigator tests, seven Java tests, five producer-mapper tests, three native-ingestion tests and three scorer tests. Agent integration tests used a separate `governance_test` database. Lint, formatting, Compose validation and Dockerfile checks passed. The investigator image built successfully from SHA-256-verified PyPI wheels after ordinary downloads timed out.

The exporter validates every final report again before generating HTML. Report references are checked for membership and exact event timestamps; this does not establish semantic entailment of every model statement. Reports are advisory, and their recommendations are not executed.

The final investigator version is `investigator-v1.3`. Manual review of the first Nova 2 reports found two domain-wording mistakes: calling a successful non-MFA login unauthenticated, and claiming GOV-006 requires change approval. Version 1.1 added narrow wording checks, but three reports exhausted their model-turn limit. Version 1.2 removes the Kubernetes mapper's irrelevant RBAC approval default from pod evidence, distinguishes image approval from change approval in validation, and records safe validation diagnostics. Version 1.3 also makes event-only timeline references explicit in the tool schema and clarifies GOV-006 metadata to match its existing Java predicate. The detection condition and historical source records are unchanged. All eligible findings are regenerated under this version; older attempts and snapshots are retained. These narrow guards are not a general proof of report semantics.

## Remaining interpretation limits

The execution pass is separate from interpretation quality. Source-based spot checks on the final v1.3 reports still found mistakes in prose about authentication, event order, session linkage and bounded-query absence. Eight reports have explicit review notes in the HTML and companion JSON; original model text is preserved. The notes are in `evaluation/e2e-review-notes-2026-11-01.json`. This review was part of the E2E task, not a second product agent, and was not an exhaustive semantic audit. No report recommendations were executed.
