# Project flow evidence - 3 November 2026

This package contains **11 PNG images**, the underlying JSON/CSV/text evidence, and an [offline evidence viewer](flow-evidence.html). It is intended for the SPA and Agentic AI reports and presentations.

The eight images under `figures/` are **offline evidence figures rendered from actual logs and records**. They are labelled accordingly. The three images under `screenshots/` are **AWS-generated CloudWatch metric images** returned by `GetMetricWidgetImage`, not console-UI screenshots. Browser policy prevented local-file browser capture, and computer-use policy prevented Terminal capture. No substitute image is presented as a screenshot of those applications.

## What was exercised

| Evidence group | Execution and result |
|---|---|
| Fresh pipeline benchmark | 72 synthetic source-shaped events through all five HTTP adapters; 48/48 expected findings across all 16 rules; no missing or unexpected findings |
| Fresh individual agent case | One additional synthetic privileged login without MFA, followed through normalization, three Kafka records, persistence, live investigation and live review |
| Fresh agent outcome | `review_passed`, with original report/evidence preserved and no revision requested |
| Fresh automated checks | 83 agent tests passed in 9.65 seconds, using scripted models/provider stubs and an isolated PostgreSQL test database |
| Saved correction example | The 2 November live run demonstrates cited chronology feedback, one revision and a recheck ending in `needs_human_review` |
| AWS corroboration | CloudWatch returned 5 invocations, 12,436 input tokens, 530 output tokens and 1,476.4 ms average invocation latency for the demo model/window |

The pipeline benchmark and individual agent case are separate workloads. The captured application database contains **73 events and 49 findings** in total. The local infrastructure is Docker/Redpanda/PostgreSQL; Bedrock inference is live AWS. CloudWatch metrics are account/model/region aggregates. The application audit supplies the case-level correlation.

## Images and ready-to-use captions

| Image | Suggested report caption | Best use |
|---|---|---|
| [01 - Pipeline run](figures/01-pipeline-run.png) | The labelled functional run persisted 72 input events and detected all 48 expected event-rule matches across 16 rules. | SPA results |
| [02 - Producer health](figures/02-producer-health.png) | All five adapters were ready and their accepted-event counters matched the captured database counts. | SPA ingestion and integration |
| [03 - Kafka trace](figures/03-kafka-hop-trace.png) | One source event is correlated with its canonical event and finding records using actual Kafka topics, partitions and offsets. | SPA data flow |
| [04 - Persisted finding](figures/04-persisted-finding.png) | PostgreSQL retains the triggering event, GOV-003 finding, baseline score, Isolation Forest score and persistence timestamps. | SPA persistence and scoring |
| [05 - Agent tool trace](figures/05-agent-tool-trace.png) | The Investigator and Evidence Reviewer executed their required evidence reads and structured submissions across five live Bedrock calls. | Agentic AI tool use |
| [06 - Reviewed report](figures/06-reviewed-report.png) | The live reviewer accepted the single-event investigation, with zero cited issues and the original evidence snapshot preserved. | Agentic AI worked example |
| [07 - Correction and recheck](figures/07-correction-and-recheck.png) | A saved 2 November case shows cited correction feedback, one preserved report revision and terminal human escalation after recheck. | Agentic AI communication and stopping rule |
| [08 - Tests and consumers](figures/08-tests-and-consumer-checks.png) | The automated agent suite passed 83 tests; a payload-level tail check found no unread application findings. | Testing and traceability |
| [09 - AWS invocations](screenshots/09-aws-bedrock-invocations.png) | AWS CloudWatch recorded five successful Bedrock invocations for Nova 2 Lite in the selected demonstration window. | Live provider evidence |
| [10 - AWS tokens](screenshots/10-aws-bedrock-tokens.png) | CloudWatch recorded 12,436 input and 530 output tokens, matching the application audit totals for this demonstration window. | Agentic AI model usage |
| [11 - AWS latency](screenshots/11-aws-bedrock-latency.png) | CloudWatch reported 1,476.4 ms average Bedrock invocation latency in the demonstration window. | Model latency evidence |

Local evidence figures are 1920 x 1200 PNGs. AWS charts are 1600 x 900 PNGs. Each figure is based on captured data; no test outcome or model response was manufactured for the images.

## Identifiers for verification

| Record | Identifier |
|---|---|
| Pipeline benchmark run | `ee395f98-db8a-4632-b2b6-13a111522cd0` |
| Fresh canonical event | `9edbc195-3705-5aba-a0cc-dbed51530700` |
| Fresh finding | `05c1291a-2f47-3957-b0a0-c45947a6b3de` |
| Fresh original investigation | `de499241-c17f-4236-95af-46aa85b0f0a4` |
| Fresh review | `d330cb4d-f3a5-47d3-adbe-532aea3098f5` |
| Saved correction root | `cddcd97a-3c7c-4f27-a6ed-412d68dbb2bc` |
| Saved revision | `5e36a07f-749a-4f97-8681-e82c9ff429a3` |
| Model/profile | `us.amazon.nova-2-lite-v1:0` |
| AWS region | `us-west-2` |

The fresh live case ran around **22:28 IST on 3 November 2026** (16:58 UTC). The AWS charts cover 22:25-22:35 IST. Application trace timestamps retain UTC or an explicit timezone offset. The stored correction example uses events from 1 November and the agent exchange recorded on 2 November; its figure is visibly labelled as that saved run.

## Trace files

- [Pipeline results](traces/pipeline-results.json): expected/observed matches, detection metrics and latency percentiles.
- [Input event](traces/demo-input.json) and [HTTP acknowledgement](traces/adapter-acknowledgement.json): the actual submitted payload and adapter response.
- [Canonical event](traces/canonical-event.json), [Kafka hops](traces/kafka-hop-trace.json) and [scored finding](traces/scored-finding.json): record-level correlation through the pipeline.
- [Full live agent trace](traces/live-agent-trace.json): original report, review, jobs, evidence snapshot and audit events.
- [Tool trace CSV](traces/live-tool-trace.csv): a compact timestamped event sequence for tables or timeline graphics.
- [Saved correction trace](traces/saved-correction-trace.json): the preserved original, cited feedback, revision and final outcome, with its source-file hash.
- [Scoring CSV](traces/finding-scores.csv): the 49 captured findings with baseline, ML and recommended scores.
- [Adapter health](traces/adapter-health.json), [service inventory](traces/services.json) and [database counts](traces/database-summary.json): observed runtime state.
- [Automated test output](traces/automated-tests.txt), [consumer groups](traces/consumer-groups.txt) and [payload tail check](traces/risk-consumer-tail-check.json): verification records.
- [AWS metric data](traces/aws-bedrock-live-metrics.json): the read-only CloudWatch response, including metric names, statistic and dimensions. The adjacent `*-query.json` files record the image requests.
- [Cross-checks](traces/evidence-validation.json): identifier, citation, count and token reconciliation performed when preparing the package.
- [Final runtime state](traces/runtime-state.json): the temporary test services were stopped after capture; their Docker volumes and a private database backup were retained.
- [Integrity manifest](manifest.json): SHA-256 hashes and sizes of the packaged evidence files.

## Reading the measurements

The fresh benchmark recorded precision/recall of 1.0, p50 **353.84 ms**, p95 **1,878.88 ms**, p99 **2,181.69 ms**, and **39.04 events/s** over the observed persistence interval. These are measurements for the small labelled functional workload.

The fresh agent case is one successful example. Its passing review is advisory. The saved correction case demonstrates the feedback/revision/escalation flow, and retains the actual `needs_human_review` outcome. Those two selected examples do not replace a corpus-wide semantic-quality evaluation.

The risk-scorer consumer reported an offset lag of 8. Reading from its committed offsets to the end of all six partitions returned **zero application records**. Kafka transaction/control offsets explain why the raw offset count and unread-payload count differ. Both measurements are retained.

The AWS metric widgets are generated according to the [CloudWatch GetMetricWidgetImage schema](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Metric-Widget-Structure.html). No AWS infrastructure or account settings were changed to collect them.

All 11 PNGs were visually inspected. The offline viewer's fragment targets, local resources and JavaScript syntax were checked, and the local links across this index and the two course reports resolved. Browser rendering of the local viewer was not tested because local-file access was blocked. The viewer and images remain available after the test services are stopped.

## Presentation sequence

For **SPA**, use figures 01, 02, 03, 04 and 08. For **Agentic AI**, use figures 03, 05, 06 and 07, followed by the AWS charts. Keep the saved-case date and terminal outcome visible when presenting figure 07.

The same sequence can guide the demonstration-video narration. This package contains the evidence images and traces; a recorded demo video is a separate media item.
